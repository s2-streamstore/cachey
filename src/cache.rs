use std::{
    ops::Range,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

use bytes::{Bytes, BytesMut};
use bytesize::ByteSize;
use compact_str::CompactString;
use foyer::{
    BlockEngineConfig, Code, DeviceBuilder, FileDeviceBuilder, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, HybridCachePolicy, IoEngineConfig,
};
use mixtrics::registry::prometheus_0_14::PrometheusMetricsRegistry;

use crate::{
    service::PAGE_SIZE,
    types::{BucketName, ObjectKey, ObjectKind},
};

/// A full page in foyer's write buffer and disk blocks: the page plus key, value header and
/// foyer's entry framing (under 1.3 KiB), aligned up to 4 KiB.
const DISK_PAGE_ENTRY_BYTES: usize = PAGE_SIZE as usize + 4 * 1024;

/// Full pages that can wait for a disk write while the previous one is in progress. foyer drops
/// evicted entries that do not fit.
const DISK_WRITE_BUFFER_PAGES: usize = 4;

/// foyer starts each disk block with an index of the entries in it.
const DISK_BLOB_INDEX_BYTES: usize = 4 * 1024;

/// Full pages per disk block, the unit foyer reclaims. The block fits exactly this many.
const DISK_BLOCK_PAGES: usize = 4;

#[derive(Debug)]
pub struct CacheConfig {
    pub memory_size: ByteSize,
    pub disk_cache: Option<DiskCacheConfig>,
    pub metrics_registry: Option<prometheus::Registry>,
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum DiskCacheKind {
    #[value(name = "block")]
    BlockFile,
    #[value(name = "fs")]
    FileSystem,
}

#[derive(Debug)]
pub struct DiskCacheConfig {
    pub path: PathBuf,
    pub kind: DiskCacheKind,
    pub capacity: Option<ByteSize>,
    pub iouring: bool,
}

pub async fn build_cache(config: CacheConfig) -> foyer::Result<HybridCache<CacheKey, CacheValue>> {
    let mut builder = HybridCacheBuilder::new().with_policy(HybridCachePolicy::WriteOnEviction);

    if let Some(registry) = config.metrics_registry {
        builder = builder.with_metrics_registry(Box::new(PrometheusMetricsRegistry::new(registry)));
    }

    let mut builder = builder
        .memory(config.memory_size.as_u64() as usize)
        .with_weighter(|key: &CacheKey, value: &CacheValue| {
            key.estimated_size() + value.estimated_size()
        })
        .storage()
        .with_spawner(
            tokio::runtime::Builder::new_multi_thread()
                .thread_name_fn(|| {
                    static TID: AtomicUsize = AtomicUsize::new(0);
                    let id = TID.fetch_add(1, Ordering::Relaxed);
                    format!("foyer-{id}")
                })
                .enable_all()
                .build()?
                .into(),
        );

    if let Some(disk_config) = config.disk_cache {
        // TODO: throttling knobs?
        let device = match disk_config.kind {
            DiskCacheKind::BlockFile => {
                let mut file_device = FileDeviceBuilder::new(disk_config.path);
                #[cfg(target_os = "linux")]
                {
                    file_device = file_device.with_direct(true);
                }
                if let Some(cap) = disk_config.capacity {
                    file_device = file_device.with_capacity(cap.as_u64() as usize);
                }
                file_device.build()?
            }
            DiskCacheKind::FileSystem => {
                let mut fs_device = FsDeviceBuilder::new(disk_config.path);
                #[cfg(target_os = "linux")]
                {
                    fs_device = fs_device.with_direct(true);
                }
                if let Some(cap) = disk_config.capacity {
                    fs_device = fs_device.with_capacity(cap.as_u64() as usize);
                }
                fs_device.build()?
            }
        };
        let write_buffer_bytes = DISK_WRITE_BUFFER_PAGES * DISK_PAGE_ENTRY_BYTES;
        let engine = BlockEngineConfig::new(device)
            .with_block_size(DISK_BLOB_INDEX_BYTES + DISK_BLOCK_PAGES * DISK_PAGE_ENTRY_BYTES)
            .with_blob_index_size(DISK_BLOB_INDEX_BYTES)
            .with_buffer_pool_size(write_buffer_bytes)
            .with_submit_queue_size_threshold(write_buffer_bytes);
        builder = builder
            .with_engine_config(engine)
            .with_io_engine_config(io_engine_config(disk_config.iouring));
    }

    builder.build().await
}

fn io_engine_config(iouring: bool) -> Box<dyn IoEngineConfig> {
    #[cfg(target_os = "linux")]
    if iouring {
        return foyer::UringIoEngineConfig::new().boxed();
    }
    #[cfg(not(target_os = "linux"))]
    let _ = iouring; // Suppress unused warning.
    foyer::PsyncIoEngineConfig::new().boxed()
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CacheKey {
    pub kind: ObjectKind,
    pub object: ObjectKey,
    /// The requested byte range; the entry's data stops early at the object's end.
    pub range: Range<u64>,
}

impl CacheKey {
    const VERSION: u8 = 4;
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct CacheKeyHeader(
    /// 8 bits version
    /// 6 bits object kind length
    /// 10 bits object key length minus one
    [u8; 3],
);

impl CacheKeyHeader {
    fn new(version: u8, kind_len: usize, key_len: usize) -> Result<Self, &'static str> {
        if kind_len == 0 {
            return Err("Kind length cannot be zero");
        }
        if kind_len > (1 << 6) {
            return Err("Kind length exceeds 6 bits");
        }
        if key_len == 0 {
            return Err("Key length cannot be zero");
        }
        if key_len > (1 << 10) {
            return Err("Key length exceeds 10 bits");
        }

        let mut bytes = [0u8; 3];
        let key_len_minus_one = key_len - 1;

        bytes[0] = version;

        // Byte 1: (kind_len - 1) (6 bits, upper) | key_len_minus_one bits 9-8 (2 bits, lower)
        bytes[1] = (((kind_len - 1) as u8) << 2) | ((key_len_minus_one >> 8) as u8 & 0b11);

        // Byte 2: key_len_minus_one bits 7-0 (8 bits)
        bytes[2] = (key_len_minus_one & 0xFF) as u8;

        Ok(Self(bytes))
    }

    fn version(self) -> u8 {
        self.0[0]
    }

    fn kind_len(self) -> usize {
        ((self.0[1] >> 2) as usize) + 1
    }

    fn key_len(self) -> usize {
        let high_bits = ((self.0[1] & 0b11) as usize) << 8;
        let low_bits = self.0[2] as usize;
        (high_bits | low_bits) + 1
    }

    fn to_bytes(self) -> [u8; 3] {
        self.0
    }
}

/// Format:
/// - header
/// - object kind
/// - object key
/// - range start and end
impl foyer::Code for CacheKey {
    fn encode(&self, writer: &mut impl std::io::Write) -> foyer::Result<()> {
        let flag = CacheKeyHeader::new(Self::VERSION, self.kind.len(), self.object.len())
            .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidData, msg))?;

        writer.write_all(&flag.to_bytes())?;
        writer.write_all(self.kind.as_bytes())?;
        writer.write_all(self.object.as_bytes())?;
        writer.write_all(&self.range.start.to_be_bytes())?;
        writer.write_all(&self.range.end.to_be_bytes())?;
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> foyer::Result<Self>
    where
        Self: Sized,
    {
        let header = {
            let mut buf = [0u8; 3];
            reader.read_exact(&mut buf)?;
            CacheKeyHeader(buf)
        };

        if header.version() != Self::VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported version {}", header.version()),
            )
            .into());
        }

        let kind = {
            let mut buf = BytesMut::zeroed(header.kind_len());
            reader.read_exact(&mut buf)?;
            let str = CompactString::from_utf8(buf).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid UTF-8 in object kind",
                )
            })?;
            ObjectKind::new(str)
                .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidData, msg))?
        };

        let object = {
            let mut buf = BytesMut::zeroed(header.key_len());
            reader.read_exact(&mut buf)?;
            let str = CompactString::from_utf8(buf).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid UTF-8 in object key",
                )
            })?;
            ObjectKey::new(str)
                .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidData, msg))?
        };

        let mut offset = || -> std::io::Result<u64> {
            let mut buf = [0u8; size_of::<u64>()];
            reader.read_exact(&mut buf)?;
            Ok(u64::from_be_bytes(buf))
        };
        let range = offset()?..offset()?;

        Ok(Self {
            kind,
            object,
            range,
        })
    }

    fn estimated_size(&self) -> usize {
        size_of::<CacheKeyHeader>() + self.kind.len() + self.object.len() + 2 * size_of::<u64>()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheValue {
    pub bucket: BucketName,
    pub mtime: u32,
    pub data: Bytes,
    pub object_size: u64,
    pub cached_at: u32,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct CacheValueHeader(
    // 1B: 1b reserved | 1b empty flag | 6b for bucket name length
    // 5B: object size
    // 3B: data_len_minus_one (ignored if empty flag set)
    // 4B: mtime
    // 4B: cached_at
    [u8; 17],
);

impl CacheValueHeader {
    fn new(
        bucket_name_len: usize,
        object_size: u64,
        mtime: u32,
        data_len: usize,
        cached_at: u32,
    ) -> Result<Self, &'static str> {
        if bucket_name_len == 0 {
            return Err("Bucket name length cannot be zero");
        }
        if bucket_name_len > (1 << 6) {
            return Err("Bucket name length exceeds limit");
        }
        if object_size >= (1 << 40) {
            return Err("Object size exceeds limit");
        }
        if data_len > (1 << 24) {
            return Err("Data length exceeds limit");
        }
        let data_len_minus_one = (data_len as u32).saturating_sub(1);
        let mut bytes = [0; 17];
        bytes[0] = u8::from(data_len == 0) << 6 | ((bucket_name_len - 1) as u8 & 0b0011_1111);
        bytes[1..6].copy_from_slice(&object_size.to_be_bytes()[3..]);
        bytes[6..9].copy_from_slice(&data_len_minus_one.to_be_bytes()[1..]);
        bytes[9..13].copy_from_slice(&mtime.to_be_bytes());
        bytes[13..].copy_from_slice(&cached_at.to_be_bytes());
        Ok(Self(bytes))
    }

    fn bucket_name_len(self) -> usize {
        ((self.0[0] & 0b0011_1111) as usize) + 1
    }

    fn object_size(self) -> u64 {
        u64::from_be_bytes([
            0, 0, 0, self.0[1], self.0[2], self.0[3], self.0[4], self.0[5],
        ])
    }

    fn data_len(self) -> usize {
        if self.0[0] & 0b0100_0000 != 0 {
            return 0;
        }
        let data_len_minus_one = u32::from_be_bytes([0, self.0[6], self.0[7], self.0[8]]);
        (data_len_minus_one + 1) as usize
    }

    fn mtime(self) -> u32 {
        u32::from_be_bytes([self.0[9], self.0[10], self.0[11], self.0[12]])
    }

    fn cached_at(self) -> u32 {
        u32::from_be_bytes([self.0[13], self.0[14], self.0[15], self.0[16]])
    }

    fn from_bytes(bytes: [u8; 17]) -> Result<Self, &'static str> {
        if bytes[0] & 0b1000_0000 != 0 {
            return Err("Invalid header");
        }
        let empty = bytes[0] & 0b0100_0000 != 0;
        if empty && bytes[6..9] != [0; 3] {
            return Err("Invalid header");
        }
        Ok(Self(bytes))
    }

    fn to_bytes(self) -> [u8; 17] {
        self.0
    }
}

/// Format:
/// - header
/// - bucket
/// - data
impl foyer::Code for CacheValue {
    fn encode(&self, writer: &mut impl std::io::Write) -> foyer::Result<()> {
        let flag = CacheValueHeader::new(
            self.bucket.len(),
            self.object_size,
            self.mtime,
            self.data.len(),
            self.cached_at,
        )
        .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidData, msg))?;
        writer.write_all(&flag.to_bytes())?;
        writer.write_all(self.bucket.as_bytes())?;
        writer.write_all(&self.data)?;
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> foyer::Result<Self>
    where
        Self: Sized,
    {
        let header = {
            let mut buf = [0u8; 17];
            reader.read_exact(&mut buf)?;
            CacheValueHeader::from_bytes(buf)
                .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidData, msg))?
        };

        let bucket = {
            let mut buf = BytesMut::zeroed(header.bucket_name_len());
            reader.read_exact(&mut buf)?;
            let str = CompactString::from_utf8(buf).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid UTF-8 in bucket name",
                )
            })?;
            BucketName::new(str)
                .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidData, msg))?
        };

        let data = {
            let mut buf = BytesMut::zeroed(header.data_len());
            reader.read_exact(&mut buf)?;
            buf.freeze()
        };

        Ok(Self {
            bucket,
            object_size: header.object_size(),
            mtime: header.mtime(),
            data,
            cached_at: header.cached_at(),
        })
    }

    fn estimated_size(&self) -> usize {
        size_of::<CacheValueHeader>() + self.bucket.len() + self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use bytesize::ByteSize;
    use foyer::{Code, HybridCache};
    use proptest::{arbitrary::any, collection, prop_assert_eq, proptest};
    use tempfile::TempDir;

    use super::{
        CacheConfig, CacheKey, CacheKeyHeader, CacheValue, CacheValueHeader, DISK_BLOCK_PAGES,
        DISK_WRITE_BUFFER_PAGES, DiskCacheConfig, DiskCacheKind, build_cache,
    };
    use crate::{
        service::PAGE_SIZE,
        types::{BucketName, ObjectKey, ObjectKind},
    };

    async fn disk_backed_cache(
        metrics_registry: Option<prometheus::Registry>,
    ) -> (TempDir, HybridCache<CacheKey, CacheValue>) {
        let disk_dir = tempfile::tempdir().unwrap();
        let cache = build_cache(CacheConfig {
            memory_size: ByteSize::gib(1),
            disk_cache: Some(DiskCacheConfig {
                path: disk_dir.path().to_owned(),
                kind: DiskCacheKind::FileSystem,
                capacity: Some(ByteSize::mib(512)),
                iouring: false,
            }),
            metrics_registry,
        })
        .await
        .unwrap();
        (disk_dir, cache)
    }

    /// Full pages with the longest names, so each entry carries the most framing.
    fn full_pages(count: usize) -> Vec<(CacheKey, CacheValue)> {
        let count = count as u64;
        (0..count)
            .map(|page_id| {
                let key = CacheKey {
                    kind: ObjectKind::new("k".repeat(64)).unwrap(),
                    object: ObjectKey::new("o".repeat(1024)).unwrap(),
                    range: page_id * PAGE_SIZE..(page_id + 1) * PAGE_SIZE,
                };
                let value = CacheValue {
                    bucket: BucketName::new("b".repeat(64)).unwrap(),
                    mtime: 1_700_000_000,
                    data: Bytes::from(vec![page_id as u8; PAGE_SIZE as usize]),
                    object_size: count * PAGE_SIZE,
                    cached_at: 1_700_000_000,
                };
                (key, value)
            })
            .collect()
    }

    async fn assert_evicted_together_reach_disk(
        cache: &HybridCache<CacheKey, CacheValue>,
        pages: &[(CacheKey, CacheValue)],
    ) {
        for (key, value) in pages {
            cache.insert(key.clone(), value.clone());
        }

        cache.memory().evict_all();
        cache.storage().wait().await;

        for (key, value) in pages {
            assert!(!cache.memory().contains(key));
            let entry = cache.get(key).await.unwrap();
            assert!(
                entry.is_some_and(|entry| entry.value() == value),
                "page {:?} was not read back from disk",
                key.range
            );
        }
    }

    #[tokio::test]
    async fn full_pages_evicted_together_reach_disk() {
        let (_disk_dir, cache) = disk_backed_cache(None).await;
        assert_evicted_together_reach_disk(&cache, &full_pages(DISK_WRITE_BUFFER_PAGES)).await;
    }

    #[tokio::test]
    async fn disk_block_fits_its_full_pages() {
        let registry = prometheus::Registry::new();
        let (_disk_dir, cache) = disk_backed_cache(Some(registry.clone())).await;
        assert_evicted_together_reach_disk(&cache, &full_pages(DISK_BLOCK_PAGES)).await;

        // foyer makes a block evictable once writes move on to the next block.
        let metrics = prometheus::TextEncoder::new()
            .encode_to_string(&registry.gather())
            .unwrap();
        assert!(
            metrics.contains(
                "foyer_storage_block_engine_block{name=\"foyer\",type=\"evictable\"} 0\n"
            ),
            "{DISK_BLOCK_PAGES} full pages spilled past the first disk block"
        );
    }

    #[test]
    fn test_cache_key_header() {
        let header = CacheKeyHeader::new(255, 63, 1024).unwrap();
        assert_eq!(header.to_bytes(), [0xff, 0xfb, 0xff]);

        assert!(CacheKeyHeader::new(0, 0, 0).is_err());
        assert!(CacheKeyHeader::new(0, 65, 0).is_err());
        assert!(CacheKeyHeader::new(0, 1, 0).is_err());
        assert!(CacheKeyHeader::new(0, 1, 1025).is_err());
    }

    #[test]
    fn test_cache_value_header() {
        let header =
            CacheValueHeader::new(63, (1 << 40) - 1, u32::MAX, (1 << 24) - 1, 1_700_000_000)
                .unwrap();
        assert_eq!(
            header.to_bytes(),
            [
                0x3e, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0xff, 0xff, 0xff, 0xff, 0x65,
                0x53, 0xf1, 0x00,
            ]
        );

        assert!(CacheValueHeader::new(0, 0, 0, 0, 0).is_err());
        assert!(CacheValueHeader::new(65, 0, 0, 0, 0).is_err());
        assert!(CacheValueHeader::new(1, 1 << 40, 0, 0, 0).is_err());
        assert!(CacheValueHeader::new(1, 0, 0, (1 << 24) + 1, 0).is_err());
    }

    #[test]
    fn test_cache_value_header_supports_page_size() {
        let header = CacheValueHeader::new(1, PAGE_SIZE, 0, PAGE_SIZE as usize, 0).unwrap();
        assert_eq!(header.data_len(), PAGE_SIZE as usize);
    }

    #[test]
    fn test_cache_value_header_supports_empty_data() {
        let header = CacheValueHeader::new(1, 0, 0, 0, 0).unwrap();
        assert_eq!(header.data_len(), 0);

        let decoded = CacheValueHeader::from_bytes(header.to_bytes()).unwrap();
        assert_eq!(decoded.data_len(), 0);
        let mut invalid = header.to_bytes();
        invalid[8] = 1;
        assert!(CacheValueHeader::from_bytes(invalid).is_err());
    }

    #[test]
    fn test_max_length_bucket_and_kind() {
        let cache_key = CacheKey {
            kind: ObjectKind::new("b".repeat(64)).unwrap(),
            object: ObjectKey::new("c".repeat(1024)).unwrap(),
            range: u64::MAX - 1..u64::MAX,
        };

        let mut encoded_key = Vec::new();
        cache_key.encode(&mut encoded_key).unwrap();

        let decoded_key = CacheKey::decode(&mut encoded_key.as_slice()).unwrap();

        assert_eq!(cache_key, decoded_key);

        let cache_value = CacheValue {
            bucket: BucketName::new("a".repeat(64)).unwrap(),
            mtime: 1_234_567_890,
            object_size: 9_876_543_210,
            data: bytes::Bytes::from(vec![1, 2, 3, 4, 5]),
            cached_at: 1_700_000_000,
        };

        let mut encoded_value = Vec::new();
        cache_value.encode(&mut encoded_value).unwrap();

        let decoded_value = CacheValue::decode(&mut encoded_value.as_slice()).unwrap();

        assert_eq!(cache_value, decoded_value);
    }

    proptest! {
        #[test]
        fn prop_cache_key_header_preserves_fields(
            version in 0u8..=255,
            kind_len in 1usize..=64,
            key_len in 1usize..=1024
        ) {
            let header = CacheKeyHeader::new(version, kind_len, key_len).unwrap();
            prop_assert_eq!(header.version(), version);
            prop_assert_eq!(header.kind_len(), kind_len);
            prop_assert_eq!(header.key_len(), key_len);
        }

        #[test]
        fn prop_cache_value_header_roundtrip(
            bucket_name_len in 1usize..=64,
            object_size in 0u64..(1u64 << 40),
            mtime in 0u32..=u32::MAX,
            data_len in 0usize..=(1 << 24),
            cached_at in 0u32..=u32::MAX
        ) {
            let header = CacheValueHeader::new(bucket_name_len, object_size, mtime, data_len, cached_at).unwrap();
            let bytes = header.to_bytes();
            let decoded = CacheValueHeader::from_bytes(bytes).unwrap();

            prop_assert_eq!(decoded.bucket_name_len(), bucket_name_len);
            prop_assert_eq!(decoded.object_size(), object_size);
            prop_assert_eq!(decoded.mtime(), mtime);
            prop_assert_eq!(decoded.data_len(), data_len);
            prop_assert_eq!(decoded.cached_at(), cached_at);
        }

        #[test]
        fn prop_cache_key_roundtrip(
            kind in "[a-z0-9.-]{1,64}",
            object in "[a-zA-Z0-9/_.-]{1,1024}",
            start in any::<u64>(),
            end in any::<u64>()
        ) {
            let key = CacheKey {
                kind: ObjectKind::new(kind).unwrap(),
                object: ObjectKey::new(object).unwrap(),
                range: start..end,
            };

            let mut encoded = Vec::new();
            key.encode(&mut encoded).unwrap();

            prop_assert_eq!(key.estimated_size(), encoded.len());

            let decoded = CacheKey::decode(&mut encoded.as_slice()).unwrap();

            prop_assert_eq!(key, decoded);
        }

        #[test]
        fn prop_cache_value_roundtrip(
            bucket_name in "[a-z0-9.-]{1,64}",
            mtime in 0u32..=u32::MAX,
            object_size in 0u64..(1u64 << 40),
            data in collection::vec(0u8..=255, 0..1000),
            cached_at in 0u32..=u32::MAX
        ) {
            let value = CacheValue {
                bucket: BucketName::new(bucket_name).unwrap(),
                mtime,
                object_size,
                data: bytes::Bytes::from(data),
                cached_at,
            };

            let mut encoded = Vec::new();
            value.encode(&mut encoded).unwrap();

            prop_assert_eq!(value.estimated_size(), encoded.len());

            let decoded = CacheValue::decode(&mut encoded.as_slice()).unwrap();

            prop_assert_eq!(value, decoded);
        }
    }

    #[test]
    fn test_cache_key_decode_errors() {
        assert!(CacheKey::decode(&mut [0xff; 4].as_slice()).is_err());

        for (version, kind, object) in [
            (0, b"kind", b"test"),
            (CacheKey::VERSION, b"\xff\xff\xff\xff", b"test"),
            (CacheKey::VERSION, b"kind", b"\xff\xff\xff\xff"),
        ] {
            let header = CacheKeyHeader::new(version, kind.len(), object.len()).unwrap();
            let data = [header.to_bytes().as_slice(), kind, object, &[0; 16]].concat();
            assert!(CacheKey::decode(&mut data.as_slice()).is_err());
        }
    }

    #[test]
    fn test_cache_value_decode_errors() {
        assert!(CacheValue::decode(&mut [0xff; 17].as_slice()).is_err());

        let header = CacheValueHeader::new(4, 0, 0, 0, 0).unwrap();
        let data = [header.to_bytes().as_slice(), &[0xff; 4]].concat();
        assert!(CacheValue::decode(&mut data.as_slice()).is_err());
    }
}
