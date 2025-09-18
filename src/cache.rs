use std::{path::PathBuf, sync::Arc};

use bytes::{Bytes, BytesMut};
use bytesize::ByteSize;
use compact_str::CompactString;
use foyer::{
    BlockEngineBuilder, Code, DeviceBuilder, EvictionConfig, FileDeviceBuilder, FsDeviceBuilder,
    HybridCache, HybridCacheBuilder, HybridCachePolicy, IoEngine, IoEngineBuilder,
    PsyncIoEngineBuilder, S3FifoConfig,
};
use mixtrics::registry::prometheus_0_14::PrometheusMetricsRegistry;

use crate::types::{BucketName, ObjectKey, ObjectKind, PageId};

#[derive(Debug)]
pub struct CacheConfig {
    pub memory_size: ByteSize,
    pub disk_cache: Option<DiskCacheConfig>,
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
}

pub async fn build_cache(config: CacheConfig) -> foyer::Result<HybridCache<CacheKey, CacheValue>> {
    let builder = HybridCacheBuilder::new();

    let mut builder = builder
        .with_policy(HybridCachePolicy::WriteOnEviction)
        .with_metrics_registry(Box::new(PrometheusMetricsRegistry::new(
            prometheus::default_registry().clone(),
        )))
        .memory(config.memory_size.as_u64() as usize)
        .with_eviction_config(EvictionConfig::S3Fifo(S3FifoConfig::default()))
        .with_weighter(|key: &CacheKey, value: &CacheValue| {
            key.estimated_size() + value.estimated_size()
        })
        .storage()
        .with_runtime_options(foyer::RuntimeOptions::Separated {
            read_runtime_options: foyer::TokioRuntimeOptions::default(),
            write_runtime_options: foyer::TokioRuntimeOptions::default(),
        })
        .with_io_engine(io_engine().await?);

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
                file_device
                    .build()
                    .map_err(|e| foyer::Error::Other(Box::new(e)))?
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
                fs_device
                    .build()
                    .map_err(|e| foyer::Error::Other(Box::new(e)))?
            }
        };
        let engine = BlockEngineBuilder::new(device).with_block_size(64 * 1024 * 1024);
        builder = builder.with_engine_config(engine);
    }

    builder.build().await
}

async fn io_engine() -> foyer::Result<Arc<dyn IoEngine>> {
    // TODO: use iouring IO engine when on Linux
    PsyncIoEngineBuilder::new()
        .build()
        .await
        .map_err(|e| foyer::Error::Other(Box::new(e)))
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CacheKey {
    pub kind: ObjectKind,
    pub object: ObjectKey,
    pub page_id: PageId,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct CacheKeyHeader(
    /// 8 bits version
    /// 6 bits object kind length
    /// 10 bits object key length
    /// 16 bits page ID
    [u8; 5],
);

impl CacheKeyHeader {
    fn new(
        version: u8,
        kind_len: usize,
        key_len: usize,
        page_id: PageId,
    ) -> Result<Self, &'static str> {
        // version is u8, so it's always valid (0-255)
        if kind_len >= (1 << 6) {
            return Err("Kind length exceeds 6 bits");
        }
        if key_len >= (1 << 10) {
            return Err("Key length exceeds 10 bits");
        }

        let mut bytes = [0u8; 5];

        // Byte 0: version (8 bits)
        bytes[0] = version;

        // Byte 1: kind_len (6 bits, upper) | key_len bits 9-8 (2 bits, lower)
        bytes[1] = ((kind_len as u8) << 2) | ((key_len >> 8) as u8 & 0b11);

        // Byte 2: key_len bits 7-0 (8 bits)
        bytes[2] = (key_len & 0xFF) as u8;

        // Bytes 3-4: page_id (16 bits, big-endian)
        bytes[3] = (page_id >> 8) as u8;
        bytes[4] = (page_id & 0xFF) as u8;

        Ok(Self(bytes))
    }

    fn version(&self) -> u8 {
        self.0[0]
    }

    fn kind_len(&self) -> usize {
        (self.0[1] >> 2) as usize
    }

    fn key_len(&self) -> usize {
        let high_bits = ((self.0[1] & 0b11) as usize) << 8;
        let low_bits = self.0[2] as usize;
        high_bits | low_bits
    }

    fn page_id(&self) -> PageId {
        u16::from_be_bytes([self.0[3], self.0[4]])
    }

    fn from_bytes(bytes: [u8; 5]) -> Result<Self, &'static str> {
        Ok(Self(bytes))
    }

    fn to_bytes(self) -> [u8; 5] {
        self.0
    }
}

/// Format:
/// - header
/// - object kind
/// - object key
impl foyer::Code for CacheKey {
    fn encode(
        &self,
        writer: &mut impl std::io::Write,
    ) -> std::result::Result<(), foyer::CodeError> {
        let flag = CacheKeyHeader::new(0, self.kind.len(), self.object.len(), self.page_id)
            .map_err(|msg| {
                foyer::CodeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, msg))
            })?;
        writer.write_all(&flag.to_bytes())?;
        writer.write_all(self.kind.as_bytes())?;
        writer.write_all(self.object.as_bytes())?;
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, foyer::CodeError>
    where
        Self: Sized,
    {
        let header = {
            let mut buf = [0u8; 5];
            reader.read_exact(&mut buf)?;
            CacheKeyHeader::from_bytes(buf).map_err(|msg| {
                foyer::CodeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, msg))
            })?
        };

        if header.version() != 0 {
            return Err(foyer::CodeError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported version {}", header.version()),
            )));
        }

        let kind = {
            let mut buf = BytesMut::zeroed(header.kind_len());
            reader.read_exact(&mut buf)?;
            let str = CompactString::from_utf8(buf).map_err(|_| {
                foyer::CodeError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid UTF-8 in object kind",
                ))
            })?;
            ObjectKind::new(str)
                .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidData, msg))?
        };

        let object = {
            let mut buf = BytesMut::zeroed(header.key_len());
            reader.read_exact(&mut buf)?;
            let str = CompactString::from_utf8(buf).map_err(|_| {
                foyer::CodeError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid UTF-8 in object key",
                ))
            })?;
            ObjectKey::new(str)
                .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidData, msg))?
        };

        let page_id = header.page_id();

        Ok(Self {
            kind,
            object,
            page_id,
        })
    }

    fn estimated_size(&self) -> usize {
        size_of::<CacheKeyHeader>() + self.kind.len() + self.object.len()
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
    /// 1B: 2b reserved | 6b for bucket name length
    /// 5B: object size
    /// 3B: data len
    /// 4B: mtime
    /// 4B: cached_at
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
        if bucket_name_len >= (1 << 6) {
            return Err("Bucket name length exceeds limit");
        }
        if object_size >= (1 << 40) {
            return Err("Object size exceeds limit");
        }
        if data_len >= (1 << 24) {
            return Err("Data length exceeds limit");
        }
        let bytes = [
            (bucket_name_len as u8) & 0b111111,
            (object_size >> 32) as u8,
            ((object_size >> 24) & 0xff) as u8,
            ((object_size >> 16) & 0xff) as u8,
            ((object_size >> 8) & 0xff) as u8,
            (object_size & 0xff) as u8,
            ((data_len >> 16) & 0xff) as u8,
            ((data_len >> 8) & 0xff) as u8,
            (data_len & 0xff) as u8,
            (mtime >> 24) as u8,
            ((mtime >> 16) & 0xff) as u8,
            ((mtime >> 8) & 0xff) as u8,
            (mtime & 0xff) as u8,
            (cached_at >> 24) as u8,
            ((cached_at >> 16) & 0xff) as u8,
            ((cached_at >> 8) & 0xff) as u8,
            (cached_at & 0xff) as u8,
        ];
        Ok(Self(bytes))
    }

    fn bucket_name_len(self) -> usize {
        (self.0[0] & 0b111111) as usize
    }

    fn object_size(self) -> u64 {
        u64::from_be_bytes([
            0, 0, 0, self.0[1], self.0[2], self.0[3], self.0[4], self.0[5],
        ])
    }

    fn data_len(self) -> usize {
        u32::from_be_bytes([0, self.0[6], self.0[7], self.0[8]]) as usize
    }

    fn mtime(self) -> u32 {
        u32::from_be_bytes([self.0[9], self.0[10], self.0[11], self.0[12]])
    }

    fn cached_at(self) -> u32 {
        u32::from_be_bytes([self.0[13], self.0[14], self.0[15], self.0[16]])
    }

    fn from_bytes(bytes: [u8; 17]) -> Result<Self, &'static str> {
        if bytes[0] & 0b11000000 != 0 {
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
    fn encode(
        &self,
        writer: &mut impl std::io::Write,
    ) -> std::result::Result<(), foyer::CodeError> {
        let flag = CacheValueHeader::new(
            self.bucket.len(),
            self.object_size,
            self.mtime,
            self.data.len(),
            self.cached_at,
        )
        .map_err(|msg| {
            foyer::CodeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, msg))
        })?;
        writer.write_all(&flag.to_bytes())?;
        writer.write_all(self.bucket.as_bytes())?;
        writer.write_all(&self.data)?;
        Ok(())
    }

    fn decode(reader: &mut impl std::io::Read) -> std::result::Result<Self, foyer::CodeError>
    where
        Self: Sized,
    {
        let header = {
            let mut buf = [0u8; 17];
            reader.read_exact(&mut buf)?;
            CacheValueHeader::from_bytes(buf).map_err(|msg| {
                foyer::CodeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, msg))
            })?
        };

        let bucket = {
            let mut buf = BytesMut::zeroed(header.bucket_name_len());
            reader.read_exact(&mut buf)?;

            let str = CompactString::from_utf8(buf).map_err(|_| {
                foyer::CodeError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid UTF-8 in bucket name",
                ))
            })?;

            BucketName::new(str).map_err(|msg| {
                foyer::CodeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, msg))
            })?
        };

        let data = {
            let mut buf = BytesMut::zeroed(header.data_len());
            reader.read_exact(&mut buf)?;
            buf.freeze()
        };

        Ok(Self {
            bucket: bucket.clone(),
            object_size: header.object_size(),
            mtime: header.mtime(),
            data: data.clone(),
            cached_at: header.cached_at(),
        })
    }

    fn estimated_size(&self) -> usize {
        size_of::<CacheValueHeader>() + self.bucket.len() + self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn test_cache_key_header() {
        // Test valid header creation
        let header = CacheKeyHeader::new(255, 63, 1023, 65535).unwrap();
        assert_eq!(header.version(), 255);
        assert_eq!(header.kind_len(), 63);
        assert_eq!(header.key_len(), 1023);
        assert_eq!(header.page_id(), 65535);

        // Test roundtrip
        let bytes = header.to_bytes();
        let decoded = CacheKeyHeader::from_bytes(bytes).unwrap();
        assert_eq!(header, decoded);

        // Test error cases
        assert!(CacheKeyHeader::new(0, 64, 0, 0).is_err()); // kind_len too large (>= 64)
        assert!(CacheKeyHeader::new(0, 0, 1024, 0).is_err()); // key_len too large (>= 1024)
    }

    #[test]
    fn test_cache_value_header() {
        // Test valid header creation
        let header =
            CacheValueHeader::new(63, (1 << 40) - 1, u32::MAX, (1 << 24) - 1, 1700000000).unwrap();
        assert_eq!(header.bucket_name_len(), 63);
        assert_eq!(header.object_size(), (1 << 40) - 1);
        assert_eq!(header.mtime(), u32::MAX);
        assert_eq!(header.data_len(), (1 << 24) - 1);
        assert_eq!(header.cached_at(), 1700000000);

        // Test roundtrip
        let bytes = header.to_bytes();
        let decoded = CacheValueHeader::from_bytes(bytes).unwrap();
        assert_eq!(header.0, decoded.0);

        // Test error cases
        assert!(CacheValueHeader::new(64, 0, 0, 0, 0).is_err()); // bucket_name_len too large
        assert!(CacheValueHeader::new(0, 1 << 40, 0, 0, 0).is_err()); // object_size too large
        assert!(CacheValueHeader::new(0, 0, 0, 1 << 24, 0).is_err()); // data_len too large
    }

    #[test]
    fn test_cache_key_encode_decode() {
        let key = CacheKey {
            kind: ObjectKind::new("test-kind").unwrap(),
            object: ObjectKey::new("test/object.txt").unwrap(),
            page_id: 42,
        };

        let mut encoded = Vec::new();
        key.encode(&mut encoded).unwrap();

        let mut reader = std::io::Cursor::new(encoded);
        let decoded = CacheKey::decode(&mut reader).unwrap();

        assert_eq!(key, decoded);
    }

    #[test]
    fn test_cache_value_encode_decode() {
        let value = CacheValue {
            bucket: BucketName::new("test-bucket").unwrap(),
            mtime: 1234567890,
            object_size: 9876543210,
            data: bytes::Bytes::from(vec![1, 2, 3, 4, 5]),
            cached_at: 1700000000,
        };

        let mut encoded = Vec::new();
        value.encode(&mut encoded).unwrap();

        let mut reader = std::io::Cursor::new(encoded);
        let decoded = CacheValue::decode(&mut reader).unwrap();

        assert_eq!(value.bucket, decoded.bucket);
        assert_eq!(value.mtime, decoded.mtime);
        assert_eq!(value.object_size, decoded.object_size);
        assert_eq!(value.data, decoded.data);
        assert_eq!(value.cached_at, decoded.cached_at);
    }

    // Property-based tests
    proptest! {
        #[test]
        fn prop_cache_key_header_roundtrip(
            version in 0u8..=255,
            kind_len in 0usize..64,
            key_len in 0usize..1024,
            page_id in 0u16..=u16::MAX
        ) {
            let header = CacheKeyHeader::new(version, kind_len, key_len, page_id).unwrap();
            let bytes = header.to_bytes();
            let decoded = CacheKeyHeader::from_bytes(bytes).unwrap();

            prop_assert_eq!(header.version(), decoded.version());
            prop_assert_eq!(header.kind_len(), decoded.kind_len());
            prop_assert_eq!(header.key_len(), decoded.key_len());
            prop_assert_eq!(header.page_id(), decoded.page_id());
        }

        #[test]
        fn prop_cache_value_header_roundtrip(
            bucket_name_len in 0usize..64,
            object_size in 0u64..(1u64 << 40),
            mtime in 0u32..=u32::MAX,
            data_len in 0usize..(1 << 24),
            cached_at in 0u32..=u32::MAX
        ) {
            let header = CacheValueHeader::new(bucket_name_len, object_size, mtime, data_len, cached_at).unwrap();
            let bytes = header.to_bytes();
            let decoded = CacheValueHeader::from_bytes(bytes).unwrap();

            prop_assert_eq!(header.bucket_name_len(), decoded.bucket_name_len());
            prop_assert_eq!(header.object_size(), decoded.object_size());
            prop_assert_eq!(header.mtime(), decoded.mtime());
            prop_assert_eq!(header.data_len(), decoded.data_len());
            prop_assert_eq!(header.cached_at(), decoded.cached_at());
        }

        #[test]
        fn prop_cache_key_roundtrip(
            kind in "[a-z0-9.-]{1,63}",
            object in "[a-zA-Z0-9/_.-]{1,1000}",
            page_id in 0u16..=u16::MAX
        ) {
            let kind = ObjectKind::new(kind);
            prop_assume!(kind.is_ok());

            let object = ObjectKey::new(object);
            prop_assume!(object.is_ok());

            let key = CacheKey {
                kind: kind.unwrap(),
                object: object.unwrap(),
                page_id,
            };

            let mut encoded = Vec::new();
            key.encode(&mut encoded).unwrap();

            prop_assert_eq!(key.estimated_size(), encoded.len());

            let mut reader = std::io::Cursor::new(encoded);
            let decoded = CacheKey::decode(&mut reader).unwrap();

            prop_assert_eq!(key, decoded);
        }

        #[test]
        fn prop_cache_value_roundtrip(
            bucket_name in "[a-z0-9.-]{3,63}",
            mtime in 0u32..=u32::MAX,
            object_size in 0u64..(1u64 << 40),
            data in prop::collection::vec(0u8..=255, 0..1000),
            cached_at in 0u32..=u32::MAX
        ) {
            let bucket = BucketName::new(bucket_name);
            prop_assume!(bucket.is_ok());

            let value = CacheValue {
                bucket: bucket.unwrap(),
                mtime,
                object_size,
                data: bytes::Bytes::from(data),
                cached_at,
            };

            let mut encoded = Vec::new();
            value.encode(&mut encoded).unwrap();

            prop_assert_eq!(value.estimated_size(), encoded.len());

            let mut reader = std::io::Cursor::new(encoded);
            let decoded = CacheValue::decode(&mut reader).unwrap();

            prop_assert_eq!(value.bucket, decoded.bucket);
            prop_assert_eq!(value.mtime, decoded.mtime);
            prop_assert_eq!(value.object_size, decoded.object_size);
            prop_assert_eq!(value.data, decoded.data);
            prop_assert_eq!(value.cached_at, decoded.cached_at);
        }
    }

    #[test]
    fn test_cache_key_decode_errors() {
        // Test invalid header (not enough bytes)
        let data = vec![0xFF, 0xFF, 0xFF, 0xFF]; // Invalid header - only 4 bytes instead of 5
        let mut reader = std::io::Cursor::new(data);
        assert!(CacheKey::decode(&mut reader).is_err());

        // Test unsupported version
        let header = CacheKeyHeader::new(1, 4, 4, 0).unwrap(); // version 1
        let mut data = Vec::new();
        data.extend_from_slice(&header.to_bytes());
        data.extend_from_slice(b"kind"); // object kind
        data.extend_from_slice(b"test"); // object key
        let mut reader = std::io::Cursor::new(data);
        assert!(CacheKey::decode(&mut reader).is_err());

        // Test invalid UTF-8 in object kind
        let header = CacheKeyHeader::new(0, 4, 4, 0).unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&header.to_bytes());
        data.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]); // invalid UTF-8 in kind
        data.extend_from_slice(b"test"); // object key
        let mut reader = std::io::Cursor::new(data);
        assert!(CacheKey::decode(&mut reader).is_err());

        // Test invalid UTF-8 in object key
        let header = CacheKeyHeader::new(0, 4, 4, 0).unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&header.to_bytes());
        data.extend_from_slice(b"kind"); // object kind
        data.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]); // invalid UTF-8 in key
        let mut reader = std::io::Cursor::new(data);
        assert!(CacheKey::decode(&mut reader).is_err());
    }

    #[test]
    fn test_cache_value_decode_errors() {
        // Test invalid header
        let data = vec![0xFF; 17]; // Invalid header with reserved bits set
        let mut reader = std::io::Cursor::new(data);
        assert!(CacheValue::decode(&mut reader).is_err());

        // Test invalid UTF-8 in bucket name
        let header = CacheValueHeader::new(4, 0, 0, 0, 0).unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&header.to_bytes());
        data.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]); // invalid UTF-8
        let mut reader = std::io::Cursor::new(data);
        assert!(CacheValue::decode(&mut reader).is_err());
    }
}
