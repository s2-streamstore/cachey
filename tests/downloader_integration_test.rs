mod common;

use std::{ops::Range, sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use cachey::{
    object_store::{BucketMetrics, DownloadError, Downloader, RequestConfig},
    service::{PAGE_SIZE, SlidingThroughput},
    types::{BucketName, BucketNameSet, ObjectKey},
};
use common::setup_rustfs;
use parking_lot::Mutex;

async fn upload_test_object(client: &aws_sdk_s3::Client, bucket: &str, key: &str, data: Bytes) {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(data.into())
        .send()
        .await
        .expect("Failed to upload object");
}

fn make_downloader(client: aws_sdk_s3::Client, hedge_quantile: f64) -> Downloader {
    let throughput = Arc::new(Mutex::new(SlidingThroughput::default()));
    Downloader::new(client, hedge_quantile, throughput)
}

fn bucket_metrics(downloader: &Downloader, bucket: &BucketName) -> BucketMetrics {
    let mut found = None;
    downloader.observe_bucket_metrics(|name, metrics| {
        if name == bucket {
            found = Some(metrics.clone());
        }
    });
    found.expect("bucket metrics should be recorded")
}

#[tokio::test]
async fn test_download_full_object() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize + 100);
    for (i, byte) in test_data.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    let test_data = test_data.freeze();
    let object_key = "test-object.txt";
    upload_test_object(&ctx.client, &ctx.bucket_name, object_key, test_data.clone()).await;

    let bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
    let key = ObjectKey::new(object_key).unwrap();

    // Download a small range from the beginning
    let out = downloader
        .download(
            &buckets,
            key,
            &Range { start: 0, end: 100 },
            &RequestConfig::default(),
        )
        .await
        .unwrap();

    assert_eq!(out.piece.data, test_data.slice(0..100));
    assert_eq!(out.piece.object_size, test_data.len() as u64);
}

#[tokio::test]
async fn test_download_partial_range() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    // Create a large object to work with PAGE_SIZE
    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize + 1000);
    for (i, byte) in test_data.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    let test_data = test_data.freeze();
    let object_key = "range-test.txt";
    upload_test_object(&ctx.client, &ctx.bucket_name, object_key, test_data.clone()).await;

    let bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
    let key = ObjectKey::new(object_key).unwrap();

    // Download a partial range
    let out = downloader
        .download(
            &buckets,
            key,
            &Range {
                start: 1000,
                end: 2000,
            },
            &RequestConfig::default(),
        )
        .await
        .unwrap();

    assert_eq!(out.piece.data, test_data.slice(1000..2000));
    assert_eq!(out.piece.object_size, test_data.len() as u64);
}

#[tokio::test]
async fn test_download_no_such_key() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    let bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
    let key = ObjectKey::new("non-existent-key").unwrap();

    let result = downloader
        .download(
            &buckets,
            key,
            &Range { start: 0, end: 100 },
            &RequestConfig::default(),
        )
        .await;

    match result {
        Err(DownloadError::NoSuchKey) => {}
        other => panic!("Expected NoSuchKey error, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_download_range_not_satisfied() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    // Create a small object
    let test_data = Bytes::from_static(&[42u8; 100]);
    let object_key = "test-short-object";
    upload_test_object(&ctx.client, &ctx.bucket_name, object_key, test_data.clone()).await;

    let bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
    let key = ObjectKey::new(object_key).unwrap();

    // Try to download beyond the object size
    // This should succeed and return the available data
    let out = downloader
        .download(
            &buckets,
            key,
            &Range {
                start: 0,
                end: 1000,
            },
            &RequestConfig::default(),
        )
        .await
        .unwrap();

    // Should return the 100 bytes that exist
    assert_eq!(out.piece.data, test_data);
    assert_eq!(out.piece.object_size, 100);
}

#[tokio::test]
async fn test_download_page_sized_range_from_small_object() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    // Create a small object (100 bytes)
    let test_data = Bytes::from_static(&[42u8; 100]);
    let object_key = "small-object";
    upload_test_object(&ctx.client, &ctx.bucket_name, object_key, test_data.clone()).await;

    let bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
    let key = ObjectKey::new(object_key).unwrap();

    let out = downloader
        .download(
            &buckets,
            key,
            &Range {
                start: 0,
                end: PAGE_SIZE,
            },
            &RequestConfig::default(),
        )
        .await
        .unwrap();

    assert_eq!(out.piece.data.len(), 100);
    assert_eq!(out.piece.data, test_data);
    assert_eq!(out.piece.object_size, 100);
}

#[tokio::test]
async fn test_download_with_fallback_bucket() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    // Create a second bucket for fallback
    let fallback_bucket_name = "fallback-bucket";
    ctx.client
        .create_bucket()
        .bucket(fallback_bucket_name)
        .send()
        .await
        .expect("Failed to create fallback bucket");

    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize + 200);
    test_data.fill(99u8);
    let test_data = test_data.freeze();
    let object_key = "fallback-object.txt";

    // Upload object only to fallback bucket
    upload_test_object(
        &ctx.client,
        fallback_bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let primary_bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let fallback_bucket = BucketName::new(fallback_bucket_name).unwrap();
    let buckets =
        BucketNameSet::new([primary_bucket.clone(), fallback_bucket.clone()].into_iter()).unwrap();
    let key = ObjectKey::new(object_key).unwrap();

    // Should fail on primary bucket and succeed on fallback
    let out = downloader
        .download(
            &buckets,
            key,
            &Range {
                start: 0,
                end: test_data.len() as u64,
            },
            &RequestConfig::default(),
        )
        .await
        .unwrap();

    assert_eq!(out.piece.data, test_data);
    assert_eq!(out.used_bucket_idx, 1);
}

#[tokio::test]
async fn test_fallback_bucket_circuit_breaker_recovers_for_new_primary_failures() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    let fallback_bucket_name = "recovery-fallback-bucket";
    ctx.client
        .create_bucket()
        .bucket(fallback_bucket_name)
        .send()
        .await
        .expect("Failed to create fallback bucket");

    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize + 200);
    test_data.fill(17u8);
    let test_data = test_data.freeze();
    let object_key = "recovery-fallback-object.txt";
    upload_test_object(
        &ctx.client,
        fallback_bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let primary_bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let fallback_bucket = BucketName::new(fallback_bucket_name).unwrap();
    let all_buckets =
        BucketNameSet::new([primary_bucket.clone(), fallback_bucket.clone()].into_iter()).unwrap();
    let primary_only = BucketNameSet::new(std::iter::once(primary_bucket.clone())).unwrap();
    let key = ObjectKey::new(object_key).unwrap();
    let range = Range {
        start: 0,
        end: test_data.len() as u64,
    };

    let mut circuit_breaker_open = false;
    for _ in 0..10 {
        let result = downloader
            .download(
                &primary_only,
                key.clone(),
                &range,
                &RequestConfig::default(),
            )
            .await;
        assert!(matches!(result, Err(DownloadError::NoSuchKey)));
        if bucket_metrics(&downloader, &primary_bucket).circuit_breaker_open {
            circuit_breaker_open = true;
            break;
        }
    }
    assert!(
        circuit_breaker_open,
        "primary bucket circuit breaker should open after repeated failures"
    );

    let open_order = downloader
        .download(&all_buckets, key.clone(), &range, &RequestConfig::default())
        .await
        .unwrap();
    assert_eq!(open_order.primary_bucket_idx, 1);
    assert_eq!(open_order.secondary_bucket_idx, Some(0));
    assert_eq!(open_order.used_bucket_idx, 1);

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;

    let recovered_metrics = bucket_metrics(&downloader, &primary_bucket);
    assert!(!recovered_metrics.circuit_breaker_open);
    assert_eq!(recovered_metrics.consecutive_failures, 0);

    let primary_only_result = downloader
        .download(&primary_only, key, &range, &RequestConfig::default())
        .await;
    assert!(matches!(primary_only_result, Err(DownloadError::NoSuchKey)));

    let post_recovery_metrics = bucket_metrics(&downloader, &primary_bucket);
    assert_eq!(post_recovery_metrics.consecutive_failures, 1);
    assert!(!post_recovery_metrics.circuit_breaker_open);
}

#[tokio::test]
async fn test_download_multiple_ranges_same_object() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize);
    test_data.fill(123u8);
    let test_data = test_data.freeze();
    let object_key = "multi-range-object.txt";
    upload_test_object(&ctx.client, &ctx.bucket_name, object_key, test_data.clone()).await;

    let bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
    let key = ObjectKey::new(object_key).unwrap();

    // Download multiple non-overlapping ranges
    let ranges = vec![
        Range { start: 0, end: 100 },
        Range {
            start: 100,
            end: 200,
        },
        Range {
            start: 500,
            end: 600,
        },
    ];

    for range in ranges {
        let out = downloader
            .download(&buckets, key.clone(), &range, &RequestConfig::default())
            .await
            .unwrap();

        let expected_data = test_data.slice(range.start as usize..range.end as usize);
        assert_eq!(out.piece.data, expected_data);
        assert_eq!(out.piece.object_size, test_data.len() as u64);
    }
}

#[tokio::test]
async fn test_download_with_hedged_requests() {
    let ctx = setup_rustfs().await;

    // Lower quantile for more aggressive hedging
    let downloader = make_downloader(ctx.client.clone(), 0.5);

    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize);
    test_data.fill(77u8);
    let test_data = test_data.freeze();
    let object_key = "hedge-test-object.txt";
    upload_test_object(&ctx.client, &ctx.bucket_name, object_key, test_data.clone()).await;

    let bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
    let key = ObjectKey::new(object_key).unwrap();

    // Make multiple requests to build up latency statistics
    for _ in 0..10 {
        let out = downloader
            .download(
                &buckets,
                key.clone(),
                &Range {
                    start: 0,
                    end: test_data.len() as u64,
                },
                &RequestConfig::default(),
            )
            .await
            .unwrap();

        assert_eq!(out.piece.data, test_data.clone());
    }
}

#[tokio::test]
#[should_panic(expected = "assertion failed")]
async fn test_download_empty_range() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    let bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
    let key = ObjectKey::new("any-key").unwrap();

    let _ = downloader
        .download(
            &buckets,
            key,
            &Range { start: 10, end: 10 },
            &RequestConfig::default(),
        )
        .await;
}

#[tokio::test]
async fn test_download_last_byte() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    let mut test_data = BytesMut::zeroed(1024);
    test_data.fill(88u8);
    let test_data = test_data.freeze();
    let object_key = "last-byte-object.bin";
    upload_test_object(&ctx.client, &ctx.bucket_name, object_key, test_data.clone()).await;

    let bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
    let key = ObjectKey::new(object_key).unwrap();

    // Download just the last byte
    let out = downloader
        .download(
            &buckets,
            key,
            &Range {
                start: 1023,
                end: 1024,
            },
            &RequestConfig::default(),
        )
        .await
        .unwrap();

    assert_eq!(out.piece.data, Bytes::from_static(&[88u8]));
    assert_eq!(out.piece.object_size, 1024);
}

#[tokio::test]
async fn test_small_object_full_range() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    let test_data = b"Hello, this is a small test object!";
    let object_key = "small-object.txt";
    upload_test_object(
        &ctx.client,
        &ctx.bucket_name,
        object_key,
        Bytes::from_static(test_data),
    )
    .await;

    let bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
    let key = ObjectKey::new(object_key).unwrap();

    let out = downloader
        .download(
            &buckets,
            key,
            &Range {
                start: 0,
                end: test_data.len() as u64,
            },
            &RequestConfig::default(),
        )
        .await
        .unwrap();

    assert_eq!(out.piece.data, Bytes::from_static(test_data));
    assert_eq!(out.piece.object_size, test_data.len() as u64);
}

#[tokio::test]
async fn test_small_object_partial_range() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    let test_data = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let object_key = "small-range-object.txt";
    upload_test_object(
        &ctx.client,
        &ctx.bucket_name,
        object_key,
        Bytes::from_static(test_data),
    )
    .await;

    let bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
    let key = ObjectKey::new(object_key).unwrap();

    let out = downloader
        .download(
            &buckets,
            key,
            &Range { start: 10, end: 20 },
            &RequestConfig::default(),
        )
        .await
        .unwrap();

    assert_eq!(out.piece.data, Bytes::from(&test_data[10..20]));
    assert_eq!(out.piece.object_size, test_data.len() as u64);
}

#[tokio::test]
async fn test_1kb_object() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    let mut test_data = BytesMut::zeroed(1024);
    test_data.fill(42u8);
    let test_data = test_data.freeze();
    let object_key = "1kb-object.bin";
    upload_test_object(&ctx.client, &ctx.bucket_name, object_key, test_data.clone()).await;

    let bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
    let key = ObjectKey::new(object_key).unwrap();

    let out = downloader
        .download(
            &buckets,
            key,
            &Range {
                start: 0,
                end: test_data.len() as u64,
            },
            &RequestConfig::default(),
        )
        .await
        .unwrap();

    assert_eq!(out.piece.data.len(), test_data.len());
    assert_eq!(out.piece.data, test_data);
    assert_eq!(out.piece.object_size, 1024);
}

#[tokio::test]
async fn test_100kb_object_partial_range() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone(), 0.9);

    let mut test_data = BytesMut::zeroed(100 * 1024);
    test_data.fill(99u8);
    let test_data = test_data.freeze();
    let object_key = "100kb-object.bin";
    upload_test_object(&ctx.client, &ctx.bucket_name, object_key, test_data.clone()).await;

    let bucket = BucketName::new(&ctx.bucket_name).unwrap();
    let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
    let key = ObjectKey::new(object_key).unwrap();

    let out = downloader
        .download(
            &buckets,
            key,
            &Range {
                start: 50000,
                end: 60000,
            },
            &RequestConfig::default(),
        )
        .await
        .unwrap();

    assert_eq!(out.piece.data.len(), 10000);
    assert_eq!(out.piece.data, test_data.slice(50000..60000));
    assert_eq!(out.piece.object_size, test_data.len() as u64);
}
