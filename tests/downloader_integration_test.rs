mod common;

use std::sync::Arc;

use bytes::Bytes;
use cachey::{
    object_store::{DownloadError, DownloadLimits, Downloader, RequestConfig},
    service::{PAGE_SIZE, SlidingThroughput},
    types::{BucketName, BucketNameSet, ObjectKey},
};
use common::{setup_rustfs, upload_test_object};
use parking_lot::Mutex;

fn make_downloader(client: aws_sdk_s3::Client) -> Downloader {
    let throughput = Arc::new(Mutex::new(SlidingThroughput::default()));
    Downloader::new(client, DownloadLimits::default(), throughput).unwrap()
}

#[tokio::test]
async fn downloads_ranges_and_reports_object_boundaries() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone());
    let buckets = BucketNameSet::from(BucketName::new(&ctx.bucket_name).unwrap());
    for size in [1024, PAGE_SIZE + 100] {
        let data = Bytes::from(
            (0..size)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let key = ObjectKey::new(format!("object-{size}")).unwrap();
        upload_test_object(&ctx.client, &ctx.bucket_name, &key, data.clone()).await;
        for range in [0..size, 10..20, size - 1..size, 0..PAGE_SIZE] {
            let output = downloader
                .download(&buckets, key.clone(), &range, &RequestConfig::default())
                .await
                .unwrap();
            assert_eq!(
                output.piece.data,
                data.slice(range.start as usize..range.end.min(size) as usize),
                "object size {size}, range {range:?}"
            );
            assert_eq!(output.piece.object_size, size);
            assert!(output.piece.mtime > 0);
        }
        let range = size..size + 1;
        let error = downloader
            .download(&buckets, key, &range, &RequestConfig::default())
            .await
            .unwrap_err();
        assert!(
            matches!(error, DownloadError::RangeNotSatisfied { requested, .. } if requested == range)
        );
    }
}

#[tokio::test]
async fn missing_primary_falls_back_without_poisoning_health() {
    let ctx = setup_rustfs().await;
    let downloader = make_downloader(ctx.client.clone());
    let peer = "fallback-bucket";
    ctx.client
        .create_bucket()
        .bucket(peer)
        .send()
        .await
        .unwrap();
    let data = Bytes::from_static(b"replicated object");
    let key = ObjectKey::new("object").unwrap();
    upload_test_object(&ctx.client, peer, &key, data.clone()).await;
    let primary = BucketName::new(&ctx.bucket_name).unwrap();
    let primary_only = BucketNameSet::from(primary.clone());
    let range = 0..PAGE_SIZE;
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
    }
    let mut primary_metrics = None;
    downloader.observe_bucket_metrics(|bucket, metrics| {
        if *bucket == primary {
            primary_metrics = Some(metrics.clone());
        }
    });
    let metrics = primary_metrics.unwrap();
    assert!(!metrics.deprioritized);
    assert_eq!(metrics.consecutive_failures, 0);
    assert!(metrics.error_rate.abs() < f64::EPSILON);

    let buckets =
        BucketNameSet::new([primary, BucketName::new(peer).unwrap()].into_iter()).unwrap();
    let output = downloader
        .download(&buckets, key, &range, &RequestConfig::default())
        .await
        .unwrap();
    assert_eq!(output.primary_bucket_idx, 0);
    assert_eq!(output.used_bucket_idx, 1);
    assert_eq!(output.piece.data, data);
}
