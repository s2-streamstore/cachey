use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use bytes::{Bytes, BytesMut};
use bytesize::ByteSize;
use cachey::{
    cache::CacheConfig,
    service::{CacheyService, PAGE_SIZE, ServiceConfig},
};
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;
use tokio::net::TcpListener;

struct TestContext {
    _container: ContainerAsync<MinIO>,
    s3_client: aws_sdk_s3::Client,
    bucket_name: String,
    server_url: String,
}

async fn setup_test_server() -> TestContext {
    let container = MinIO::default()
        .start()
        .await
        .expect("Failed to start MinIO container");

    let host_port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("Failed to get host port");

    let endpoint = format!("http://127.0.0.1:{host_port}");

    let creds = Credentials::new("minioadmin", "minioadmin", None, None, "test");

    let s3_config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(creds)
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .build();

    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

    let bucket_name = "test-bucket";
    s3_client
        .create_bucket()
        .bucket(bucket_name)
        .send()
        .await
        .expect("Failed to create bucket");

    let service_config = ServiceConfig {
        cache: CacheConfig {
            memory_size: ByteSize::mib(256),
            disk_cache: None,
            iouring: false,
        },
        hedge_quantile: 0.99,
    };

    let server_handle = axum_server::Handle::new();
    let cachey = CacheyService::new(service_config, s3_client.clone(), server_handle)
        .await
        .expect("Failed to create cache service");

    let app = cachey.into_router();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind to port");
    let addr = listener.local_addr().expect("Failed to get local addr");
    let server_url = format!("http://{addr}");

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("Failed to start server");
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    TestContext {
        _container: container,
        s3_client,
        bucket_name: bucket_name.to_string(),
        server_url,
    }
}

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

#[tokio::test]
async fn test_fetch_endpoint_full_object() {
    let ctx = setup_test_server().await;

    // Larger than PAGE_SIZE for multiple pages
    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize + 100);
    for (i, byte) in test_data.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    let test_data = test_data.freeze();
    let object_key = "test-object.txt";
    upload_test_object(
        &ctx.s3_client,
        &ctx.bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let client = reqwest::Client::new();

    // Request a small range from the beginning
    let response = client
        .get(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, &ctx.bucket_name, object_key
        ))
        .header("Range", "bytes=0-99")
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 206);

    let content_type = response
        .headers()
        .get("content-type")
        .expect("Missing content-type header");
    assert_eq!(content_type, "application/octet-stream");

    let body = response
        .bytes()
        .await
        .expect("Failed to read response body");
    assert_eq!(body, test_data.slice(0..100));
}

#[tokio::test]
async fn test_fetch_endpoint_partial_range() {
    let ctx = setup_test_server().await;

    // Larger than PAGE_SIZE for multiple pages
    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize + 1000);
    for (i, byte) in test_data.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    let test_data = test_data.freeze();
    let object_key = "range-test.txt";
    upload_test_object(
        &ctx.s3_client,
        &ctx.bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, &ctx.bucket_name, object_key
        ))
        .header("Range", "bytes=1000-1999")
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 206);

    let body = response
        .bytes()
        .await
        .expect("Failed to read response body");
    assert_eq!(body, test_data.slice(1000..2000));
}

#[tokio::test]
async fn test_fetch_endpoint_head_request() {
    let ctx = setup_test_server().await;

    // Create a large object
    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize + 500);
    test_data.fill(42u8);
    let test_data = test_data.freeze();
    let object_key = "head-test.txt";
    upload_test_object(
        &ctx.s3_client,
        &ctx.bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client
        .head(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, &ctx.bucket_name, object_key
        ))
        .header("Range", "bytes=0-499")
        .send()
        .await
        .expect("Failed to send HEAD request");

    assert_eq!(response.status(), 206);

    let content_length = response
        .headers()
        .get("content-length")
        .expect("Missing content-length header")
        .to_str()
        .expect("Invalid content-length")
        .parse::<usize>()
        .expect("Failed to parse content-length");

    assert_eq!(content_length, 500);

    let body = response
        .bytes()
        .await
        .expect("Failed to read response body");
    assert!(body.is_empty());
}

#[tokio::test]
async fn test_fetch_endpoint_missing_range_header() {
    let ctx = setup_test_server().await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/fetch/test-bucket/some-object", ctx.server_url))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 400);
    let body = response.text().await.expect("Failed to read response body");
    assert_eq!(body, "Range header is required");
}

#[tokio::test]
async fn test_fetch_endpoint_invalid_range() {
    let ctx = setup_test_server().await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/fetch/test-bucket/some-object", ctx.server_url))
        .header("Range", "bytes=invalid")
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 416);
}

#[tokio::test]
async fn test_fetch_endpoint_not_found() {
    let ctx = setup_test_server().await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!(
            "{}/fetch/{}/non-existent-object",
            ctx.server_url, &ctx.bucket_name
        ))
        .header("Range", "bytes=0-100")
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn test_fetch_endpoint_with_custom_bucket_header() {
    let ctx = setup_test_server().await;

    // Create a large object
    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize + 200);
    test_data.fill(99u8);
    let test_data = test_data.freeze();
    let object_key = "custom-bucket-test.txt";
    upload_test_object(
        &ctx.s3_client,
        &ctx.bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!(
            "{}/fetch/ignored-kind/{}",
            ctx.server_url, object_key
        ))
        .header("Range", "bytes=0-199")
        .header("c0-bucket", &ctx.bucket_name)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 206);

    let body = response
        .bytes()
        .await
        .expect("Failed to read response body");
    assert_eq!(body, test_data.slice(0..200));
}

#[tokio::test]
async fn test_metrics_endpoint() {
    let ctx = setup_test_server().await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/metrics", ctx.server_url))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 200);

    let body = response.text().await.expect("Failed to read response body");

    // The metrics endpoint returns prometheus format metrics
    // Even empty, it should return something (at least an empty string from prometheus)
    // Let's just check that the endpoint responds successfully
    // The body might be empty if no metrics have been registered yet
    println!("Metrics body length: {}", body.len());
    if !body.is_empty() {
        println!("Metrics body preview: {}", &body[..body.len().min(200)]);
    }
}

#[tokio::test]
async fn test_fetch_endpoint_cache_hit() {
    let ctx = setup_test_server().await;

    // Create a large object that spans exactly one page
    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize);
    test_data.fill(77u8);
    let test_data = test_data.freeze();
    let object_key = "cache-test.txt";
    upload_test_object(
        &ctx.s3_client,
        &ctx.bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let client = reqwest::Client::new();

    // First request - cache miss, request first 1000 bytes
    let response1 = client
        .get(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, &ctx.bucket_name, object_key
        ))
        .header("Range", "bytes=0-999")
        .send()
        .await
        .expect("Failed to send first request");

    assert_eq!(response1.status(), 206);
    let body1 = response1
        .bytes()
        .await
        .expect("Failed to read first response");
    assert_eq!(body1, test_data.slice(0..1000));

    // Small delay to ensure cache is populated
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second request - should be a cache hit for the same range
    let response2 = client
        .get(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, &ctx.bucket_name, object_key
        ))
        .header("Range", "bytes=500-1499")
        .send()
        .await
        .expect("Failed to send second request");

    assert_eq!(response2.status(), 206);
    let body2 = response2
        .bytes()
        .await
        .expect("Failed to read second response");
    assert_eq!(body2, test_data.slice(500..1500));
}

#[tokio::test]
async fn test_fetch_endpoint_concurrent_requests() {
    let ctx = setup_test_server().await;

    // Create a large object
    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize);
    test_data.fill(123u8);
    let test_data = test_data.freeze();
    let object_key = "concurrent-test.txt";
    upload_test_object(
        &ctx.s3_client,
        &ctx.bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let client = reqwest::Client::new();
    let server_url = ctx.server_url.clone();
    let bucket_name = ctx.bucket_name.clone();

    let mut handles = vec![];
    for i in 0..5 {
        let client = client.clone();
        let server_url = server_url.clone();
        let bucket_name = bucket_name.clone();
        let object_key = object_key.to_string();
        let test_data = test_data.clone();

        let handle = tokio::spawn(async move {
            // Each request asks for a different range
            let start = i * 100;
            let end = start + 99;
            let response = client
                .get(format!("{server_url}/fetch/{bucket_name}/{object_key}"))
                .header("Range", format!("bytes={start}-{end}"))
                .send()
                .await
                .expect("Failed to send concurrent request");

            assert_eq!(response.status(), 206);
            let body = response
                .bytes()
                .await
                .expect("Failed to read concurrent response");
            assert_eq!(body, test_data.slice(start..=end));
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("Concurrent request failed");
    }
}

#[tokio::test]
async fn test_small_object_full_range() {
    let ctx = setup_test_server().await;

    // Create a small object (much smaller than PAGE_SIZE)
    let test_data = b"Hello, this is a small test object!";
    let object_key = "small-object.txt";
    upload_test_object(
        &ctx.s3_client,
        &ctx.bucket_name,
        object_key,
        Bytes::from_static(test_data),
    )
    .await;

    let client = reqwest::Client::new();

    // Request the full small object
    let response = client
        .get(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, &ctx.bucket_name, object_key
        ))
        .header("Range", format!("bytes=0-{}", test_data.len() - 1))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 206);

    let body = response
        .bytes()
        .await
        .expect("Failed to read response body");
    assert_eq!(body, Bytes::from_static(test_data));
}

#[tokio::test]
async fn test_small_object_partial_range() {
    let ctx = setup_test_server().await;

    // Create a small object
    let test_data = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let object_key = "small-range-object.txt";
    upload_test_object(
        &ctx.s3_client,
        &ctx.bucket_name,
        object_key,
        Bytes::from_static(test_data),
    )
    .await;

    let client = reqwest::Client::new();

    // Request a partial range from the small object
    let response = client
        .get(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, &ctx.bucket_name, object_key
        ))
        .header("Range", "bytes=10-19")
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 206);

    let body = response
        .bytes()
        .await
        .expect("Failed to read response body");
    assert_eq!(body, Bytes::from(&test_data[10..20]));
}

#[tokio::test]
async fn test_1kb_object() {
    let ctx = setup_test_server().await;

    // Create a 1KB object
    let mut test_data = BytesMut::zeroed(1024);
    test_data.fill(42u8);
    let test_data = test_data.freeze();
    let object_key = "1kb-object.bin";
    upload_test_object(
        &ctx.s3_client,
        &ctx.bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let client = reqwest::Client::new();

    // Request the full 1KB object
    let response = client
        .get(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, &ctx.bucket_name, object_key
        ))
        .header("Range", format!("bytes=0-{}", test_data.len() - 1))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 206);

    let body = response
        .bytes()
        .await
        .expect("Failed to read response body");
    assert_eq!(body.len(), test_data.len());
    assert_eq!(body, test_data);
}

#[tokio::test]
async fn test_100kb_object_partial_range() {
    let ctx = setup_test_server().await;

    // Create a 100KB object (still much smaller than PAGE_SIZE)
    let mut test_data = BytesMut::zeroed(100 * 1024);
    test_data.fill(99u8);
    let test_data = test_data.freeze();
    let object_key = "100kb-object.bin";
    upload_test_object(
        &ctx.s3_client,
        &ctx.bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let client = reqwest::Client::new();

    // Request a range from the middle of the object
    let response = client
        .get(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, &ctx.bucket_name, object_key
        ))
        .header("Range", "bytes=50000-59999")
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 206);

    let body = response
        .bytes()
        .await
        .expect("Failed to read response body");
    assert_eq!(body.len(), 10000);
    assert_eq!(body, test_data.slice(50000..60000));
}

#[tokio::test]
async fn test_fetch_endpoint_multi_page_range() {
    let ctx = setup_test_server().await;

    // Create an object that spans multiple pages (3 * PAGE_SIZE)
    let object_size = 3 * PAGE_SIZE as usize;
    let mut test_data = BytesMut::zeroed(object_size);
    for (i, byte) in test_data.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    let test_data = test_data.freeze();
    let object_key = "multi-page-object.bin";
    upload_test_object(
        &ctx.s3_client,
        &ctx.bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let client = reqwest::Client::new();

    // Request a range that spans across 2 pages
    // Start in the middle of first page, end in the middle of second page
    let start = PAGE_SIZE as usize / 2; // Middle of first page
    let end = PAGE_SIZE as usize + (PAGE_SIZE as usize / 2); // Middle of second page

    let response = client
        .get(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, &ctx.bucket_name, object_key
        ))
        .header("Range", format!("bytes={}-{}", start, end - 1))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 206);

    let body = response
        .bytes()
        .await
        .expect("Failed to read response body");

    assert_eq!(body.len(), end - start);
    assert_eq!(body, test_data.slice(start..end));

    // Test another range that spans all 3 pages
    // Request from middle of first page to middle of third page
    let start2 = PAGE_SIZE as usize / 2;
    let end2 = (2 * PAGE_SIZE as usize) + (PAGE_SIZE as usize / 2);

    let response2 = client
        .get(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, &ctx.bucket_name, object_key
        ))
        .header("Range", format!("bytes={}-{}", start2, end2 - 1))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response2.status(), 206);

    let body2 = response2
        .bytes()
        .await
        .expect("Failed to read response body");

    assert_eq!(body2.len(), end2 - start2);
    assert_eq!(body2, test_data.slice(start2..end2));
}
