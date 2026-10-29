mod common;

use bytes::{Bytes, BytesMut};
use bytesize::ByteSize;
use cachey::{
    cache::CacheConfig,
    service::{CacheyService, PAGE_SIZE, ServiceConfig},
};
use common::{RustfsTestContext, upload_test_object};
use http_body_util::BodyExt;
use tokio::net::TcpListener;

struct TestContext {
    rustfs: RustfsTestContext,
    server_url: String,
}

async fn setup_test_server() -> TestContext {
    let rustfs = common::setup_rustfs().await;
    let service_config = ServiceConfig {
        cache: CacheConfig {
            memory_size: ByteSize::mib(256),
            disk_cache: None,
            metrics_registry: Some(prometheus::Registry::new()),
        },
        download_limits: cachey::object_store::DownloadLimits::default(),
    };

    let server_handle = axum_server::Handle::new();
    let cachey = CacheyService::new(service_config, rustfs.client.clone(), server_handle)
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

    TestContext { rustfs, server_url }
}

#[tokio::test]
async fn test_fetch_endpoint_head_request() {
    let ctx = setup_test_server().await;

    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize + 500);
    test_data.fill(42u8);
    let test_data = test_data.freeze();
    let object_key = "head-test.txt";
    upload_test_object(
        &ctx.rustfs.client,
        &ctx.rustfs.bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client
        .head(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, ctx.rustfs.bucket_name, object_key
        ))
        .header("Range", "bytes=0-499")
        .send()
        .await
        .expect("Failed to send HEAD request");

    assert_eq!(response.status(), 206);

    assert_eq!(response.headers()["content-length"], "500");

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
            ctx.server_url, ctx.rustfs.bucket_name
        ))
        .header("Range", "bytes=0-100")
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 404);
}

/// An exact read caches its range apart from the page holding it.
#[tokio::test]
async fn exact_reads_are_cached_apart_from_pages() {
    let ctx = setup_test_server().await;
    let data = Bytes::from_static(b"0123456789abcdefghijklmnopqrstuvwxyz");
    let key = "exact-object";
    upload_test_object(
        &ctx.rustfs.client,
        &ctx.rustfs.bucket_name,
        key,
        data.clone(),
    )
    .await;
    let client = reqwest::Client::new();
    let url = format!("{}/fetch/{}/{key}", ctx.server_url, ctx.rustfs.bucket_name);

    for exact in [true, false] {
        let response = client
            .get(&url)
            .header("Range", "bytes=4-9")
            .header("C0-Exact", exact.to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 206);
        let status = response.headers()["c0-status"].to_str().unwrap().to_owned();
        assert!(status.ends_with("; 0"), "exact: {exact}, status: {status}");
        assert_eq!(response.bytes().await.unwrap(), data.slice(4..10));
    }
}

#[tokio::test]
async fn cached_object_serves_ranges_after_backend_deletion() {
    let ctx = setup_test_server().await;
    let data = Bytes::from_static(b"0123456789abcdefghijklmnopqrstuvwxyz");
    let key = "cached-object";
    upload_test_object(
        &ctx.rustfs.client,
        &ctx.rustfs.bucket_name,
        key,
        data.clone(),
    )
    .await;
    let client = reqwest::Client::new();
    let url = format!("{}/fetch/{}/{key}", ctx.server_url, ctx.rustfs.bucket_name);

    for (index, range) in [10..20, 0..data.len(), 10..PAGE_SIZE as usize]
        .into_iter()
        .enumerate()
    {
        let response = client
            .get(&url)
            .header("Range", format!("bytes={}-{}", range.start, range.end - 1))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 206);
        assert_eq!(
            response.headers()["content-type"],
            "application/octet-stream"
        );
        assert_eq!(
            response.headers()["content-range"],
            format!(
                "bytes {}-{}/{}",
                range.start,
                range.end.min(data.len()) - 1,
                data.len()
            )
        );
        assert_eq!(
            response.bytes().await.unwrap(),
            data.slice(range.start..range.end.min(data.len()))
        );

        if index == 0 {
            ctx.rustfs
                .client
                .delete_object()
                .bucket(&ctx.rustfs.bucket_name)
                .key(key)
                .send()
                .await
                .unwrap();
        }
    }

    let response = client
        .get(&url)
        .header("Range", format!("bytes={0}-{0}", data.len()))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 416);
    assert_eq!(
        response.headers()["content-range"],
        format!("bytes */{}", data.len())
    );
}

#[tokio::test]
async fn test_fetch_endpoint_range_ending_at_page_boundary() {
    let ctx = setup_test_server().await;

    let mut test_data = BytesMut::zeroed(PAGE_SIZE as usize);
    for (i, byte) in test_data.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    let test_data = test_data.freeze();
    let object_key = "exact-page-size.bin";
    upload_test_object(
        &ctx.rustfs.client,
        &ctx.rustfs.bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, ctx.rustfs.bucket_name, object_key
        ))
        .header("Range", format!("bytes=0-{}", PAGE_SIZE - 1))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 206);

    let body = response
        .bytes()
        .await
        .expect("Failed to read response body");
    assert_eq!(body, test_data);
}

#[tokio::test]
async fn test_small_object_range_start_beyond_end_returns_416() {
    let ctx = setup_test_server().await;

    let test_data = Bytes::from(vec![42u8; 100 * 1024]);
    let object_key = "small-object-range-beyond-end.bin";
    upload_test_object(
        &ctx.rustfs.client,
        &ctx.rustfs.bucket_name,
        object_key,
        test_data,
    )
    .await;

    let start = PAGE_SIZE;
    let end = PAGE_SIZE + 1000;
    let client = reqwest::Client::new();
    let response = client
        .get(format!(
            "{}/fetch/{}/{}",
            ctx.server_url, ctx.rustfs.bucket_name, object_key
        ))
        .header("Range", format!("bytes={start}-{end}"))
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 416);
}

#[tokio::test]
async fn test_fetch_endpoint_multi_page_range() {
    let ctx = setup_test_server().await;
    let data = Bytes::from(
        (0..3 * PAGE_SIZE)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let key = "multi-page-object.bin";
    upload_test_object(
        &ctx.rustfs.client,
        &ctx.rustfs.bucket_name,
        key,
        data.clone(),
    )
    .await;

    let start = PAGE_SIZE / 2;
    let client = reqwest::Client::new();
    let url = format!("{}/fetch/{}/{key}", ctx.server_url, ctx.rustfs.bucket_name);
    for end in [PAGE_SIZE + PAGE_SIZE / 2, 2 * PAGE_SIZE + PAGE_SIZE / 2] {
        let response = client
            .get(&url)
            .header("Range", format!("bytes={start}-{}", end - 1))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 206);
        assert_eq!(
            response.bytes().await.unwrap(),
            data.slice(start as usize..end as usize)
        );
    }
}

#[tokio::test]
async fn test_fetch_endpoint_multi_page_trailers_past_eof() {
    let ctx = setup_test_server().await;

    let object_size = 2 * PAGE_SIZE as usize + 123;
    let mut test_data = BytesMut::zeroed(object_size);
    for (i, byte) in test_data.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    let test_data = test_data.freeze();
    let object_key = "multi-page-trailers.bin";
    upload_test_object(
        &ctx.rustfs.client,
        &ctx.rustfs.bucket_name,
        object_key,
        test_data.clone(),
    )
    .await;

    let uri = format!(
        "{}/fetch/{}/{}",
        ctx.server_url, ctx.rustfs.bucket_name, object_key
    )
    .parse::<hyper::Uri>()
    .expect("Failed to parse URI");

    let req = hyper::Request::builder()
        .uri(uri)
        .version(hyper::Version::HTTP_2)
        .header(
            "Range",
            format!("bytes=0-{}", object_size + PAGE_SIZE as usize - 1),
        )
        .body(http_body_util::Empty::<Bytes>::new())
        .expect("Failed to build request");

    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .http2_only(true)
        .build_http();

    let response = client.request(req).await.expect("Failed to send request");

    assert_eq!(response.status(), 206);
    assert_eq!(response.version(), hyper::Version::HTTP_2);

    assert_eq!(
        response.headers()["content-range"],
        format!("bytes 0-{}/{object_size}", object_size - 1)
    );
    assert_eq!(
        response.headers()["c0-status"],
        format!("0-{}; {}; 0", PAGE_SIZE - 1, ctx.rustfs.bucket_name)
    );

    let (_parts, body) = response.into_parts();
    let collected = body.collect().await.expect("Failed to collect body");

    let trailers = collected
        .trailers()
        .cloned()
        .expect("Expected trailers to be present in HTTP/2 response");

    let body_bytes = collected.to_bytes();
    assert_eq!(body_bytes, test_data);

    let statuses: Vec<_> = trailers
        .get_all("c0-status")
        .iter()
        .map(|value| value.to_str().expect("valid status"))
        .collect();
    let expected: Vec<_> = (1..3)
        .map(|page| {
            format!(
                "{}-{}; {}; 0",
                page * PAGE_SIZE,
                ((page + 1) * PAGE_SIZE).min(object_size as u64) - 1,
                ctx.rustfs.bucket_name
            )
        })
        .collect();
    assert_eq!(statuses, expected);
}
