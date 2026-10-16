#![cfg(unix)]

use std::{
    fs::{self, File},
    net::TcpListener,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, Response, StatusCode},
    routing::get,
};
use bytes::Bytes;
use cachey::service::PAGE_SIZE;
use tokio::{
    process::{Child, Command},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock predates the epoch")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("cachey-shutdown-{}-{nonce}", std::process::id()));
        fs::create_dir(&path).expect("create test directory");
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone)]
struct MockS3State {
    data: Bytes,
    available: Arc<AtomicBool>,
    requests: Arc<AtomicUsize>,
}

struct MockS3 {
    endpoint: String,
    state: MockS3State,
    task: JoinHandle<()>,
}

impl MockS3 {
    async fn start() -> Self {
        let state = MockS3State {
            data: (0..1024 * 1024 + 17)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>()
                .into(),
            available: Arc::new(AtomicBool::new(true)),
            requests: Arc::new(AtomicUsize::new(0)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock S3");
        let endpoint = format!("http://{}", listener.local_addr().expect("mock S3 address"));
        let app = Router::new()
            .route("/bucket/{*key}", get(mock_s3_get))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve mock S3");
        });
        Self {
            endpoint,
            state,
            task,
        }
    }
}

impl Drop for MockS3 {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn mock_s3_get(State(state): State<MockS3State>, headers: HeaderMap) -> Response<Body> {
    state.requests.fetch_add(1, Ordering::Relaxed);
    if !state.available.load(Ordering::Relaxed) {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("content-type", "application/xml")
            .body(Body::from("<Error><Code>NoSuchKey</Code></Error>"))
            .expect("missing object response");
    }
    assert_eq!(headers["range"], format!("bytes=0-{}", PAGE_SIZE - 1));
    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header("content-length", state.data.len())
        .header(
            "content-range",
            format!("bytes 0-{}/{}", state.data.len() - 1, state.data.len()),
        )
        .header("last-modified", "Tue, 14 Nov 2023 22:13:20 GMT")
        .header("etag", "\"shutdown-fixture\"")
        .body(Body::from(state.data))
        .expect("object response")
}

struct CacheyProcess {
    child: Child,
    url: String,
    log_path: PathBuf,
}

impl CacheyProcess {
    async fn start(directory: &TestDirectory, endpoint: &str, disk_kind: Option<&str>) -> Self {
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("reserve server port")
            .local_addr()
            .expect("server address")
            .port();
        let log_path = directory.0.join(format!("server-{port}.log"));
        let log = File::create(&log_path).expect("create server log");
        let mut command = Command::new(env!("CARGO_BIN_EXE_server"));
        command
            .args(["--port", &port.to_string(), "--memory", "128MiB"])
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_ENDPOINT_URL", endpoint)
            .env("AWS_ENDPOINT_URL_S3", endpoint)
            .env("AWS_CONFIG_FILE", directory.0.join("unused-config"))
            .env(
                "AWS_SHARED_CREDENTIALS_FILE",
                directory.0.join("unused-credentials"),
            )
            .env_remove("AWS_PROFILE")
            .env_remove("AWS_SESSION_TOKEN")
            .env("TOKIO_WORKER_THREADS", "2")
            .stdout(log.try_clone().expect("clone server log"))
            .stderr(log)
            .kill_on_drop(true);
        if let Some(kind) = disk_kind {
            command
                .arg("--disk-path")
                .arg(directory.0.join("cache"))
                .args(["--disk-kind", kind, "--disk-capacity", "256MiB"]);
        }
        let mut process = Self {
            child: command.spawn().expect("start Cachey CLI"),
            url: format!("http://127.0.0.1:{port}"),
            log_path,
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .expect("readiness client");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = process.child.try_wait().expect("poll Cachey CLI") {
                panic!(
                    "Cachey exited before readiness: {status}\n{}",
                    process.logs()
                );
            }
            if let Ok(response) = client.get(format!("{}/metrics", process.url)).send().await
                && response.status().is_success()
            {
                return process;
            }
            assert!(
                Instant::now() < deadline,
                "Cachey readiness timed out\n{}",
                process.logs()
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    fn logs(&self) -> String {
        fs::read_to_string(&self.log_path).expect("read server log")
    }

    async fn stop(&mut self, signal: &str) {
        let pid = self.child.id().expect("Cachey is still running");
        let signaled = Command::new("kill")
            .args([signal, &pid.to_string()])
            .status()
            .await
            .expect("send shutdown signal");
        assert!(signaled.success(), "send {signal} to Cachey");
        let status = timeout(Duration::from_secs(35), self.child.wait())
            .await
            .unwrap_or_else(|_| panic!("Cachey shutdown timed out\n{}", self.logs()))
            .expect("wait for Cachey exit");
        assert!(status.success(), "Cachey exited {status}\n{}", self.logs());
    }
}

async fn assert_disk_cache_survives_shutdown(disk_kind: &str, signal: &str) {
    let directory = TestDirectory::new();
    let backend = MockS3::start().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("fetch client");
    let mut server = CacheyProcess::start(&directory, &backend.endpoint, Some(disk_kind)).await;
    for index in 0..8 {
        let response = client
            .get(format!("{}/fetch/shutdown/object-{index}", server.url))
            .header("range", "bytes=0-1023")
            .header("c0-bucket", "bucket")
            .send()
            .await
            .expect("warm cache");
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.bytes().await.unwrap(),
            backend.state.data.slice(..1024)
        );
    }
    assert_eq!(backend.state.requests.load(Ordering::Relaxed), 8);
    server.stop(signal).await;
    backend.state.available.store(false, Ordering::Relaxed);

    let mut restarted = CacheyProcess::start(&directory, &backend.endpoint, Some(disk_kind)).await;
    for index in 0..8 {
        let response = client
            .get(format!("{}/fetch/shutdown/object-{index}", restarted.url))
            .header("range", format!("bytes=0-{}", backend.state.data.len() - 1))
            .header("c0-bucket", "bucket")
            .send()
            .await
            .expect("read cache after restart");
        assert_eq!(
            response.status(),
            StatusCode::PARTIAL_CONTENT,
            "object {index} missing after {signal} with {disk_kind} cache\n{}\n{}",
            server.logs(),
            restarted.logs()
        );
        assert_eq!(response.bytes().await.unwrap(), backend.state.data);
    }
    assert_eq!(backend.state.requests.load(Ordering::Relaxed), 8);
    restarted.stop(signal).await;
}

#[tokio::test]
async fn sigterm_closes_filesystem_cache_before_exit() {
    assert_disk_cache_survives_shutdown("fs", "-TERM").await;
}

#[tokio::test]
async fn sigint_closes_filesystem_cache_before_exit() {
    assert_disk_cache_survives_shutdown("fs", "-INT").await;
}

#[tokio::test]
async fn sigterm_closes_block_file_cache_before_exit() {
    assert_disk_cache_survives_shutdown("block", "-TERM").await;
}

#[tokio::test]
async fn sigint_closes_block_file_cache_before_exit() {
    assert_disk_cache_survives_shutdown("block", "-INT").await;
}

#[tokio::test]
async fn memory_only_server_shuts_down_cleanly() {
    let directory = TestDirectory::new();
    let backend = MockS3::start().await;
    for signal in ["-TERM", "-INT"] {
        let mut server = CacheyProcess::start(&directory, &backend.endpoint, None).await;
        server.stop(signal).await;
    }
}
