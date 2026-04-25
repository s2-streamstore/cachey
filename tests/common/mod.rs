use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};

pub struct RustfsTestContext {
    pub _container: ContainerAsync<GenericImage>,
    pub client: aws_sdk_s3::Client,
    pub bucket_name: String,
}

pub async fn setup_rustfs() -> RustfsTestContext {
    let container = GenericImage::new("rustfs/rustfs", "1.0.0-alpha.98")
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/health/ready")
                .with_port(9000u16.into())
                .with_expected_status_code(200u16),
        ))
        .with_env_var("RUSTFS_ADDRESS", ":9000")
        .with_env_var("RUSTFS_ACCESS_KEY", "rustfsadmin")
        .with_env_var("RUSTFS_SECRET_KEY", "rustfsadmin")
        .with_cmd(["/data"])
        .start()
        .await
        .expect("Failed to start RustFS container");

    let host_port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("Failed to get host port");

    let endpoint = format!("http://127.0.0.1:{host_port}");
    let creds = Credentials::new("rustfsadmin", "rustfsadmin", None, None, "test");

    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(creds)
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .build();

    let client = aws_sdk_s3::Client::from_conf(config);

    let bucket_name = "test-bucket";
    client
        .create_bucket()
        .bucket(bucket_name)
        .send()
        .await
        .expect("Failed to create bucket");

    RustfsTestContext {
        _container: container,
        client,
        bucket_name: bucket_name.to_string(),
    }
}
