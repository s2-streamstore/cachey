use std::{net::SocketAddr, path::PathBuf, time::Duration};

use axum_server::tls_rustls::RustlsConfig;
use bytesize::ByteSize;
use cachey::{
    cache::{CacheConfig, DiskCacheConfig, DiskCacheKind},
    service::{CacheyService, ServiceConfig},
};
use clap::{ArgAction, Args as ArgGroup, Parser};
use tracing::info;

#[derive(ArgGroup, Debug)]
struct DiskCacheGroup {
    /// Path to disk cache storage, which may be a directory or block device
    #[arg(long)]
    disk_path: Option<PathBuf>,

    /// Kind of disk cache, which may be a file system or block device
    #[arg(long, default_value = "fs", requires = "disk_path")]
    disk_kind: DiskCacheKind,

    /// Maximum disk cache capacity (e.g., "100GiB")
    /// If not specified, up to 80% of the available space will be used
    #[arg(long, value_parser = parse_bytes, requires = "disk_path")]
    disk_capacity: Option<ByteSize>,

    /// Use io_uring (if available) for disk IO.
    #[arg(long, action = ArgAction::SetTrue)]
    iouring: bool,
}

#[derive(ArgGroup, Debug, Clone)]
struct TlsConfig {
    /// Use a self-signed certificate for TLS
    #[arg(long, conflicts_with_all = ["tls_cert", "tls_key"])]
    tls_self: bool,

    /// Path to the TLS certificate file (e.g., cert.pem)
    /// Must be used together with --tls-key
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<PathBuf>,

    /// Path to the private key file (e.g., key.pem)
    /// Must be used together with --tls-cert
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Maximum memory to use for cache (e.g., "512MiB", "2GB", "1.5GiB")
    #[arg(long, value_parser = parse_bytes, default_value = "4GiB")]
    memory: ByteSize,

    /// Disk cache configuration
    #[command(flatten)]
    disk_cache: DiskCacheGroup,

    /// Latency quantile for making hedged requests (0.0-1.0, use 0 to disable hedging)
    #[arg(long, default_value = "0.99", value_parser = parse_hedge_quantile)]
    hedge_quantile: f64,

    /// TLS configuration (defaults to plain HTTP if not specified).
    #[command(flatten)]
    tls: TlsConfig,

    /// Port to listen on [default: 443 if HTTPS configured, otherwise 80 for HTTP]
    #[arg(long)]
    port: Option<u16>,
}

fn parse_bytes(s: &str) -> Result<ByteSize, String> {
    s.parse::<ByteSize>().map_err(|e| {
        format!("Invalid memory size: {e}. Use formats like '512MiB', '2GB', '1.5GiB'",)
    })
}

fn parse_hedge_quantile(s: &str) -> Result<f64, String> {
    let value = s.parse::<f64>().map_err(|e| {
        format!("Invalid hedge quantile: {e}. Must be a number between 0.0 and 1.0",)
    })?;

    if !(0.0..=1.0).contains(&value) {
        return Err(format!(
            "Invalid hedge quantile: {value}. Must be between 0.0 and 1.0 (use 0 to disable hedging)"
        ));
    }

    Ok(value)
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let service_config = ServiceConfig {
        cache: CacheConfig {
            memory_size: args.memory,
            disk_cache: if let Some(path) = args.disk_cache.disk_path {
                Some(DiskCacheConfig {
                    path,
                    kind: args.disk_cache.disk_kind,
                    capacity: args.disk_cache.disk_capacity,
                    iouring: args.disk_cache.iouring,
                })
            } else {
                info!("disk cache disabled");
                None
            },
        },
        hedge_quantile: args.hedge_quantile,
    };

    info!(?service_config);

    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&aws_config);

    let addr = {
        let port = args.port.unwrap_or_else(|| {
            if args.tls.tls_self || args.tls.tls_cert.is_some() {
                443
            } else {
                80
            }
        });
        format!("0.0.0.0:{port}")
    };

    let server_handle = axum_server::Handle::new();

    tokio::spawn(shutdown_signal(server_handle.clone()));

    let app = CacheyService::new(service_config, s3_client, server_handle.clone())
        .await?
        .into_router();

    match (args.tls.tls_self, args.tls.tls_cert, args.tls.tls_key) {
        (false, Some(cert_path), Some(key_path)) => {
            info!(
                addr,
                ?cert_path,
                "starting https server with provided certificate"
            );
            let rustls_config = RustlsConfig::from_pem_file(cert_path, key_path).await?;
            axum_server::bind_rustls(addr.parse()?, rustls_config)
                .handle(server_handle)
                .serve(app.into_make_service())
                .await?;
        }
        (true, None, None) => {
            info!(
                addr,
                "starting https server with self-signed certificate, clients will need to use --insecure"
            );
            let rcgen::CertifiedKey { cert, signing_key } = rcgen::generate_simple_self_signed([
                "localhost".to_string(),
                "127.0.0.1".to_string(),
                "::1".to_string(),
            ])?;
            let rustls_config = RustlsConfig::from_pem(
                cert.pem().into_bytes(),
                signing_key.serialize_pem().into_bytes(),
            )
            .await?;
            axum_server::bind_rustls(addr.parse()?, rustls_config)
                .handle(server_handle)
                .serve(app.into_make_service())
                .await?;
        }
        (false, None, None) => {
            info!(addr, "starting plain http server");
            axum_server::bind(addr.parse()?)
                .handle(server_handle)
                .serve(app.into_make_service())
                .await?;
        }
        _ => {
            // This shouldn't happen due to clap validation...
            return Err(eyre::eyre!("Invalid TLS configuration"));
        }
    }

    Ok(())
}

async fn shutdown_signal(handle: axum_server::Handle<SocketAddr>) {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("ctrl-c");
    };

    #[cfg(unix)]
    let term = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("received Ctrl+C, starting graceful shutdown");
        },
        _ = term => {
            info!("received SIGTERM, starting graceful shutdown");
        },
    }

    handle.graceful_shutdown(Some(Duration::from_secs(30)));
}
