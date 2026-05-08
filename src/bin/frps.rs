use anyhow::Result;
use clap::Parser;
use frprs::{config::ServerConfig, server};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "frps", about = "Rust frp server")]
struct Args {
    #[arg(short = 'c', long = "config", default_value = "conf/frps.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    let cfg = ServerConfig::load(&args.config)?;
    server::run(cfg).await
}
