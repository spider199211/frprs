use anyhow::Result;
use clap::Parser;
use frprs::client;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "frpc", about = "Rust frp client")]
struct Args {
    #[arg(short = 'c', long = "config", default_value = "conf/frpc.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    client::run_from_file(args.config).await
}
