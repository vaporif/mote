use clap::Parser;

#[derive(Parser)]
#[command(
    name = "mote-analytics",
    about = "Query service for Mote ephemeral storage"
)]
struct Cli {
    /// Path to the `ExEx` Unix socket
    #[arg(long, default_value = "/tmp/mote-exex.sock")]
    exex_socket: std::path::PathBuf,

    /// Flight SQL server port
    #[arg(long, default_value_t = 50051)]
    flight_port: u16,

    /// Health HTTP server port
    #[arg(long, default_value_t = 8080)]
    health_port: u16,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mote_analytics=info".into()),
        )
        .init();

    let cli = Cli::parse();
    mote_analytics::run(cli.exex_socket, cli.flight_port, cli.health_port).await
}
