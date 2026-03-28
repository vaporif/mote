use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "glint-db-sidecar",
    about = "Unified DB sidecar for Glint: live + historical entity queries"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Run(RunArgs),
    Db {
        #[command(subcommand)]
        command: DbCommand,
    },
}

#[derive(Parser)]
pub struct RunArgs {
    #[arg(long, default_value = "/tmp/glint-exex.sock")]
    pub exex_socket: PathBuf,

    #[arg(long, default_value_t = 50051)]
    pub flight_port: u16,

    #[arg(long, default_value_t = 8080)]
    pub health_port: u16,

    #[arg(long, default_value = "glint-sidecar.db")]
    pub db_path: PathBuf,
}

#[derive(Subcommand)]
pub enum DbCommand {
    Rebuild {
        #[arg(long)]
        rpc_url: String,

        #[arg(long, default_value_t = 0)]
        from_block: u64,

        #[arg(long, default_value = "glint-sidecar.db")]
        db_path: PathBuf,
    },
    Status {
        #[arg(long, default_value = "glint-sidecar.db")]
        db_path: PathBuf,
    },
    Prune {
        #[arg(long)]
        before_block: u64,

        #[arg(long, default_value = "glint-sidecar.db")]
        db_path: PathBuf,
    },
}
