mod cli;
mod flight_sql;
mod health;
mod ipc_client;
mod rebuild;

mod sidecar;

use clap::Parser;
use cli::{Cli, Command, DbCommand};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "glint_db_sidecar=info".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Run(args) => sidecar::run(args).await,
        Command::Db { command } => match command {
            DbCommand::Rebuild {
                rpc_url,
                from_block,
                db_path,
            } => rebuild::rebuild(&db_path, &rpc_url, from_block),
            DbCommand::Status { db_path } => {
                let conn = rusqlite::Connection::open(&db_path)?;
                glint_historical::schema::check_schema_version(&conn)?;

                let last_block = glint_historical::schema::get_last_processed_block(&conn)?;
                let count = glint_historical::schema::event_count(&conn)?;
                let db_size = std::fs::metadata(&db_path)?.len();

                println!("Database: {}", db_path.display());
                println!(
                    "Last processed block: {}",
                    last_block.map_or_else(|| "none".to_owned(), |b| b.to_string())
                );
                println!("Event count: {count}");
                #[allow(clippy::cast_precision_loss)]
                let size_mb = db_size as f64 / 1_048_576.0;
                println!("Database size: {db_size} bytes ({size_mb:.2} MB)");
                Ok(())
            }
            DbCommand::Prune {
                before_block,
                db_path,
            } => {
                let conn = rusqlite::Connection::open(&db_path)?;
                glint_historical::schema::check_schema_version(&conn)?;

                let deleted = glint_historical::schema::prune_before_block(&conn, before_block)?;
                tracing::info!(deleted, before_block, "pruned events");
                println!("Deleted {deleted} events before block {before_block}");
                Ok(())
            }
        },
    }
}
