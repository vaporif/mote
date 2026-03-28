use std::path::Path;

use glint_historical::schema;
use rusqlite::Connection;
use tracing::info;

pub fn rebuild(db_path: &Path, _rpc_url: &str, from_block: u64) -> eyre::Result<()> {
    info!(db = %db_path.display(), "starting database rebuild");

    let conn = Connection::open(db_path)?;
    schema::drop_and_recreate(&conn)?;

    if from_block > 0 {
        schema::set_last_processed_block(&conn, from_block.saturating_sub(1))?;
        info!(
            from_block,
            "database reset with resume point — restart the sidecar to replay from ExEx"
        );
    } else {
        info!("database reset to genesis — restart the sidecar to replay from ExEx");
    }

    Ok(())
}
