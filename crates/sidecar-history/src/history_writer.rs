use eyre::WrapErr;
use glint_primitives::exex_types::EntityEventType;
use rusqlite::{OptionalExtension, Transaction};

pub struct EntityEvent<'a> {
    pub event_type: EntityEventType,
    pub block_number: i64,
    pub entity_key: &'a [u8],
    pub owner: Option<&'a [u8]>,
    pub expires_at_block: Option<i64>,
    pub content_type: Option<&'a str>,
    pub payload: Option<&'a [u8]>,
    pub tx_hash: &'a [u8],
    pub extend_policy: Option<i64>,
    pub operator: Option<&'a [u8]>,
    pub string_annotations: &'a [(String, String)],
    pub numeric_annotations: &'a [(String, i64)],
}

struct PreviousRow {
    owner: Vec<u8>,
    expires_at_block: i64,
    content_type: String,
    payload: Vec<u8>,
    created_at_block: i64,
    tx_hash: Vec<u8>,
    extend_policy: i64,
    operator: Option<Vec<u8>>,
    string_annotations: Vec<(String, String)>,
    numeric_annotations: Vec<(String, i64)>,
}

fn close_current_row(
    tx: &Transaction<'_>,
    entity_key: &[u8],
    block: i64,
) -> eyre::Result<Option<PreviousRow>> {
    let mut query = tx.prepare_cached(
        "SELECT owner, expires_at_block, content_type, payload, created_at_block,
                tx_hash, extend_policy, operator, valid_from_block
         FROM entities_history
         WHERE entity_key = ?1 AND valid_to_block IS NULL",
    )?;

    let row = query
        .query_row([entity_key], |r| {
            Ok((
                PreviousRow {
                    owner: r.get(0)?,
                    expires_at_block: r.get(1)?,
                    content_type: r.get(2)?,
                    payload: r.get(3)?,
                    created_at_block: r.get(4)?,
                    tx_hash: r.get(5)?,
                    extend_policy: r.get(6)?,
                    operator: r.get(7)?,
                    string_annotations: Vec::new(),
                    numeric_annotations: Vec::new(),
                },
                r.get::<_, i64>(8)?,
            ))
        })
        .optional()
        .wrap_err("reading current open history row")?;

    let Some((mut prev, valid_from)) = row else {
        return Ok(None);
    };

    prev.string_annotations = read_string_annotations(tx, entity_key, valid_from)?;
    prev.numeric_annotations = read_numeric_annotations(tx, entity_key, valid_from)?;

    tx.execute(
        "UPDATE entities_history SET valid_to_block = ?1
         WHERE entity_key = ?2 AND valid_to_block IS NULL",
        rusqlite::params![block, entity_key],
    )?;

    Ok(Some(prev))
}

#[allow(clippy::too_many_arguments)]
fn insert_open_row(
    tx: &Transaction<'_>,
    entity_key: &[u8],
    block: i64,
    owner: &[u8],
    expires_at_block: i64,
    content_type: &str,
    payload: &[u8],
    created_at_block: i64,
    tx_hash: &[u8],
    extend_policy: i64,
    operator: Option<&[u8]>,
    string_annotations: &[(String, String)],
    numeric_annotations: &[(String, i64)],
) -> eyre::Result<()> {
    tx.execute(
        "INSERT OR REPLACE INTO entities_history (
            entity_key, valid_from_block, valid_to_block, owner, expires_at_block,
            content_type, payload, created_at_block, tx_hash, extend_policy, operator
        ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        rusqlite::params![
            entity_key,
            block,
            owner,
            expires_at_block,
            content_type,
            payload,
            created_at_block,
            tx_hash,
            extend_policy,
            operator,
        ],
    )?;

    let mut insert_str = tx.prepare_cached(
        "INSERT OR REPLACE INTO history_string_annotations (entity_key, valid_from_block, ann_key, ann_value)
         VALUES (?1, ?2, ?3, ?4)",
    )?;
    for (k, v) in string_annotations {
        insert_str.execute(rusqlite::params![entity_key, block, k, v])?;
    }

    let mut insert_num = tx.prepare_cached(
        "INSERT OR REPLACE INTO history_numeric_annotations (entity_key, valid_from_block, ann_key, ann_value)
         VALUES (?1, ?2, ?3, ?4)",
    )?;
    for (k, v) in numeric_annotations {
        insert_num.execute(rusqlite::params![entity_key, block, k, v])?;
    }

    Ok(())
}

fn read_string_annotations(
    tx: &Transaction<'_>,
    entity_key: &[u8],
    valid_from_block: i64,
) -> eyre::Result<Vec<(String, String)>> {
    let mut stmt = tx.prepare_cached(
        "SELECT ann_key, ann_value FROM history_string_annotations
         WHERE entity_key = ?1 AND valid_from_block = ?2",
    )?;
    let rows = stmt.query_map(rusqlite::params![entity_key, valid_from_block], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })?;
    rows.collect::<Result<Vec<_>, _>>()
        .wrap_err("reading history string annotations")
}

fn read_numeric_annotations(
    tx: &Transaction<'_>,
    entity_key: &[u8],
    valid_from_block: i64,
) -> eyre::Result<Vec<(String, i64)>> {
    let mut stmt = tx.prepare_cached(
        "SELECT ann_key, ann_value FROM history_numeric_annotations
         WHERE entity_key = ?1 AND valid_from_block = ?2",
    )?;
    let rows = stmt.query_map(rusqlite::params![entity_key, valid_from_block], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
    })?;
    rows.collect::<Result<Vec<_>, _>>()
        .wrap_err("reading history numeric annotations")
}

pub fn write_history(tx: &Transaction<'_>, event: &EntityEvent<'_>) -> eyre::Result<()> {
    match event.event_type {
        EntityEventType::Created => {
            let Some(owner) = event.owner else {
                return Ok(());
            };
            let Some(expires) = event.expires_at_block else {
                return Ok(());
            };
            let Some(ct) = event.content_type else {
                return Ok(());
            };
            let Some(pl) = event.payload else {
                return Ok(());
            };
            let ep = event.extend_policy.unwrap_or(0);

            insert_open_row(
                tx,
                event.entity_key,
                event.block_number,
                owner,
                expires,
                ct,
                pl,
                event.block_number,
                event.tx_hash,
                ep,
                event.operator,
                event.string_annotations,
                event.numeric_annotations,
            )?;
        }
        EntityEventType::Updated => {
            let prev = close_current_row(tx, event.entity_key, event.block_number)?;

            let Some(owner) = event.owner else {
                return Ok(());
            };
            let Some(expires) = event.expires_at_block else {
                return Ok(());
            };
            let Some(ct) = event.content_type else {
                return Ok(());
            };
            let Some(pl) = event.payload else {
                return Ok(());
            };
            let ep = event.extend_policy.unwrap_or(0);

            let created_at = prev
                .as_ref()
                .map_or(event.block_number, |p| p.created_at_block);

            insert_open_row(
                tx,
                event.entity_key,
                event.block_number,
                owner,
                expires,
                ct,
                pl,
                created_at,
                event.tx_hash,
                ep,
                event.operator,
                event.string_annotations,
                event.numeric_annotations,
            )?;
        }
        EntityEventType::Deleted | EntityEventType::Expired => {
            close_current_row(tx, event.entity_key, event.block_number)?;
        }
        EntityEventType::Extended => {
            let prev = close_current_row(tx, event.entity_key, event.block_number)?;
            let Some(prev) = prev else { return Ok(()) };
            let new_expires = event.expires_at_block.unwrap_or(prev.expires_at_block);

            insert_open_row(
                tx,
                event.entity_key,
                event.block_number,
                &prev.owner,
                new_expires,
                &prev.content_type,
                &prev.payload,
                prev.created_at_block,
                &prev.tx_hash,
                prev.extend_policy,
                prev.operator.as_deref(),
                &prev.string_annotations,
                &prev.numeric_annotations,
            )?;
        }
        EntityEventType::PermissionsChanged => {
            let prev = close_current_row(tx, event.entity_key, event.block_number)?;
            let Some(prev) = prev else { return Ok(()) };

            let new_owner = event.owner.unwrap_or(&prev.owner);
            let new_ep = event.extend_policy.unwrap_or(prev.extend_policy);
            let new_operator = event.operator.or(prev.operator.as_deref());

            insert_open_row(
                tx,
                event.entity_key,
                event.block_number,
                new_owner,
                prev.expires_at_block,
                &prev.content_type,
                &prev.payload,
                prev.created_at_block,
                &prev.tx_hash,
                new_ep,
                new_operator,
                &prev.string_annotations,
                &prev.numeric_annotations,
            )?;
        }
    }

    Ok(())
}
