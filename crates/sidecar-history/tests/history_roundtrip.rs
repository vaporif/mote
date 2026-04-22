use alloy_primitives::{Address, B256};
use glint_historical::{schema, writer};
use glint_primitives::test_utils::{EventBuilder, build_batch};
use rusqlite::Connection;

fn setup() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();
    conn
}

#[test]
fn created_inserts_open_history_row() {
    let conn = setup();
    let batch = build_batch(&[EventBuilder::created(10, 0x01)]);
    writer::insert_batch(&conn, &batch).unwrap();

    let entity_key = B256::repeat_byte(0x01);
    let (count, valid_to): (i64, Option<i64>) = conn
        .query_row(
            "SELECT COUNT(*), valid_to_block FROM entities_history WHERE entity_key = ?1",
            [entity_key.as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(valid_to, None, "row should be open (valid_to_block = NULL)");
}

#[test]
fn updated_closes_old_row_and_opens_new() {
    let conn = setup();
    let batch = build_batch(&[
        EventBuilder::created(10, 0x01),
        EventBuilder::updated(20, 0x01, 110, 120).with_log_index(1),
    ]);
    writer::insert_batch(&conn, &batch).unwrap();

    let entity_key = B256::repeat_byte(0x01);
    let mut stmt = conn
        .prepare(
            "SELECT valid_from_block, valid_to_block FROM entities_history
             WHERE entity_key = ?1 ORDER BY valid_from_block",
        )
        .unwrap();
    let rows: Vec<(i64, Option<i64>)> = stmt
        .query_map([entity_key.as_slice()], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(rows.len(), 2);
    // First row: created at 10, closed at 20
    assert_eq!(rows[0].0, 10);
    assert_eq!(rows[0].1, Some(20));
    // Second row: created at 20, still open
    assert_eq!(rows[1].0, 20);
    assert_eq!(rows[1].1, None);
}

#[test]
fn extended_copies_payload_from_previous_row() {
    let conn = setup();
    let batch = build_batch(&[
        EventBuilder::created(10, 0x01)
            .with_payload(b"important data")
            .with_content_type("application/octet-stream"),
        EventBuilder::extended(20, 0x01, 110, 200).with_log_index(1),
    ]);
    writer::insert_batch(&conn, &batch).unwrap();

    let entity_key = B256::repeat_byte(0x01);
    // The new open row should carry forward the payload and content_type
    let (payload, content_type, expires): (Vec<u8>, String, i64) = conn
        .query_row(
            "SELECT payload, content_type, expires_at_block FROM entities_history
             WHERE entity_key = ?1 AND valid_to_block IS NULL",
            [entity_key.as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();

    assert_eq!(payload, b"important data");
    assert_eq!(content_type, "application/octet-stream");
    assert_eq!(expires, 200, "expires_at_block should be the new value");
}

#[test]
fn deleted_closes_row_without_opening_new() {
    let conn = setup();
    let batch = build_batch(&[
        EventBuilder::created(10, 0x01),
        EventBuilder::deleted(20, 0x01).with_log_index(1),
    ]);
    writer::insert_batch(&conn, &batch).unwrap();

    let entity_key = B256::repeat_byte(0x01);
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM entities_history WHERE entity_key = ?1",
            [entity_key.as_slice()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "only the original created row should exist");

    let valid_to: i64 = conn
        .query_row(
            "SELECT valid_to_block FROM entities_history WHERE entity_key = ?1",
            [entity_key.as_slice()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(valid_to, 20, "row should be closed at the delete block");
}

#[test]
fn permissions_changed_updates_owner_keeps_payload() {
    let new_owner = Address::repeat_byte(0xBB);
    let new_operator = Address::repeat_byte(0xCC);
    let conn = setup();
    let batch = build_batch(&[
        EventBuilder::created(10, 0x01)
            .with_payload(b"keep this")
            .with_content_type("text/plain"),
        EventBuilder::permissions_changed(20, 0x01, new_owner, 1, new_operator).with_log_index(1),
    ]);
    writer::insert_batch(&conn, &batch).unwrap();

    let entity_key = B256::repeat_byte(0x01);
    let (owner, payload, extend_policy): (Vec<u8>, Vec<u8>, i64) = conn
        .query_row(
            "SELECT owner, payload, extend_policy FROM entities_history
             WHERE entity_key = ?1 AND valid_to_block IS NULL",
            [entity_key.as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();

    assert_eq!(owner, new_owner.as_slice());
    assert_eq!(payload, b"keep this");
    assert_eq!(extend_policy, 1);
}

#[test]
fn revert_deletes_new_rows_and_reopens_previous() {
    let conn = setup();
    let batch = build_batch(&[
        EventBuilder::created(10, 0x01),
        EventBuilder::updated(20, 0x01, 110, 120).with_log_index(1),
    ]);
    writer::insert_batch(&conn, &batch).unwrap();

    // Revert from block 20
    schema::delete_events_from_block(&conn, 20).unwrap();

    let entity_key = B256::repeat_byte(0x01);
    let mut stmt = conn
        .prepare(
            "SELECT valid_from_block, valid_to_block FROM entities_history
             WHERE entity_key = ?1 ORDER BY valid_from_block",
        )
        .unwrap();
    let rows: Vec<(i64, Option<i64>)> = stmt
        .query_map([entity_key.as_slice()], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(rows.len(), 1, "only the original row should remain");
    assert_eq!(rows[0].0, 10);
    assert_eq!(
        rows[0].1, None,
        "row should be reopened (valid_to_block = NULL)"
    );
}

#[test]
fn annotations_written_and_copied_on_extend() {
    let conn = setup();
    let batch = build_batch(&[
        EventBuilder::created(10, 0x01)
            .with_string_annotations(vec![("env".into(), "prod".into())])
            .with_numeric_annotations(vec![("version".into(), 42)]),
        EventBuilder::extended(20, 0x01, 110, 200).with_log_index(1),
    ]);
    writer::insert_batch(&conn, &batch).unwrap();

    let entity_key = B256::repeat_byte(0x01);

    // Check annotations on the new (extended) row at valid_from_block = 20
    let (ann_key, ann_value): (String, String) = conn
        .query_row(
            "SELECT ann_key, ann_value FROM history_string_annotations
             WHERE entity_key = ?1 AND valid_from_block = 20",
            [entity_key.as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(ann_key, "env");
    assert_eq!(ann_value, "prod");

    let (ann_key, ann_value): (String, i64) = conn
        .query_row(
            "SELECT ann_key, ann_value FROM history_numeric_annotations
             WHERE entity_key = ?1 AND valid_from_block = 20",
            [entity_key.as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(ann_key, "version");
    assert_eq!(ann_value, 42);
}
