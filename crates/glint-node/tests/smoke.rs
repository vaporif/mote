use alloy_primitives::{Address, B256, Bytes};

#[test]
fn genesis_config_round_trips_through_extraction() {
    let genesis_str = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../etc/genesis.json"),
    )
    .unwrap();
    let genesis: serde_json::Value = serde_json::from_str(&genesis_str).unwrap();
    let config = glint_node::genesis::extract_glint_config(&genesis).unwrap();
    assert_eq!(config.max_btl, 302_400);
    assert!(config.validate().is_ok());
}

#[test]
fn cold_start_pipeline_tracker_to_index() {
    use glint_engine::expiration::ExpirationIndex;
    use glint_engine::recovery::LiveEntityTracker;
    use glint_primitives::parse::EntityEvent;

    let mut tracker = LiveEntityTracker::new();
    let key = B256::repeat_byte(0x01);
    tracker.apply_event(&EntityEvent::Created {
        entity_key: key,
        owner: Address::ZERO,
        expires_at: 1000,
        content_type: String::new(),
        payload: Bytes::default(),
        string_keys: vec![],
        string_values: vec![],
        numeric_keys: vec![],
        numeric_values: vec![],
        extend_policy: 0,
        operator: Address::ZERO,
    });
    tracker.apply_event(&EntityEvent::Extended {
        entity_key: key,
        old_expires_at: 1000,
        new_expires_at: 1500,
        owner: Address::ZERO,
    });

    let mut index = ExpirationIndex::new();
    index.rebuild_from_logs(tracker.into_inner().into_iter());
    assert_eq!(index.get_expired(1000), None);
    assert_eq!(index.get_expired(1500).map(|s| s.len()), Some(1));
}
