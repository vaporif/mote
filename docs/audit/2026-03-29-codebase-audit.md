# Glint Codebase Audit — 2026-03-29

Glint is an ephemeral on-chain storage system built as an OP Stack L3 using reth. It reimplements GolemBase/Arkiv concepts — time-limited entities with annotations and SQL queries — as a reth plugin rather than a geth fork. Entities have a BTL (Blocks To Live) that determines when they expire and are automatically cleaned up.

Cross-referenced against arkiv-op-geth (Go reference implementation) and glint-docs design specs.

---

## Severity Definitions

| Level | Meaning |
|-------|---------|
| **P0 — Critical** | Data loss, consensus divergence, or silent corruption in production |
| **P1 — High** | Incorrect behavior under realistic conditions (reorgs, reconnects) |
| **P2 — Medium** | Logic gaps, maintenance hazards, or missing validation |
| **P3 — Low** | Operational hardening, test coverage, code quality |

---

## P0 — Critical

### C1. ExEx (Execution Extension) Replay Drops Live Batches During Snapshot — FIXED

**Status:** Resolved. Buffering with deduplication implemented in `crates/exex/src/stream.rs` — drained batches are buffered and replayed after snapshot, with `HashSet<BatchKey>` dedup to avoid duplicates.

~~**Location:** `crates/exex/src/stream.rs:338-340`~~

~~**Problem:** When a sidecar reconnects and requests a snapshot, the ExEx drains all pending batches from the channel. If new blocks were mined between the snapshot request and response, those batches are silently dropped.~~

~~**Fix:** Buffer drained batches. After sending the snapshot watermark, replay the buffered batches before resuming the live stream.~~

---

### C2. Revert Batches Not Persisted to Ring Buffer — FIXED

**Status:** Resolved. Revert batches are now stored in the ring buffer via `ring_buffer.push(bnh, BatchOp::Revert, batch)` in `crates/exex/src/lib.rs`.

~~**Problem:** During a chain reorg, revert batches were sent to currently-connected consumers but not stored in the ring buffer.~~

~~**Fix:** Store revert batches in the ring buffer alongside commit batches.~~

---

### C3. Sidecar Full Replay on Any Non-Trivial Revert

**Location:** `crates/sidecar-live/src/batch_decoder.rs:186-191`

**Problem:** When the sidecar receives a revert batch, it can only undo `Created` and `Extended` events (by removing the entity or restoring the old expiration). For any other event type — `Updated`, `Deleted`, `Expired`, `PermissionsChanged` — the batch decoder returns `ApplyResult::NeedsReplay`, which causes the sidecar to disconnect, clear its entire in-memory store, and reconnect with `resume_block = 0` for a full replay from scratch.

This means ANY chain reorg that involves an update, delete, or expiration event causes a complete sidecar reset. During the replay window, the sidecar serves no data (or stale data).

**Impact:** On reorg-heavy environments, sidecar availability degrades. Even a single-block reorg containing an entity update triggers a full replay.

**Fix:** Implement proper inverse operations for all event types in the batch decoder. For updates: restore the previous state from the `old_expires_at_block` and prior content. For deletes: re-insert the entity. For expires: re-insert with the stored metadata. This requires the sidecar to retain enough history to reconstruct prior state, or the ExEx to include "before" state in revert batches.

---

### C4. Sidecar Connection Error Resets Resume Block to Zero

**Location:** `bin/sidecar/src/sidecar.rs:120-123`

**Problem:** When `run_connection()` returns an error (e.g., socket timeout, ExEx temporarily unavailable, malformed message), the sidecar sets `resume_block = 0`, forcing a full replay from genesis on the next reconnect. The graceful-close path (`ConnectionOutcome::Closed { last_block }`) correctly preserves the resume point, but the error path discards all progress.

This means any transient connection error — network blip, ExEx restart, socket read timeout — causes the sidecar to:
1. Clear its entire in-memory EntityStore
2. Reconnect requesting replay from block 0
3. Reprocess all historical blocks before becoming live again

**Impact:** Sidecar availability is fragile. Transient errors cause minutes-long replay windows during which queries return incomplete data.

**Fix:** Distinguish between state-corrupting errors (which require full replay) and transient connection errors (which can resume from the last known good block):

```rust
Err(e) => {
    if e.is_state_corruption() {
        resume_block = 0;
        store.clear();
    } else {
        // Preserve resume_block from last successful batch
        warn!("transient connection error, will resume from block {resume_block}: {e}");
    }
}
```

The sidecar can also fall back to its SQLite `last_processed_block` as a safe resume point.

---

## P1 — High

### H1. Entity Expiration Not Filtered in Sidecar Live Queries — FIXED

**Status:** Resolved. `EntityStore` now tracks `current_block`, updates it on every batch, and filters expired entities (`expires_at_block <= current_block`) in queries and snapshots.

~~**Problem:** The in-memory EntityStore had no concept of "current block" and could not independently filter expired entities.~~

~~**Fix:** Add `current_block` field to EntityStore and filter expired entities in queries.~~

---

### H2. get_mut() Exposes Indexed Fields to Silent Corruption — FIXED

**Status:** Resolved. `get_mut()` now returns `EntityRowMut<'_>` wrapper type that only exposes `set_expires_at_block()` and `set_tx_hash()`, preventing mutation of indexed fields at the type level.

~~**Problem:** `get_mut()` returned `&mut EntityRow`, granting mutable access to indexed fields (owner, annotations) which could silently corrupt bitmap indexes.~~

~~**Fix:** Return a wrapper type that only exposes mutable access to non-indexed fields.~~

---

### H3. Incomplete Reorg Handling in Expiration Index — PARTIALLY FIXED

**Status:** Partially resolved. Drain history buffer now restores recently-drained expirations on reorg. However, deep reorgs exceeding `DRAIN_HISTORY_CAPACITY` can still lose expiration data.

**Remaining gap:** If a reorg is deeper than the drain history capacity, some drained expirations cannot be restored and a warning is logged. Full fix would require rebuilding the affected range from canonical chain event logs.

~~**Original problem:** Only the drain cursor was reset on reorg, without re-inserting expired entities.~~

---

### H4. Snapshot Window Race Condition — FIXED

**Status:** Resolved as part of C1 fix. Batches arriving during the snapshot window are buffered and replayed after the snapshot is applied.

---

## P2 — Medium

### M1. Intrinsic Gas Calculation Uses Unchecked Arithmetic

**Location:** `crates/engine/src/executor.rs:279-280`

```rust
let intrinsic_gas = INTRINSIC_GAS + calldata.len() as u64 * GAS_PER_DATA_BYTE;
let total_gas = intrinsic_gas.saturating_add(staged.gas_used);
```

**Problem:** The outer gas frame uses wrapping `+`/`*` and `saturating_add`, while the inner CRUD operations use the `CheckedGas` trait with proper overflow detection. This is an inconsistency, not a production bug — `calldata.len()` is bounded by block gas limits (30M gas / 16 gas per byte = ~1.875M bytes, well within `u64`). However, the pattern is fragile: a future change to gas constants could introduce silent overflow without any compiler warning.

**Fix:** Use `checked_mul`/`checked_add` for consistency with the `CheckedGas` pattern used in CRUD operations.

---

### M2. Slot Counter Base Constant Defined in Engine, Not Primitives — FIXED

**Status:** Resolved. Both slot constants are now defined in `crates/primitives/src/constants.rs`.

~~**Problem:** `SLOTS_PER_ENTITY` was in engine crate while `SLOTS_PER_ENTITY_WITH_OPERATOR` was in primitives.~~

---

### M3. Historical Query Block Range Not Validated — FIXED

**Status:** Resolved. Block range validation now checks `lower <= upper` and returns an error otherwise.

~~**Problem:** `range.0 > range.1` silently returned empty results.~~

---

### M4. process_updates Resets TTL From Current Block

**Location:** `crates/engine/src/executor/crud.rs:302-304`

```rust
let new_expires = current_block
    .checked_add(update.btl)
    .ok_or_else(|| glint_err("block + btl overflow"))?;
```

**Problem:** An update operation calculates `new_expires_at_block` as `current_block + update.btl`, effectively resetting the TTL from the current block. This means an update can both extend and shorten an entity's remaining lifetime depending on the BTL value provided. If a caller intends to update only the content (not the TTL), they must calculate and provide the exact remaining BTL, which is error-prone.

**Arkiv behavior:** Arkiv does the same — update is implemented as atomic delete + create, so `ExpiresAtBlock = blockNumber + BTL`. This is consistent but worth documenting explicitly since it surprises users who expect "update content only."

**Impact:** Not a bug, but a usability footgun. SDK should provide helpers that calculate the remaining BTL when the caller only wants to update content.

**Fix:** Document this behavior in the spec. Add an SDK convenience method: `update_content_only(entity_key, new_payload)` that reads the current expiration and computes the correct BTL to preserve it.

---

### M5. Log Index Uses saturating_add

**Location:** `crates/exex/src/lib.rs:325`

```rust
global_log_index = global_log_index.saturating_add(1);
```

**Problem:** If a block has more than `u32::MAX` logs, indices silently duplicate. While practically impossible (block gas limit prevents this), saturating arithmetic hides overflow bugs rather than catching them.

**Fix:** Use `checked_add` and return an error on overflow, or use `u64` for the log index.

---

### M6. ExEx Ring Buffer Not Persisted — Sidecar Replays From Zero on Node Restart

**Location:** `crates/exex/src/lib.rs:49-50`

```rust
// TODO: checkpoint persistence — right now we replay from genesis on every
// restart. Persist the ring buffer and max_reported height to disk.
```

**Problem:** The ExEx ring buffer (which stores recent Arrow batches for sidecar snapshot replay) is not persisted to disk. On node restart, the ring buffer is empty, so a connecting sidecar cannot get a snapshot and must replay from block 0.

**Note:** The expiration index itself DOES have a checkpoint mechanism (`crates/engine/src/checkpoint.rs`) — both `bin/eth` and `bin/op` save/load checkpoints on shutdown/startup via `--glint.checkpoint-path`. This finding is specifically about the ExEx ring buffer, not the expiration index.

**Impact:** Every node restart forces sidecar to do a full replay. For large entity histories, this causes extended sidecar downtime.

---

### M7. Operation Execution Order Differs From Arkiv

**Location:** `crates/engine/src/executor/crud.rs:105-109`

**Verified implementation order:**
```
Creates → Updates → Deletes → Extends → ChangeOwners
```

**Arkiv order:** Creates → Deletes → Updates → Extends → ChangeOwners

**Difference:** Glint processes Updates before Deletes; Arkiv processes Deletes before Updates.

**Risk mitigation:** The validation in `crates/primitives/src/validation.rs:153-170` checks for duplicate entity keys across deletes, updates, extends, and change_owners within a single transaction. A transaction with both an update and delete for the same entity key is rejected at validation time, so the ordering difference cannot cause divergent behavior for valid transactions.

**Impact:** No correctness issue for valid transactions. The different ordering is safe due to the duplicate-key validation. However, this should be documented as an intentional design choice to prevent confusion when comparing with Arkiv.

**Fix:** Document the execution order in the spec and add a code comment at `crud.rs:105` explaining the ordering rationale.

---

## P3 — Low / Operational

### L1. SDK send() Does Not Check Receipt Status — FIXED

**Status:** Resolved. `send()` now checks `receipt.status()` and returns an error on revert.

~~**Problem:** Reverted transactions returned `Ok(receipt)` without checking status.~~

---

### L2. SDK Uses Fixed Gas Limit Without Estimation — PARTIALLY FIXED

**Status:** Gas limit is now configurable via builder pattern (`gas_limit()` method). Default remains 1,000,000. No automatic `eth_estimateGas` yet.

**Remaining gap:** Automatic gas estimation for convenience. Low priority since callers can override.

---

### L3. Flight SQL Has No Resource Limits

**Location:** `bin/sidecar/src/flight_sql.rs:37-41`

```rust
// TODO: add tokio::sync::Semaphore for concurrent query limit
// TODO: wrap query execution in tokio::time::timeout
// TODO: configure max gRPC message size via tonic Server::builder()
```

**Impact:** Unbounded concurrent queries, no timeouts, no message size limits. A malicious or buggy client can DoS the sidecar.

---

### L4. ExEx Socket Path in /tmp

**Location:** `crates/node/src/cli.rs` — default `/tmp/glint-exex.sock`

**Impact:** World-readable location. Any process on the machine can connect to the ExEx socket and consume entity events.

**Fix:** Default to `/run/user/$UID/glint-exex.sock` or a configurable path in a restricted directory.

---

### L5. No Metrics / Prometheus Integration

**Location:** Multiple `TODO: metrics` comments in `crates/exex/src/lib.rs`

**Impact:** No observability for production monitoring — ring buffer size, batch processing latency, entity count, expiration rate, query latency.

---

### L6. Code Duplication Between eth and op Binaries

**Location:** `bin/eth/src/main.rs` (176 lines) vs `bin/op/src/main.rs` (191 lines)

**Problem:** Nearly identical startup logic (checkpoint loading, ExEx wiring, RPC registration, shutdown handling). Changes must be applied to both files.

**Fix:** Extract shared startup logic into `crates/node/src/` and have both binaries call into it.

---

### L7. No TLS on Flight SQL

**Impact:** gRPC traffic between sidecar and clients is unencrypted. On a shared network, entity data and queries are visible to eavesdroppers.

---

### L8. Schema Version Mismatch Requires Manual Recovery

**Location:** `crates/sidecar-history/src/schema.rs`

**Problem:** When the sidecar detects a schema version mismatch (e.g., after a code upgrade that changes the SQLite schema), `check_schema_version()` returns an error rather than migrating automatically. The operator must manually run `glint db rebuild` to drop and recreate the database, losing all historical data.

**Fix:** Implement incremental migrations (ALTER TABLE, add columns with defaults) to handle version upgrades without data loss.

---

### L9. E2E Test Coverage Gaps

**Current coverage (5 tests):**
- Entity creation
- Flight SQL basic query
- Flight SQL complex query with annotations
- Flight SQL multi-entity filters
- Historical query

**Missing scenarios:**
- Revert / reorg handling
- Operator / permissions (extend_policy, operator delegation)
- Entity expiration (TTL running out, entity disappearing from queries)
- Entity update and delete
- Crash recovery (node restart, sidecar restart)
- Multiple entities expiring in same block
- Max operations per transaction
- Invalid transactions (should revert)
- Concurrent sidecar queries during block production
- Sidecar reconnection after ExEx disconnect

---

### L10. Advisory Vulnerabilities in Dependencies

**Location:** `deny.toml`

4 ignored CVEs in dev/transitive dependencies:
- RUSTSEC-2025-0141 (bincode unmaintained, via reth)
- RUSTSEC-2024-0436 (paste unmaintained, via reth)
- RUSTSEC-2025-0111 (tokio-tar, dev-only)
- RUSTSEC-2025-0134 (rustls-pemfile, dev-only)

**Impact:** All transitive from reth or dev-only. Low risk but should be monitored for upgrades.

---

## Design Comparison: Glint vs Arkiv

### Where Glint Is Better

| Aspect | Arkiv | Glint | Why Glint Wins |
|--------|-------|-------|----------------|
| Fork maintenance | Modifies geth internals | Plugin via traits | No merge conflicts on upstream updates |
| Per-entity storage | 96 bytes (3 slots) | 64-96 bytes (2-3 slots) | 33% less trie writes without operator (same with operator) |
| Content hash | None | keccak256 of full entity | Enables fraud proofs |
| Extend permissions | Anyone can extend (no check) | Configurable per-entity policy | Owner controls who extends |
| Operator delegation | Not supported | Optional operator per entity | Backend services can manage without owning |
| BTL cap | Uncapped (entities can live forever) | MAX_BTL enforced (~1 week) | Actually ephemeral; bounds recovery |
| Query engine | In-process SQLite | Separate DataFusion + Flight SQL | Crash-independent; SQL standard |
| Compression | Brotli per-tx (decompression bomb risk) | None (OP batcher compresses) | Simpler; no DoS vector |
| Annotation bounds | Uncapped | 64 total, 256B keys, 1024B values | Predictable resource usage |
| Type safety | Go (runtime errors) | Rust (compile-time guarantees) | Prevents class of bugs by construction |

### Where Arkiv Is Better (or Different)

| Aspect | Arkiv | Glint | Gap |
|--------|-------|-------|-----|
| Expiration index | On-chain (auto-reverts with state) | In-memory (manual reorg handling) | Glint must handle reorg manually (H3) |
| Glob matching | Supported on annotations | Not supported | Could be useful for discovery |
| At-block queries | Point-in-time state reconstruction | Not supported (only live + historical events) | Would need state snapshots |
| CLI tool | Entity operations CLI | SDK only | No quick admin tool |
| Operations per tx | 1000 | 100 | Glint's limit is conservative |
| Maturity | Running in production | Pre-production | More edge cases found and fixed |

### Intentional Differences (Not Bugs)

1. **Entity key derivation** — Glint includes `payload_len` in hash: `keccak256(tx_hash || len(payload) as u32 || payload || op_index)`. Arkiv omits it: `keccak256(txHash, payload, opIndex)`. Both are collision-resistant. Glint's approach adds a domain separator for the variable-length payload field.

2. **No Brotli compression** — By design. OP Stack batcher already compresses. Per-tx compression adds attack surface.

3. **Separate sidecar process** — By design. Query service crash-independence is a core principle.

4. **MAX_BTL cap** — By design. Ephemeral means ephemeral. Bounds cold-start recovery time.

---

## Summary

| Severity | Total | Fixed | Partial | Open | Key Themes |
|----------|-------|-------|---------|------|------------|
| P0 Critical | 4 | 2 | 0 | 2 | C1+C2 fixed; C3 (sidecar full-replay) and C4 (connection error resume) still open |
| P1 High | 4 | 3 | 1 | 0 | H1+H2+H4 fixed; H3 partially fixed (deep reorgs still a gap) |
| P2 Medium | 7 | 2 | 0 | 5 | M2+M3 fixed; M1, M4, M5, M6, M7 still open |
| P3 Low | 10 | 1 | 1 | 8 | L1 fixed; L2 partial; L3-L10 still open |

### Remaining Priority Order

1. **C3** (Sidecar NeedsReplay) — Any non-trivial reorg forces full replay; implement proper revert handling
2. **C4** (Connection error resume) — Transient errors shouldn't cause full replay
3. **H3** (Deep reorg gap in expiration index) — Drain history has finite capacity
4. **M1** (Checked gas arithmetic) — Consistency fix
5. **M5** (Checked log index) — Consistency fix
6. **M6** (Ring buffer persistence) — Eliminates full sidecar replays on node restart
7. **L3** (Flight SQL limits) — Required before any public deployment
8. **L9** (E2E test coverage) — Prevents regressions as features are added

Lower priority: M4 (doc), M7 (doc), L4 (socket path), L5 (metrics), L6 (dedup), L7 (TLS), L8 (migrations), L10 (advisories).
