# Arkiv vs Glint: Feature Comparison

**Date**: 2026-03-26
**Full analysis**: `../glint-docs/design/arkiv-vs-glint-comparison.md`

## Architecture

| Aspect | Arkiv (op-geth fork, Go) | Glint (reth plugin, Rust) |
|--------|--------------------------|---------------------------|
| Approach | Modifies geth internals (`state_transition.go`) | Wraps `BlockExecutorFactory`, no source mods |
| Processor address | `0x...61726B6976` ("arkiv") | `0x...676c696e74` ("glint") |
| Tx encoding | RLP + Brotli (20MB compressed max) | RLP only |
| Language safety | Runtime panics, unchecked errors | `Result<T,E>`, compile-time guarantees |

## CRUD Operations

| Operation | Arkiv | Glint |
|-----------|-------|-------|
| Create | Yes | Yes |
| Update | Yes (owner-only) | Yes (owner-only) |
| Delete | Yes (owner-only) | Yes (owner-only) |
| Extend BTL | Yes (permissionless) | Yes (permissionless) |
| Change Owner | **Yes** | **No** (not needed for ephemeral storage) |

## Entity Model

| Feature | Arkiv | Glint |
|---------|-------|-------|
| Entity key | `keccak256(txHash, payload, opIndex)` | `keccak256(txHash, payloadLen, payload, opIndex)` |
| On-chain metadata | 32B (owner + expiresAt) | 32B (owner + reserved + expiresAt) |
| Content hash on-chain | No | **Yes** — slot 2, enables fraud proofs |
| Slots per entity | ~3 (1 meta + 2 expiration index) | 2 (meta + content hash), 0 index overhead |
| Payload storage | SQLite (off-chain, unverifiable) | Event logs (reconstructable, verifiable via hash) |

## Limits

| Limit | Arkiv | Glint |
|-------|-------|-------|
| Max ops/tx | 1000 (hardcoded) | 100 (configurable) |
| Max payload | No per-entity limit | 128 KB |
| Max BTL | **Unbounded** (uint64) | 302,400 blocks (~1 week) |
| Max annotations/entity | Unbounded | 64 |
| Max annotation key | Unbounded | 256 bytes |
| Max annotation value | Unbounded | 1024 bytes |

## Gas Model

| Feature | Arkiv | Glint |
|---------|-------|-------|
| Intrinsic gas | Charged | Charged |
| CRUD execution gas | **Zero** (no per-op cost) | Metered: 50k create, 40k update, 10k delete/extend + per-byte + per-BTL |
| Expiration gas | 0 (system tx) | 0 (system pre-execution) |

Arkiv's zero CRUD gas = DoS vector. No economic signal for BTL duration or payload size.

## Expiration Index

| Feature | Arkiv | Glint |
|---------|-------|-------|
| Structure | On-chain EnumerableSet (Array + HashMap) | In-memory `HashMap<u64, Vec<B256>>` |
| Persistence | State trie (survives restarts) | Rebuilt from logs on cold start |
| Per-entity overhead | ~2 trie slots (array + hashmap entry) | ~38 bytes RAM, 0 on-chain |
| 10k expirations/block | ~60k SLOAD + 60k SSTORE (**~500M gas, exceeds block limit**) | **~170ms wall clock** |
| Cold start | 0 | Scans MAX_BTL blocks (seconds to minutes) |

## Query & RPC

| Feature | Arkiv | Glint |
|---------|-------|-------|
| JSON-RPC | `arkiv_query`, `getEntityCount`, `getBlockTiming`, `getUsedSlots` | **Not yet implemented** |
| Query language | `=`, `!=`, `<`, `>`, glob, `IN`, `NOT IN`, `&&`, `\|\|` | N/A |
| Query backend | SQLite bitmap store (unverifiable) | N/A on node |
| Flight SQL | No | **Yes** — DataFusion, full SQL |
| At-block queries | Yes | No (live state only) |
| Health endpoint | No | **Yes** |

## Analytics & Streaming

| Feature | Arkiv | Glint |
|---------|-------|-------|
| Streaming | Background ETL goroutine (fire-and-forget) | ExEx + Arrow IPC (unix socket) |
| Crash recovery | **None** — ETL crash = stale data forever | Ring buffer replay on reconnect |
| Backpressure | No | Yes |
| Format | Custom | Arrow RecordBatch (zero-copy) |
| Health monitoring | No | Yes (HTTP readiness) |

## Events

| Event | Arkiv | Glint |
|-------|-------|-------|
| Created | Yes (+ cost) | Yes (+ payload, annotations) |
| Updated | Yes (+ cost) | Yes (+ payload, annotations) |
| Deleted | Yes | Yes |
| Expired | Yes | Yes |
| Extended | Yes (+ cost) | Yes |
| Owner Changed | Yes | N/A |

Glint emits full payload in logs (enables reconstruction from chain data alone).

## Where Glint Wins

1. Content hash on-chain (verifiable payloads)
2. Metered gas (DoS resistance)
3. In-memory expiration index (1000x faster than on-chain)
4. BTL cap (enforces ephemerality)
5. ExEx streaming (crash-independent, replay, backpressure)
6. Flight SQL (full SQL vs custom query language)
7. Configurable limits (vs hardcoded)
8. Compile-time safety (eliminates Arkiv bug classes)

## What Glint Should Add

1. **Make Extend owner-only** — permissionless extend is a privacy/griefing vector
2. **Expiration index checkpointing** — persist to file on shutdown, load on startup
3. **Basic `glint_*` JSON-RPC** — entity reads without sidecar dependency
4. **HashSet in expiration index** — fix O(n) remove to O(1)

## What Glint Should NOT Port

1. ChangeOwner — over-engineering for ephemeral storage
2. Brotli compression — OP Stack batch submitter handles this layer
3. Zero-gas CRUD — DoS vector
4. Unbounded BTL — breaks ephemeral semantics
5. On-chain EnumerableSet — state bloat, catastrophic at scale
6. Unbounded annotations — bloat vector

## Arkiv Bugs (Motivation for Rebuild)

1. `Validate()` return discarded in txpool — invalid txs enter mempool
2. Wrong error variable after `ExecuteArkivTransaction` — failures swallowed
3. `blockHash` passed as `txHash` in tracer path — wrong entity keys
4. Double execution when tracing attached
5. Brotli decompression bomb (no size limit in mempool)
6. `GetEntityMetaData` returns zero-value for missing entities
7. SQLite ETL goroutine is fire-and-forget (no recovery)
8. Broken CLI (calls `golembase_*` but server registers `arkiv_*`)
9. `arkiv_getEntityCount` returns hardcoded 0
10. `GasUsed = 0` for all Arkiv txs (no gas accounting)
