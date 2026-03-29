# Glint

[![CI](https://github.com/vaporif/glint/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/vaporif/glint/actions/workflows/ci.yml)
[![E2E](https://github.com/vaporif/glint/actions/workflows/e2e.yml/badge.svg?branch=main)](https://github.com/vaporif/glint/actions/workflows/e2e.yml)
[![Audit](https://github.com/vaporif/glint/actions/workflows/audit.yml/badge.svg?branch=main)](https://github.com/vaporif/glint/actions/workflows/audit.yml)

Ephemeral on-chain storage layer, built on reth.

Glint adds a BTL (Blocks-to-Live) primitive to Ethereum. Entities have a TTL, carry queryable annotations, and disappear when their time is up.

Runs as both a standalone Ethereum node (`eth-glint`) and an OP Stack L3 (`op-glint`).

**The full flow - node startup, entity creation, ExEx streaming, and Flight SQL queries - is covered by e2e tests.**

## Why

Blockchains store data permanently. If you want to publish a limit order that's valid for the next 10 blocks, you pay to store it forever even though nobody needs it after that. There's no native TTL in Ethereum.

Think CoW Swap orders valid for minutes, oracle price feeds stale after a few blocks, compute marketplace offers that expire when filled, ephemeral task boards for AI agents. Anywhere people publish short-lived structured records and others need to query them by metadata.

## Getting started

Requires [Nix](https://nixos.org/) with flakes enabled, or Rust nightly + the tools listed in `flake.nix`.

```bash
# enter dev shell (installs rust toolchain, cargo-nextest, taplo, typos, etc.)
nix develop
# or: direnv allow

# build everything
just build

# run all checks (clippy + tests + fmt + lint)
just check
```

### Run locally

Terminal 1 - start the node in dev mode (auto-mines blocks every second):

```bash
just run-eth --dev --dev.block-time 1000ms --http
# or for OP Stack: just run-op --dev --dev.block-time 1000ms --http
```

Terminal 2 - start the sidecar (connects to the node's ExEx socket, serves Flight SQL + historical queries):

```bash
just run-sidecar
```

The node listens on `localhost:8545` (JSON-RPC). The sidecar exposes Flight SQL on `localhost:50051` and health on `localhost:8080`.

### Query entities

Any Flight SQL client works. With `arrow-flight` CLI or DBeaver, connect to `localhost:50051`:

```sql
-- live entities
SELECT entity_key, content_type, expires_at_block FROM entities;

-- annotation lookups (bitmap-indexed)
SELECT * FROM entities WHERE str_ann(string_annotations, 'pair') = 'USDC/WETH';
SELECT * FROM entities WHERE num_ann(numeric_annotations, 'price') > 1000;
SELECT * FROM entities WHERE owner = x'aa...' AND num_ann(numeric_annotations, 'price') >= 500;

-- historical events (SQLite-backed, requires block range)
SELECT * FROM entity_events WHERE block_number BETWEEN 100 AND 200;
```

`str_ann` / `num_ann` are UDFs. Still evaluating whether to replace them with a flatter schema in v2 so clients get plain SQL with no custom functions.

### JSON-RPC

The node exposes a few entity-specific RPC methods alongside the standard Ethereum ones:

- `glint_getEntity(key)` - metadata, operator, content hash
- `glint_getEntityCount()` - total live entities
- `glint_getUsedSlots()` - storage slot count
- `glint_getBlockTiming()` - current block number and timestamp

## Relationship to Arkiv

Glint wouldn't exist without [Arkiv](https://github.com/Arkiv-Network/arkiv-op-geth) (formerly GolemBase). The Arkiv team designed the core model - magic address interception, BTL expiration, content-addressed keys, annotation model, atomic ops, owner-gated mutations.

Why rewrite it? Optimism is moving from op-geth to reth. Glint takes the same ideas and implements them as a reth plugin using `BlockExecutor` and `ExEx`, no geth fork needed.

I also revisited a few design tradeoffs along the way:

| | Arkiv | Glint | Why |
|---|---|---|---|
| Base | op-geth fork | reth plugin (BlockExecutor + ExEx) | Follows Optimism's reth migration. |
| On-chain cost | ~96 bytes/entity (3 slots) | 64 bytes/entity (2 slots) | Expiration index moved off-chain into memory. |
| Content integrity | - | 32-byte content hash | Lets clients verify query results against the trie. |
| Query engine | SQLite with bitmap indexes, in-process, custom JSON-RPC | DataFusion (columnar, in-memory) with roaring bitmap indexes, separate process, Flight SQL | Process isolation, columnar scans for analytics. See [query engine](#query-engine). |
| Compression | Brotli per-tx | None | OP batcher already compresses the batch. |
| MAX_BTL | Uncapped | Enforced at txpool + execution | Bounds recovery time and index size. |
| Extend | Permissionless, no cap | Per-entity policy (anyone or owner/operator), capped at MAX_BTL | Creator chooses who can extend. |
| Operator delegation | - | Optional operator per entity | Backend can manage entities without owning them. |
| ChangeOwner | Supported | Supported | Transfer ownership, change extend policy, set/remove operator in one atomic op. |

Arkiv also has things Glint doesn't yet - JSON-RPC query endpoints, glob matching on annotations, at-block historical queries. On the roadmap.

## Architecture

```mermaid
graph TB
    subgraph node["glint-node (reth)"]
        direction TB

        subgraph engine["glint-engine (BlockExecutor)"]
            tx_in[/"User tx"/]
            check{To processor<br/>address?}
            crud["Entity CRUD<br/>trie writes (64 bytes)<br/>+ event logs"]
            evm["Standard EVM<br/>execution"]
            expire["Expiration cleanup<br/>(pre-execution)"]

            tx_in --> check
            check -->|yes| crud
            check -->|no| evm
            expire -.->|runs before<br/>each block| crud
        end

        subgraph exex["glint-exex (ExEx)"]
            notify["ExExNotification<br/>(commit/reorg)"]
            arrow["Entity event logs<br/>→ Arrow RecordBatch"]
            ring["Ring buffer<br/>(replay on reconnect)"]
            notify --> arrow
            arrow --> ring
        end

        rpc_node["glint_* JSON-RPC<br/>(entity, slots, timing)"]

        crud -->|committed block| notify
    end

    subgraph sidecar["glint-sidecar"]
        direction TB

        subgraph live["Live path (glint-analytics)"]
            store["In-memory EntityStore<br/>+ roaring bitmap indexes"]
            df["DataFusion<br/>(filter pushdown)"]
            store --> df
        end

        subgraph hist["Historical path (glint-historical)"]
            sqlite["SQLite<br/>(entity_events)"]
            hist_prov["Historical TableProvider"]
            sqlite --> hist_prov
        end

        flight["Flight SQL server<br/>(entities + entity_events)"]
        health["Health endpoint"]

        df --> flight
        hist_prov --> flight
    end

    ring -->|Arrow IPC<br/>unix socket| store
    ring -->|Arrow IPC<br/>unix socket| sqlite

    subgraph clients["Clients"]
        sdk["glint-sdk (Rust)"]
        grafana["Grafana"]
        dbeaver["DBeaver / Jupyter"]
        app["dApps"]
    end

    flight --> sdk
    flight --> grafana
    flight --> dbeaver
    rpc_node --> app
    rpc_node --> sdk

    style node fill:#f0f4f8,stroke:#4a6785,color:#1a2a3a
    style engine fill:#dce6f0,stroke:#4a6785,color:#1a2a3a
    style exex fill:#dce6f0,stroke:#4a6785,color:#1a2a3a
    style sidecar fill:#e8f5e9,stroke:#2e7d32,color:#1a2a3a
    style live fill:#c8e6c9,stroke:#2e7d32,color:#1a2a3a
    style hist fill:#c8e6c9,stroke:#2e7d32,color:#1a2a3a
    style clients fill:#fff3e0,stroke:#e65100,color:#1a2a3a
```

`glint-engine` is a custom `BlockExecutor` inside reth. Transactions sent to a magic address (`0x...676c696e74`, ASCII "glint") get intercepted as entity operations - create, update, delete, extend, change owner. Everything else goes through normal EVM execution. Each entity costs 64 bytes on-chain: 32 bytes of metadata (owner + expiration + flags) and 32 bytes of content hash. Expired entities get cleaned up before each block's transactions run.

`glint-exex` watches committed blocks, converts entity event logs into Arrow RecordBatches, holds them in a ring buffer for replay, and pushes them over a unix socket.

`glint-sidecar` is the query layer. It runs as a separate process, consumes the ExEx stream, and serves two tables over Flight SQL:

- **`entities`** - live entity state held in memory by `glint-analytics`. Roaring bitmap indexes on owner, string annotations, and numeric annotations. DataFusion with filter pushdown.
- **`entity_events`** - historical event log persisted to SQLite by `glint-historical`. Block-range queries.

If the sidecar crashes or falls behind, the node keeps producing blocks. On reconnect the ExEx replays from its ring buffer.

`glint-sdk` is a Rust client library for building and sending Glint transactions, querying entities over JSON-RPC, and running Flight SQL queries.

### Query engine

Everything lives in memory. Glint entities expire, so the live set is bounded by creation rate times MAX_BTL. For realistic workloads - intent protocols, oracle feeds, compute marketplaces - that's tens of thousands to low hundreds of thousands of active entities, not millions. Orders expire in minutes, price feeds even faster. At 100K entities with 10 annotations each and ~500 byte payloads, you're looking at ~100MB. Even aggressive usage stays under a gigabyte.

Entity data is stored as Arrow RecordBatches - columnar, cache-friendly. DataFusion runs analytical queries (aggregations, GROUP BY, window functions) with vectorized execution directly on this data. Nothing gets serialized between formats - Arrow from ExEx through to query results.

Pure columnar has a problem though: annotation lookups ("find all USDC/WETH orders where price > 3500") hit every row. Arkiv used SQLite bitmap indexes for this - works well for point lookups, but SQLite is row-oriented so you lose columnar analytics.

Secondary indexes sit alongside the Arrow data - hash indexes on annotation key/value pairs and owner, a B-tree for numeric range queries, all backed by roaring bitmaps. A custom DataFusion `TableProvider` checks incoming filters against these indexes. If a filter matches an indexed field, it resolves via bitmap lookup in microseconds. If not, DataFusion does a full columnar scan, which is still fast for analytics. One engine, one copy of the data.

If we do need to support longer-lasting entities that won't fit in RAM, there's [datafusion-table-providers](https://github.com/datafusion-contrib/datafusion-table-providers). It can use SQLite (or others) as storage with DataFusion as the query layer. We'd lose OLAP performance though.

Supported indexed operations: equality and inequality on `owner`, `str_ann()`, and `num_ann()`; range queries (`>`, `>=`, `<`, `<=`) on numeric annotations; `IN` lists on all indexed fields; `AND`/`OR` combinations. Unrecognized filters fall through to DataFusion's post-scan filtering.

<details>
<summary>Other query engines considered</summary>

- SQLite - good indexes, but row-oriented. Loses columnar analytics and needs Arrow-to-row conversion on every ingest. Considered as a backend behind DataFusion, but unnecessary when the live entity set fits in memory.
- DuckDB - C++ with Rust FFI. Fast, but adds a C++ dependency and ~30MB to the binary.
- SpacetimeDB - standalone server, can't use as a library. BSL licensed.
- ClickHouse via chdb-rust - ~125 downloads/month, experimental API, 300MB shared library.
- SurrealDB - BSL license and full DB engine overhead.
- Materialize / ReadySet / RisingWave - streaming SQL, all need separate servers. Materialize and ReadySet are BSL.
- Feldera (DBSP) - MIT licensed, but SQL layer needs a Java (Apache Calcite) build step.
- Parquet files from ExEx - duplicates data already in MDBX, reads are always stale, file management becomes the ExEx's problem.
</details>

## How it works

### Entity lifecycle

1. **Create** (anyone) - Send a transaction to the processor address with RLP-encoded `GlintTransaction` operations. The sender becomes the owner. The entity gets a deterministic key (`keccak256(tx_hash || payload_len || payload || op_index)`), 64 bytes written to trie, and a lifecycle event log emitted. The full payload lives only in the event log, not in the trie. You can optionally set an `operator` and an `extend_policy` at creation time.

   A 32-byte content hash goes on-chain so clients can verify query results against the trie. A malicious sequencer can't serve altered data without the hash mismatch showing up in a Merkle proof.

2. **Update** (owner or operator) - Replace payload and annotations, reset the BTL. Same key, new content. The operator can update content and BTL but cannot change permissions (extend_policy or operator) - only the owner can do that.

3. **Extend** (depends on policy) - Add blocks to remaining lifetime, capped at MAX_BTL. If `extend_policy` is `AnyoneCanExtend`, anyone can do this - so if you depend on someone's data, you can keep it alive. If `OwnerOnly`, only the owner or operator can extend.

4. **ChangeOwner** (owner only) - Transfer ownership, change the extend policy, and/or set or remove the operator, all in one atomic operation. At least one field must change. Operators cannot call this - only the entity owner.

5. **Delete** (owner or operator) - Immediate removal.

6. **Expire** (automatic) - At the start of each block, before any transactions execute, the engine checks an in-memory expiration index and removes everything whose TTL has elapsed. The index isn't stored on-chain - on cold start it rebuilds by scanning MAX_BTL blocks of event logs.

### Recovery

Everything in-memory rebuilds from the chain. No snapshots, no separate sync mode.

On node restart, `glint-engine` scans MAX_BTL blocks of entity event logs from reth's database to reconstruct the expiration index. Log reading only, not EVM re-execution - at ~1 week of history (302,400 blocks at 2s) this takes seconds to a few minutes.

On sidecar restart, it connects to the ExEx IPC stream and rebuilds from empty. The ExEx replays from its ring buffer, and after MAX_BTL blocks from tip all live entities are reconstructed. Anything older is already expired. Historical events are persisted in SQLite and survive restarts. Crash, disconnect, fresh deploy - same path every time.

If the ExEx's IPC buffer overflows (1024 batches, ~34 min of headroom at 2s blocks), it disconnects the sidecar, which rebuilds from scratch on reconnect. If the ExEx itself panics, reth keeps producing blocks and retains notifications until the ExEx catches up.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT) at your option.
