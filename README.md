# Mote

Ephemeral on-chain storage as an OP Stack L3, built on reth.

Mote adds a BTL (Blocks-to-Live) primitive to Ethereum. Entities have a TTL, carry queryable annotations, and disappear when their time is up.

## Why

Blockchains store data permanently. If you want to publish a limit order that's valid for the next 10 blocks, you pay to store it forever even though nobody needs it after that. There's no native TTL in Ethereum.

Think intent protocols (UniswapX/CoW), ephemeral registries, oracle price feeds with built-in staleness, compute marketplaces, AI agent coordination - anywhere people publish short-lived structured records and others need to query them.

## Relationship to GolemBase

Mote wouldn't exist without [GolemBase](https://github.com/Arkiv-Network/arkiv-op-geth) (also called Arkiv). The GolemBase team designed the core model - magic address interception, BTL expiration, content-addressed keys, annotation model, atomic ops, owner-gated mutations.

Why rewrite instead of fork: GolemBase is an op-geth fork, and Optimism is phasing out op-geth in favor of reth. A geth fork is a dead end. Mote takes the same ideas and implements them as a reth plugin.

Beyond the base change, Mote also fixes a few things:

| | GolemBase | Mote | Why |
|---|---|---|---|
| Base | op-geth fork | reth plugin (BlockExecutor + ExEx) | Optimism is dropping op-geth. |
| On-chain cost | ~96 bytes/entity (3 slots) | 64 bytes/entity (2 slots) | Moved the expiration index off-chain. 33% cheaper per entity. |
| Content integrity | None | 32-byte content hash | Without it, a sequencer can serve fake data and nobody can prove it |
| Query engine | SQLite (in-process goroutine) | DataFusion (separate process, Arrow streaming) | See [why Arrow + DataFusion](#why-arrow--datafusion-not-sqlite) |
| Compression | Brotli per-tx | None | OP batcher already compresses. Per-tx Brotli has a decompression bomb in the txpool path (`io.ReadAll` with no size limit). |
| MAX_BTL | Not enforced | Enforced at txpool + execution | Without it, entities live forever. The "ephemeral" thing falls apart. |
| Extend | Permissionless, no cap | Permissionless, capped at MAX_BTL | GolemBase lets anyone extend any entity to infinity |
| ChangeOwner | Supported | Removed | Delete + recreate is simpler, doesn't break external key references |

## Architecture

```mermaid
graph TB
    subgraph node["mote-node (reth + OP Stack L3)"]
        direction TB

        subgraph engine["mote-engine (BlockExecutor)"]
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

        subgraph exex["mote-exex (ExEx)"]
            notify["ExExNotification<br/>(commit/reorg)"]
            arrow["Entity event logs<br/>→ Arrow RecordBatch"]
            notify --> arrow
        end

        crud -->|committed block| notify
    end

    subgraph analytics["mote-analytics (separate process)"]
        mem["In-memory Arrow tables<br/>(all live entities)"]
        df["DataFusion query engine"]
        flight["Flight SQL server"]
        rpc["JSON-RPC endpoint"]
        mem --> df
        df --> flight
        df --> rpc
    end

    arrow -->|Arrow IPC<br/>unix socket| mem

    subgraph clients["Clients"]
        grafana["Grafana"]
        dbeaver["DBeaver"]
        jupyter["Jupyter"]
        app["dApps"]
    end

    flight --> grafana
    flight --> dbeaver
    flight --> jupyter
    rpc --> app

    style node fill:#f0f4f8,stroke:#4a6785,color:#1a2a3a
    style engine fill:#dce6f0,stroke:#4a6785,color:#1a2a3a
    style exex fill:#dce6f0,stroke:#4a6785,color:#1a2a3a
    style analytics fill:#e8f5e9,stroke:#2e7d32,color:#1a2a3a
    style clients fill:#fff3e0,stroke:#e65100,color:#1a2a3a
```

`mote-engine` is a custom `BlockExecutor` inside reth. Transactions sent to a magic address (`0x...6d6f7465`, ASCII "mote") get intercepted as entity operations - create, update, delete, extend. Everything else goes through normal EVM execution. Each entity costs 64 bytes on-chain: 32 bytes of metadata (owner + expiration) and 32 bytes of content hash. Expired entities get cleaned up before each block's transactions run.

`mote-exex` watches committed blocks, converts entity event logs into Arrow RecordBatches, and pushes them over a unix socket. Pure output - no state of its own.

`mote-analytics` runs as a separate binary consuming that stream. In-memory table of all live entities, SQL via Flight SQL, JSON-RPC endpoint. If it crashes or falls behind, blocks keep getting produced - the node doesn't know or care.

### Why Arrow + DataFusion (not SQLite)

Arrow is the in-memory format at every stage - from ExEx output through query execution. Nothing gets serialized or copied between formats.

| | SQLite (GolemBase) | DataFusion + Arrow (Mote) |
|---|---|---|
| Isolation | In-process goroutine. If it dies, queries silently go stale with no health check or recovery. | Separate process. Can OOM or panic without touching block production. |
| Data format | Row-oriented. Every ingest serializes into SQLite's B-tree pages. | Columnar RecordBatches end-to-end. Zero-copy from ExEx through query execution. |
| Staleness detection | None. Clients can't tell if data is hours old. | Analytics process tracks exactly which block it's caught up to. |
| FFI / build | C dependency via an obscure v0.0.7 bitmap store library. | Pure Rust. `cargo build` just works. |
| Client ecosystem | Custom JSON-RPC only. | Flight SQL out of the box - Grafana, DBeaver, Tableau, Jupyter via standard JDBC/ODBC. Python in 3 lines via `pyarrow.flight`. |
| Extensibility | Ad-hoc SQL schema, manual query plumbing. | Implement DataFusion's `TableProvider` trait, get full SQL with pushdown filters. |
| License | Varies by binding | Apache 2.0, same as reth. |

Other query engines considered for mote-analytics:

- DuckDB - C++ with Rust FFI. Mature and fast, but adds a C++ dependency to the build and ~30MB to the binary. Arrow zero-copy works well, but DataFusion gets the same thing without leaving Rust.
- SpacetimeDB - standalone server, can't use it as a library. The abstraction layer adds milliseconds of latency where DataFusion does microseconds. BSL licensed.
- ClickHouse via chdb-rust - `chdb-rust` has ~125 downloads/month, experimental API, 300MB shared library. ClickHouse server itself needs its own deployment.
- SurrealDB - closest off-the-shelf option (has `LIVE SELECT`, written in Rust), but BSL license and full DB engine overhead for ~50MB of live data.
- Materialize / ReadySet / RisingWave - streaming SQL with incremental view maintenance. All need separate server deployments. Materialize and ReadySet are BSL. RisingWave is Apache 2.0 but designed for distributed cloud-scale, overkill here.
- Feldera (DBSP) - Rust crate for incremental view maintenance, MIT licensed. SQL layer requires a Java (Apache Calcite) build step though. Not practical.
- Parquet files from ExEx instead of streaming - duplicates data already in MDBX, reads are always stale (batch flush lag), and file management (rotation, cleanup, reorg tombstones) becomes the ExEx's problem.

## How it works

### Entity lifecycle

1. **Create** - Send a transaction to the processor address with RLP-encoded `MoteTransaction` operations. The entity gets a deterministic key (`keccak256(tx_hash || payload_len || payload || op_index)`), 64 bytes written to trie, and a lifecycle event log emitted. The full payload lives only in the event log, not in the trie.

   A 32-byte content hash (`keccak256(payload || content_type || rlp(annotations))`) goes on-chain so clients can verify query results against the trie. A malicious sequencer can't serve altered data without the hash mismatch showing up in a Merkle proof.

2. **Update** (owner only) - Replace payload and annotations, reset the BTL. Same key, new content.

3. **Extend** (anyone) - Add blocks to remaining lifetime, capped at MAX_BTL. Permissionless - if you depend on someone's data, you can keep it alive.

4. **Delete** (owner only) - Immediate removal.

5. **Expire** (automatic) - At the start of each block, before any transactions execute, the engine checks an in-memory expiration index (`HashMap<BlockNumber, Vec<EntityKey>>`) and removes everything whose TTL has elapsed. The index isn't stored on-chain - on cold start it rebuilds by scanning MAX_BTL blocks of event logs.

### Recovery

Everything in-memory rebuilds from the chain. No snapshots, no separate sync mode.

On node restart, `mote-engine` scans MAX_BTL blocks of entity event logs from reth's database to reconstruct the expiration index. Log reading only, not EVM re-execution - at ~1 week of history (302,400 blocks at 2s) this takes seconds to a few minutes.

On analytics restart, `mote-analytics` connects to the ExEx IPC stream and rebuilds from empty. The ExEx replays from its WAL checkpoint, and after MAX_BTL blocks from tip all live entities are reconstructed. Anything older is already expired. Crash, disconnect, fresh deploy - same path every time.

If the ExEx's IPC buffer overflows (1024 batches, ~34 min of headroom at 2s blocks), it disconnects mote-analytics, which rebuilds from scratch on reconnect. If the ExEx itself panics, reth keeps producing blocks and the WAL retains notifications until the ExEx catches up.

## Crate structure

```
mote/crates/
├── mote-primitives/     # Core types: Entity, MoteTransaction, annotations, storage keys
├── mote-engine/         # Custom BlockExecutorFactory wrapping EthBlockExecutorFactory
├── mote-exex/           # ExEx: event logs → Arrow RecordBatch → IPC stream
├── mote-analytics/      # Separate binary: DataFusion + Flight SQL + entity RPC
└── mote-node/           # Node binary: reth NodeBuilder + custom executor + ExEx
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT) at your option.
