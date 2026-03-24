# Mote

Ephemeral on-chain storage as an OP Stack L3, built on reth.

Store data with a TTL. Query it with SQL. It expires on its own. Mote adds a BTL (Blocks-to-Live) primitive to Ethereum - entities you create disappear when their time is up, and annotations let you query them while they're alive.

## Why

Blockchains store data permanently. If you want to publish "I have 4 GPUs free for the next hour," you pay to store it forever even though nobody needs it after that hour. There's no native TTL in Ethereum.

Mote gives you cheap writes, bounded state, automatic cleanup, and SQL over annotations.

We're looking at AI agent coordination, intent protocols (UniswapX/CoW), compute marketplaces, ephemeral registries - basically anywhere people publish short-lived structured records and others need to query them.

## Architecture

```
mote-node (reth + OP Stack L3)
┌──────────────────────────────────────────────────┐
│                                                  │
│  mote-engine (Custom BlockExecutor)              │
│  ┌────────────────────────────────────────────┐  │
│  │  Wraps EthBlockExecutorFactory             │  │
│  │  Intercepts txs to PROCESSOR_ADDRESS       │  │
│  │  Entity CRUD → trie writes (64 bytes)      │  │
│  │  Expiration housekeeping (pre-execution)    │  │
│  │  Emits lifecycle event logs                │  │
│  └────────────────────────────────────────────┘  │
│                                                  │
│  mote-exex (ExEx)                                │
│  ┌────────────────────────────────────────────┐  │
│  │  Receives ExExNotification (commit/reorg)  │  │
│  │  Entity event logs → Arrow RecordBatch     │  │
│  │  Streams via Arrow IPC (unix socket)       │──┼──► mote-analytics
│  │  Writes nothing, queries nothing           │  │
│  └────────────────────────────────────────────┘  │
│                                                  │
└──────────────────────────────────────────────────┘

mote-analytics (separate process)
┌──────────────────────────────────────────────────┐
│  In-memory Arrow tables (all live entities)      │
│  DataFusion query engine                         │
│  Flight SQL server (Grafana, DBeaver, notebooks) │
│  Entity query RPC (JSON-RPC)                     │
└──────────────────────────────────────────────────┘
```

Three components, two processes.

`mote-engine` lives inside reth as a custom `BlockExecutor`. Transactions to a magic address (`0x...6d6f7465`, ASCII "mote") get intercepted and processed as entity operations - create, update, delete, extend. Everything else passes through to normal EVM execution. Each entity costs 64 bytes on-chain: 32 bytes of metadata (owner + expiration) and 32 bytes of content hash. Expired entities get cleaned up at the start of each block.

`mote-exex` is a reth Execution Extension. Watches committed blocks, converts entity lifecycle event logs into Arrow RecordBatches, streams them over a unix socket. A data faucet - writes nothing, queries nothing, tracks no consumer state.

`mote-analytics` is a separate binary consuming the Arrow stream. Keeps an in-memory table of all live entities, serves SQL via Flight SQL (Grafana, DBeaver, Jupyter) and a JSON-RPC endpoint. Can crash, restart, or fall behind without affecting block production.

### Why two processes

If the query service OOMs or DataFusion panics, blocks keep getting produced. The two things are completely independent. You can upgrade or restart the query service without touching the node.

## How it works

### Entity lifecycle

1. **Create** - Send a transaction to the processor address with RLP-encoded operations. The entity gets a deterministic key (`keccak256(tx_hash || payload_len || payload || op_index)`), 64 bytes written to trie, and a lifecycle event log emitted. The full payload lives only in the event log, not in the trie.

2. **Update** (owner only) - Replace payload and annotations, reset the BTL. Same key, new content.

3. **Extend** (anyone) - Add blocks to remaining lifetime, capped at MAX_BTL. Permissionless so that anyone who depends on the data can keep it alive.

4. **Delete** (owner only) - Immediate removal.

5. **Expire** (automatic) - At the start of each block, before any transactions execute, the engine checks the in-memory expiration index and removes everything whose TTL has elapsed.

### Data flow

```
User tx → reth → MoteBlockExecutor
                   ├── processor address? → entity CRUD → trie writes + event logs
                   └── other address?     → standard EVM execution

Committed block → ExEx → parse entity event logs → Arrow RecordBatch
                         → IPC unix socket → mote-analytics
                                              → in-memory state → Flight SQL / JSON-RPC → clients
```

### Wire format

Transactions carry RLP-encoded `MoteTransaction` structs with four operation types: creates, updates, deletes, extends. No per-transaction compression. The OP Stack batcher already compresses batches, and skipping per-tx compression eliminates the decompression bomb attack surface.

### Content hash

Each entity stores a 32-byte content hash on-chain: `keccak256(payload || content_type || rlp(string_annotations) || rlp(numeric_annotations))`. Computed from raw wire bytes, never decoded and re-encoded. This lets clients verify that what the query service returned matches what's committed on-chain. A malicious sequencer can't serve altered data without the hash mismatch being detectable via Merkle proof.

### Expiration tracking

The expiration index is an in-memory `HashMap<BlockNumber, Vec<EntityKey>>`, not stored on-chain. This saves 33% of trie costs compared to GolemBase's on-chain EnumerableSet. On cold start, the index gets rebuilt by scanning MAX_BTL blocks of event logs - takes seconds to a few minutes.

## What we do differently from GolemBase

Mote is a ground-up rewrite of [GolemBase](https://github.com/ArkivNetwork/golembase-op-geth) (also called Arkiv), an op-geth fork with ~5,900 lines of custom Go. The concepts are good. The implementation will be different in our case.

### What we keep

- Magic address interception (no contract deployment needed)
- BTL-based auto-expiration
- Content-addressed entity keys (`keccak256(tx_hash, payload, op_index)`)
- String + numeric annotation model
- Atomic multi-operation transactions
- Ownership model (owner = tx.sender, owner-gated mutations)

### What we change

| | GolemBase | Mote | Why |
|---|---|---|---|
| Base | op-geth fork (~5,900 lines) | reth plugin (BlockExecutor + ExEx) | Forks die when upstream moves. reth's trait system lets us extend without forking. |
| Language | Go | Rust | Type system prevents entire classes of bugs (see below) |
| On-chain cost | ~96 bytes/entity (3 slots) | 64 bytes/entity (2 slots) | Moved the expiration index off-chain. 33% cheaper per entity, forever. |
| Content integrity | None | 32-byte content hash | Without it, a sequencer can serve fake data and nobody can prove it |
| Query engine | SQLite (in-process, fire-and-forget goroutine) | DataFusion (separate process, Arrow streaming) | The SQLite goroutine can crash silently and serve stale data forever |
| Compression | Brotli per-tx | None | OP batcher compresses. Per-tx Brotli has a decompression bomb in the txpool path (`io.ReadAll` with no size limit). |
| MAX_BTL | Not enforced | Enforced at txpool + execution | Without it, entities live forever. The whole "ephemeral" thing falls apart. |
| Extend | Permissionless, no cap | Permissionless, capped at MAX_BTL | GolemBase lets anyone extend any entity to infinity |
| Gas for operations | Zero (hardcoded `GasUsed: 0`) | Proportional to cost (planned) | Zero gas = unlimited free operations = DoS |
| ChangeOwner | Supported | Removed | Delete + recreate is simpler, doesn't break external key references |


## Crate structure

```
mote/crates/
├── mote-primitives/     # Core types: Entity, MoteTransaction, annotations, storage keys
├── mote-engine/         # Custom BlockExecutorFactory wrapping EthBlockExecutorFactory
├── mote-exex/           # ExEx: event logs → Arrow RecordBatch → IPC stream
├── mote-analytics/      # Separate binary: DataFusion + Flight SQL + entity RPC
└── mote-node/           # Node binary: reth NodeBuilder + custom executor + ExEx
```


