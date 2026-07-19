# MiniRaftKV

A distributed key-value store built from scratch in Java — implementing the **Raft consensus algorithm** for replication and an **LSM-Tree storage engine** for persistence, connected over **gRPC**.

This is a learning project written without consensus or storage libraries: leader election, log replication, crash recovery, MemTable/SSTable, and compaction are all implemented by hand.

---

## Features

**Raft consensus**
- Leader election with randomized election timeouts
- Log replication: append → replicate → commit → apply
- Log consistency check and conflict resolution (truncate divergent entries, overwrite with leader's)
- Real vote logic (term comparison + one vote per term, persisted)
- Crash recovery: `currentTerm`, `votedFor`, and log entries persisted to disk

**LSM-Tree storage engine**
- `MemTable` — in-memory sorted table (`TreeMap`), overwrite-on-write deduplication
- `SSTable` — immutable sorted file on disk, written by sequential batch flush
- Automatic flush when the MemTable reaches capacity
- Reads check MemTable first, then SSTables newest → oldest
- `compact()` — merges SSTables (old → new, so newer values win), removes stale entries and deletes obsolete files

**Client interface & benchmarking**
- `ClientCommand` gRPC endpoint (leader-only writes)
- Multi-threaded concurrent benchmark harness measuring throughput

---

## Architecture

```
                Client
                  │  ClientCommand (gRPC)
                  ▼
        ┌──────────────────┐
        │   Raft Leader    │
        │  ┌────────────┐  │   AppendEntries (gRPC)
        │  │  Raft Log  │──┼──────────────────────► Followers
        │  └─────┬──────┘  │                        (replicate)
        │        │ commit  │
        │        ▼         │
        │  ┌────────────┐  │
        │  │  LSMStore  │  │   ← state machine
        │  └─────┬──────┘  │
        └────────┼─────────┘
                 │
        ┌────────▼─────────────────────────┐
        │  MemTable (memory, sorted)       │
        │        │ flush when full         │
        │        ▼                         │
        │  SSTable_1  SSTable_2  ...       │  (disk, immutable)
        │        │ compact                 │
        │        ▼                         │
        │  SSTable_merged                  │
        └──────────────────────────────────┘
```

**Write path:** client → leader appends to Raft log → replicated to followers via heartbeats → committed once a majority acknowledges → applied to `LSMStore` → buffered in MemTable → flushed to an SSTable when full.

**Read path:** MemTable first (newest data), then SSTables from newest to oldest — the first hit is the current value, since newer SSTables shadow older ones.

---

## Project structure

```
src/main/java/com/miniraftkv/
├── node/
│   ├── Node.java         # Raft node: election, replication, commit/apply, persistence
│   └── Benchmark.java    # concurrent throughput benchmark
├── rpc/
│   ├── RaftClient.java       # outbound RPCs (RequestVote / AppendEntries / ClientCommand)
│   └── RaftServiceImpl.java  # inbound gRPC service
├── storage/
│   ├── MemTable.java     # in-memory sorted table
│   ├── SSTable.java      # immutable on-disk sorted file
│   ├── LSMStore.java     # put / get / flush / compact
│   └── LSMTest.java      # storage engine tests
├── log/LogEntry.java
├── state/NodeState.java  # FOLLOWER / CANDIDATE / LEADER
└── proto/raft.proto      # gRPC service and message definitions
```

---

## Running

**Requirements:** Java 8+, Maven

```bash
mvn compile
```

**Run a 3-node cluster** (starts three nodes on ports 5001–5003, elects a leader, writes two keys, then reads them back from the LSM store):

```bash
mvn exec:java -Dexec.mainClass=com.miniraftkv.node.Node
```

Expected output:

```
>>> STEP 1: waiting for election...
[Node3] Became CANDIDATE, term=1, requesting votes from peers...
[Node3] Became LEADER for term 1!
>>> STEP 3: leader accepted!
[Node3] Appended: LogEntry{term=1, index=1, command='set x=5'}
[Node3] Applied set x=5
>>> STEP 6: reading from LSM
Node3: x = 5, y = 10
```

**Run the storage engine tests:**

```bash
mvn exec:java -Dexec.mainClass=com.miniraftkv.storage.LSMTest
```

**Run the benchmark:**

```bash
mvn exec:java -Dexec.mainClass=com.miniraftkv.node.Benchmark
```

---

## Benchmark results

Local 3-node cluster, single machine.

| Setup | Threads | Throughput |
|---|---|---|
| Serial client | 1 | ~850 QPS |
| Concurrent clients | 100 | **6,000+ QPS** (~7×) |

The gain comes from overlapping request latency across clients — a serial client spends most of its time waiting on the round trip. Peak throughput plateaued around 100–200 threads.

*Note on methodology:* these are throughput figures (total requests / wall time). Per-request latency and p99 are **not** measured — the average latency derived from throughput would not reflect real per-request distribution.

Single runs are also noisy (the same 150-thread config produced 3,900 and 5,200 QPS on different runs, likely due to JVM warm-up, election state, and machine load), so numbers were taken as peaks across multiple runs.

---

## Debugging notes

Two stability issues worth recording:

**Leader repeatedly triggering new elections.** Adding the node's state to the election log revealed `state=LEADER` while starting an election. `becomeLeader()` cancels the election timer correctly, but `startElection()` unconditionally called `resetElectionTimer()` at the end — re-arming the timer after the node had already won. Fixed by only resetting when still a candidate:

```java
if (state == NodeState.CANDIDATE) {
    resetElectionTimer();
}
```

**Commands appended but never applied.** Commands reached the leader's log, but `Applied` never printed. Tracing showed the leader was replaced within a few hundred milliseconds of appending — and since `applyLog()` runs inside `sendHeartbeats()`, which returns early for non-leaders, the entries were never applied.

The root cause is that every RPC (vote request, heartbeat, replication) constructs a **new gRPC channel synchronously**, so a single heartbeat round could take longer than a follower's election timeout. Election timeouts were raised to 3–6s as an interim fix. The proper fix is connection reuse and parallel/async vote requests.

---

## Known limitations

Deliberately listed — these are real gaps, not oversights:

- **Followers do not apply committed entries.** `applyLog()` only runs in the leader's heartbeat loop. In real Raft, followers apply based on `leaderCommit`. Currently only the leader holds complete state machine data.
- **`prevLogIndex` / `prevLogTerm` are hardcoded to 0** in `sendHeartbeatToPeer`, so the consistency-check path is not fully exercised end to end.
- **No snapshotting or log compaction.** The Raft log grows unbounded; restart replays the entire log.
- **Compaction is manually triggered.** No background or threshold-based compaction, so SSTables accumulate until `compact()` is called explicitly.
- **Election stability relies on tuned timeouts** rather than fixing the underlying synchronous per-RPC connection setup.
- **SSTable lookups are linear scans.** Files are sorted, so binary search (or a sparse index / Bloom filter) would be the natural optimization.

---

## Possible next steps

1. Apply on followers via `leaderCommit` — makes it a true replicated state machine
2. Reuse gRPC channels; make vote requests parallel — removes the root cause of election churn
3. Snapshotting + `InstallSnapshot` RPC — bounds log growth and speeds up follower catch-up
4. Background compaction with a size/count trigger
5. Binary search and Bloom filters over SSTables to reduce read amplification
6. Per-request latency instrumentation with p50/p95/p99 distributions

---

## Built with

Java · gRPC · Protocol Buffers · Maven
