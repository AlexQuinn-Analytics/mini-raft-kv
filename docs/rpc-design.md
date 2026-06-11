# Raft RPC Design

## Overview

Raft uses two core RPCs:

1. RequestVote
2. AppendEntries

RequestVote is used during leader election.

AppendEntries is used for both heartbeat and log replication.

---

# RequestVote RPC

## Request

```proto
message RequestVoteRequest {
    int64 term = 1;
    string candidateId = 2;
    int64 lastLogIndex = 3;
    int64 lastLogTerm = 4;
}
```

### Fields

| Field        | Description                   |
| ------------ | ----------------------------- |
| term         | Candidate's current term      |
| candidateId  | Candidate node ID             |
| lastLogIndex | Index of candidate's last log |
| lastLogTerm  | Term of candidate's last log  |

---

## Response

```proto
message RequestVoteResponse {
    int64 term = 1;
    bool voteGranted = 2;
}
```

### Fields

| Field       | Description             |
| ----------- | ----------------------- |
| term        | Receiver's current term |
| voteGranted | Whether vote is granted |

---

# AppendEntries RPC

## Request

```proto
message AppendEntriesRequest {
    int64 term = 1;
    string leaderId = 2;
    int64 prevLogIndex = 3;
    int64 prevLogTerm = 4;
    repeated LogEntry entries = 5;
    int64 leaderCommit = 6;
}
```

### Fields

| Field        | Description              |
| ------------ | ------------------------ |
| term         | Leader's current term    |
| leaderId     | Leader node ID           |
| prevLogIndex | Previous log index       |
| prevLogTerm  | Previous log term        |
| entries      | Log entries to replicate |
| leaderCommit | Leader's commit index    |

---

## Response

```proto
message AppendEntriesResponse {
    int64 term = 1;
    bool success = 2;
}
```

### Fields

| Field   | Description              |
| ------- | ------------------------ |
| term    | Receiver's current term  |
| success | Whether append succeeded |

---

# Log Entry

```proto
message LogEntry {
    int64 term = 1;
    string command = 2;
}
```

Example:

```text
SET x 1
SET user Alex
```

---

# Heartbeat

Heartbeat is a special AppendEntries request.

The leader sends:

```text
entries = []
```

No log entries are included.

Purpose:

* Keep followers alive
* Prevent unnecessary elections
* Maintain leadership

---

# Future Extensions

The following features are intentionally postponed:

* Snapshot
* Joint Consensus
* ReadIndex
* Lease Read
* Multi-Raft
* RocksDB Storage

Current project scope focuses only on core Raft functionality.
