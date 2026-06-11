# Raft Notes

## What is a Term?

A term is a logical clock used by Raft.

Each election starts a new term.

Terms help nodes determine which information is newer and prevent outdated leaders from continuing to serve.

---

## Why use Random Election Timeout?

If all nodes start elections at the same time, votes may split and no leader can be elected.

Random election timeouts reduce the chance of vote splitting.

This increases the probability that one candidate receives a majority of votes.

---

## What is Majority?

Majority means more than half of the nodes.

Examples:

* 3 nodes → majority = 2
* 5 nodes → majority = 3

Raft requires a majority to elect a leader and commit log entries.

---

## Why can there be at most one Leader in a Term?

A node can vote for only one candidate in a given term.

Because a leader must receive a majority of votes, two different candidates cannot both obtain a majority in the same term.

Therefore, there can be at most one leader per term.

---

## Why does AppendEntries also act as Heartbeat?

AppendEntries is used to replicate logs.

When the leader sends an AppendEntries RPC with no log entries, it acts as a heartbeat.

Followers use these heartbeats to know that the leader is still alive.
