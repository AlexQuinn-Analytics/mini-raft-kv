# Raft State Transition

## Overview

Raft has three node states:

1. Follower
2. Candidate
3. Leader

A node can only be in one state at a time.

---

## State Diagram

```text
                election timeout

      +-----------------------------+
      |                             |
      v                             |

  Follower  -----------------> Candidate
      ^                           |
      |                           |
      |                           | majority votes
      |                           |
      |                           v

      +---------------------- Leader

```

---

## Follower

Responsibilities:

* Respond to RequestVote RPC
* Respond to AppendEntries RPC
* Receive heartbeats from leader

State Transition:

Follower → Candidate

Condition:

* Election timeout occurs
* No heartbeat received from leader

Actions:

* Increment currentTerm
* Vote for itself
* Start election

---

## Candidate

Responsibilities:

* Start leader election
* Send RequestVote RPC
* Count received votes

State Transition:

Candidate → Leader

Condition:

* Receives majority votes

Actions:

* Become leader
* Start sending heartbeats

---

Candidate → Follower

Condition:

* Receives RPC with higher term

Actions:

* Update currentTerm
* Stop election
* Become follower

---

Candidate → Candidate

Condition:

* Election timeout occurs again
* No majority achieved

Actions:

* Start a new election
* Increment currentTerm

---

## Leader

Responsibilities:

* Send heartbeats
* Replicate logs
* Process client requests

State Transition:

Leader → Follower

Condition:

* Receives message with higher term

Actions:

* Update currentTerm
* Step down immediately
* Become follower

---

## Key Rule

At most one leader can exist in a term.

Reason:

* A node can vote only once per term.
* A leader requires a majority of votes.
* Two different nodes cannot both obtain a majority in the same term.
