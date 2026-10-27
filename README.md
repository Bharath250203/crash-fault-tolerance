# Distributed Banking System with Paxos Consensus

A fault-tolerant distributed banking application implementing Stable-Leader Paxos for transaction consensus across multiple replicas.

## Overview

This system implements a distributed banking application where multiple clients can submit balance transfer transactions to a cluster of 5 nodes. The system uses Paxos consensus protocol to ensure all transactions are consistently replicated and executed exactly once, even in the presence of node failures.

## Key Features

- **Stable-Leader Paxos**: Single PREPARE phase per leader, multiple ACCEPT phases for transactions
- **Fault Tolerance**: Tolerates up to 2 node failures (f=2) with 5 total nodes
- **Exactly-Once Semantics**: Dual-level deduplication (Paxos-level and execution-level)
- **Leader Election**: Randomized timeout-based election with ballot number ordering
- **Parallel Clients**: 10 concurrent clients (A-J) with independent timers
- **Gap-Aware Commit Processing**: Automatic catch-up for out-of-order commits
- **NEW-VIEW Recovery**: Consolidates uncommitted transactions after leader changes

## System Architecture

### Components

- **5 Nodes**: Replicated state machines maintaining consistent transaction logs
- **10 Clients**: Submit transactions in parallel (closed-loop per client)
- **Paxos Protocol**: Leader election, normal operations, and recovery mechanisms
- **Key-Value Store**: Maintains client account balances

### File Structure

```
CSE535_Banking_System/
├── cmd/
│   └── main.go           # Main program, test harness, client logic
├── paxos/
│   ├── node.go           # Paxos node implementation, consensus logic
│   ├── rpc.go            # RPC handlers for inter-node and client communication
│   └── types.go          # Protocol message definitions
├── store/
│   └── kvstore.go        # Persistent key-value storage for balances
└── test.csv              # Test case input file
```

## Building and Running

### Prerequisites

- Go 1.16 or higher
- Unix-like environment (macOS, Linux)

### Build

```bash
go build -o exe/main cmd/main.go
```

### Run

```bash
./exe/main
```

The program will:
1. Parse test cases from `test.csv`
2. Initialize 5 nodes and 10 clients
3. Wait for user input to process each test set
4. Display results after each set

## Usage

### Interactive Commands

During execution, press Enter to advance to the next test set, or use these commands:

- `whoisleader` - Display current leader and all node states
- `status <nodeID>` - Show node status
- `printlog <nodeID>` - Display AcceptLog for a node
- `printview <nodeID>` - Display NEW-VIEW messages
- `printdb <nodeID>` - Show current account balances
- `printstatus <nodeID> <seqNum>` - Check status of a sequence number (A/C/E/X)
- `kill <nodeID>` - Crash a specific node
- `pause` - Pause all election timers
- `resume` - Resume all election timers
- `exit` - Terminate the program

### Test Case Format

CSV file with 3 columns: `Set Number`, `Transactions`, `Live Nodes`

Example:
```csv
Set Number,Transactions,Live Nodes
1,"(A, J, 3)","[n1, n2, n3, n4, n5]"
,"(C, D, 1)",
2,"(B, I, 2)","[n1, n2, n3, n4]"
,LF,
,"(E, F, 1)",
```

- **Transactions**: `(Sender, Receiver, Amount)`
- **Live Nodes**: List of active nodes for the set
- **LF**: Leader Failure marker (kills current leader)

## Implementation Highlights

### Paxos Protocol

**Leader Election**:
- Nodes use randomized timeouts (2-3 seconds) to detect leader failure
- PREPARE/PROMISE messages with ballot number ordering
- Majority quorum (3 of 5 nodes) required for election

**Normal Operations**:
- Leader receives client requests and assigns sequence numbers
- ACCEPT messages broadcast to all nodes
- COMMIT sent after receiving majority ACCEPTED responses
- Transactions executed and replies sent to clients

**Recovery (NEW-VIEW)**:
- New leader consolidates AcceptLog from majority promises
- Fills gaps with no-op operations
- Re-proposes uncommitted transactions with new ballot number

### Fault Tolerance

**Leader Failure**:
- Detected via missing heartbeats
- Automatic election triggered
- NEW-VIEW recovers uncommitted transactions

**Node Crash**:
- Timers paused to prevent ballot inflation
- Node excluded from quorum calculations
- Recovers by catch-up on next set

**Network Partitions**:
- Minority partitions cannot make progress
- Majority partition continues normal operations
- Transactions from no-quorum periods may be lost (Paxos-safe)

### Exactly-Once Guarantees

**Paxos-Level**: Checks if RequestID exists in AcceptLog before creating new instance

**Execution-Level**: Tracks ExecutedReqIDs to prevent duplicate execution on replays

**RPC-Level**: Stores LastReplies by ClientID for idempotent responses

## Client Behavior

- Each of 10 clients maintains independent request sequence and timer
- Closed-loop: waits for response before sending next request
- Different clients execute transactions in parallel
- No knowledge of node liveness (tries nodes on RPC failure)
- Always contacts node n1 first when leader unknown
- No retry on insufficient balance
- Retries up to 3 times on timeout or unreachable nodes

## Performance Characteristics

- **Leader Election**: 2-3 seconds (randomized timeout)
- **Transaction Latency**: 100-500ms (leader + majority)
- **Recovery Time**: 1-3 seconds (election + NEW-VIEW)
- **Client Timeout**: 5 seconds per request
- **Heartbeat Interval**: 300ms

All tests verify:
- Consistency: all nodes have same final balances
- Exactly-once: no duplicate transaction execution
- Safety: minority partitions cannot commit
- Liveness: majority partitions make progress

## Notes

- Database files (`node_*.db`) are SQLite databases persisted per node
- System uses HTTP-based RPC for inter-node and client-node communication
- Timers are paused during result presentation to prevent spurious elections
- Final balances may vary depending on leader identity and NEW-VIEW outcomes when transactions are accepted by a minority

## Authors
Bharath Sai Lingampalli
Course: CSE 535 - Distributed Systems
Project: Paxos Consensus Implementation

