# Mini Dynamo: A Distributed Key-Value Store

Mini Dynamo is a fault-tolerant, highly available distributed key-value store inspired by Amazon's Dynamo architecture. It demonstrates core distributed systems concepts including Consistent Hashing, Vector Clocks, Gossip protocol, and Hinted Handoff to guarantee partition tolerance and eventual consistency.

## Architecture & How It Works

Nodes expose a lightweight HTTP API for inter-node communication and client access.

### Consistent Hash Ring
- Data is partitioned across nodes using a **Consistent Hash Ring**. 
- To distribute load evenly, each physical physical node uses multiple "Virtual Nodes" mapped onto the ring.
- Keys are hashed (using SHA-256) and placed onto the closest node moving clockwise around the ring.

### Request Coordination
Any node receiving a client request acts as the coordinator. It determines the responsible replica set via the hash ring and forwards requests to the appropriate nodes, collecting responses to satisfy the quorum requirements.

### Quorum Replication
- Writes and Reads are replicated across an `N` number of nodes (the "Preference List").
- We use Quorum parameters `W` (Write Quorum) and `R` (Read Quorum).
- A write is considered successful only when at least `W` nodes acknowledge the write.
- A read is considered successful only when at least `R` nodes return data. `W + R > N` guarantees high consistency.

### Vector Clocks & Conflict Resolution
- To remain highly available during network partitions, Mini Dynamo allows **concurrent writes** to the same key on different nodes.
- Each key is versioned using a **Vector Clock** (e.g., `[NodeA: 1, NodeB: 1]`).
- On read, if `R` nodes return different vector clocks that cannot be causally merged (they are concurrent), the system returns *all* conflicting versions to the client.

### Hinted Handoff
The system is structured so that if a replica is unavailable, a fallback node could temporarily store the write and forward it once the original node recovers.

### Gossip Protocol
A gossip-based membership system replaces static node lists to allow dynamic cluster membership and decentralized failure detection.

## How to Run

1. Start 3 independent nodes on different ports. Each node will join the hash ring and begin gossiping to find its peers.

```bash
# Terminal 1
go run simulation/main.go
```
*Note: Currently, we demonstrate this using a simulation wrapper that boots all servers locally, but they use HTTP to communicate like real disparate nodes.*

To run individual servers on the host network, you could build `node.go`:
```bash
go run node.go --port 8001
go run node.go --port 8002
go run node.go --port 8003
```

## Running an API Test

Once the servers are online, you can use `curl` to interact with any node in the cluster. Because of the consistent hashring, any node can act as a coordinator.

**Write a value to the cluster:**
```bash
curl -X PUT http://localhost:8001/kv/foo \
    -H "Content-Type: application/json" \
    -d '{ "value": "bar" }'
```

**Read the value from the cluster:**
```bash
curl http://localhost:8002/kv/foo
```
*Notice we wrote to Node 8001 and read from Node 8002. The read coordinator will query the preference list and reconstruct the exact vector clock.*

## Failure Testing (Systems Thinking)

Distributed systems built by SREs are designed for failure. Mini Dynamo uses Hinted Handoff and Gossip to seamlessly recover from disasters.

**Example Scenario: Node goes down**
1. Assume Key `"foo"` maps to `Node 9002`, `Node 9003`, and `Node 9004`.
2. **Kill Node 9002**.
3. A client attempts to write `"foo"`. The coordinator checks Gossip, sees `Node 9002` is dead, and writes to `Node 9003`, `Node 9004`, and falls back to `Node 9000`.
4. The write **still succeeds** because quorum `W=2` is met.
5. `Node 9000` receives the write marked as a `Hint` intended for `Node 9002`.
6. You **revive Node 9002**.
7. Gossip protocol discovers `Node 9002` is `Alive`.
8. `Node 9000`'s Handoff Manager observes the state change and automatically replays the buffered hint to `Node 9002`.
9. The cluster reaches eventual consistency without data loss.

SRE / Systems Design Note: The store exposes tunable consistency through N, W, and R. When W + R > N the system guarantees strong consistency; when W + R ≤ N it favors availability with eventual consistency. This mirrors the tradeoffs made by production distributed systems where network partitions and node failures are expected conditions.

## Tradeoffs

- Latency vs durability: higher W improves durability but increases write latency
- Read repair not implemented; conflicting siblings are returned to clients
- Static membership (no automatic node discovery)
