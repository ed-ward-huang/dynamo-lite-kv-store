package node

import (
	"log"

	"github.com/mini-dynamo/gossip"
	"github.com/mini-dynamo/hashring"
)

// ReplicationManager handles identifying which nodes should store a key
type ReplicationManager struct {
	Ring              *hashring.ConsistentHash
	ReplicationFactor int
	Membership        *gossip.Membership
}

// NewReplicationManager initializes a ReplicationManager
func NewReplicationManager(ch *hashring.ConsistentHash, n int) *ReplicationManager {
	return &ReplicationManager{
		Ring:              ch,
		ReplicationFactor: n,
	}
}

// SetMembership injects the gossip membership state later if available
func (rm *ReplicationManager) SetMembership(m *gossip.Membership) {
	rm.Membership = m
}

// GetPreferenceList returns the N virtual nodes (from distinct physical nodes) responsible for a key
func (rm *ReplicationManager) GetPreferenceList(key string) []string {
	replicas := rm.Ring.GetReplicas(key, rm.ReplicationFactor)
	log.Printf("Preference list for key '%s': %v\n", key, replicas)
	return replicas
}

// NodeTarget defines an intended destination for a key
type NodeTarget struct {
	Address     string
	IsFallback  bool   // True if this node wasn't in the original top N
	HandOffFor  string // If IsFallback is true, this is the original offline node it's holding data for
}

// GetHealthyPreferenceList returns the top N nodes, but if any normally required nodes are dead,
// it continues walking the ring to find an acceptable fallback, tagging them as handoffs.
func (rm *ReplicationManager) GetHealthyPreferenceList(key string) []NodeTarget {
	if rm.Membership == nil {
		// Fallback to static if no gossip
		basic := rm.GetPreferenceList(key)
		targets := make([]NodeTarget, len(basic))
		for i, addr := range basic {
			targets[i] = NodeTarget{Address: addr}
		}
		return targets
	}

	// Because we want to find fallbacks, we might need more than N nodes from the ring.
	// We'll ask the hash ring for up to 3*N to be safe.
	ringNodes := rm.Ring.GetReplicas(key, rm.ReplicationFactor*3)
	if len(ringNodes) == 0 {
		return nil
	}
	
	states := rm.Membership.GetAllNodes()
	var targets []NodeTarget

	// Determine the "True Top N"
	var trueTopN []string
	for i := 0; i < rm.ReplicationFactor && i < len(ringNodes); i++ {
		trueTopN = append(trueTopN, ringNodes[i])
	}

	var offlineTopNNodes []string

	// Pass 1: Add all strictly healthy nodes from the true top N
	for _, addr := range trueTopN {
		isAlive := true
		for _, nodeMeta := range states {
			if nodeMeta.Address == addr {
				if nodeMeta.State == gossip.Dead || nodeMeta.State == gossip.Suspect {
					isAlive = false
				}
				break
			}
		}

		if isAlive {
			targets = append(targets, NodeTarget{Address: addr})
		} else {
			offlineTopNNodes = append(offlineTopNNodes, addr)
		}
	}

	// Pass 2: If we didn't get N nodes, start collecting fallbacks further down the ring
	if len(targets) < rm.ReplicationFactor {
		for i := rm.ReplicationFactor; i < len(ringNodes); i++ {
			if len(targets) >= rm.ReplicationFactor {
				break
			}

			addr := ringNodes[i]
			isAlive := true
			for _, nodeMeta := range states {
				if nodeMeta.Address == addr {
					if nodeMeta.State == gossip.Dead || nodeMeta.State == gossip.Suspect {
						isAlive = false
					}
					break
				}
			}

			if isAlive {
				// Pick the next offline node we want this fallback to hold data for
				var handOffFor string
				if len(offlineTopNNodes) > 0 {
					handOffFor = offlineTopNNodes[0]
					// Remove the one we just assigned
					offlineTopNNodes = offlineTopNNodes[1:] 
				}

				targets = append(targets, NodeTarget{
					Address:    addr,
					IsFallback: true,
					HandOffFor: handOffFor,
				})
			}
		}
	}

	return targets
}
