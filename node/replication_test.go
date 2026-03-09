package node

import (
	"testing"

	"github.com/mini-dynamo/gossip"
	"github.com/mini-dynamo/hashring"
)

func TestReplicationManager_GetHealthyPreferenceList(t *testing.T) {
	ring := hashring.NewConsistentHash(3)
	ring.AddNode("127.0.0.1:8001")
	ring.AddNode("127.0.0.1:8002")
	ring.AddNode("127.0.0.1:8003")
	ring.AddNode("127.0.0.1:8004")
	ring.AddNode("127.0.0.1:8005")

	rm := NewReplicationManager(ring, 3)
	
	targets := rm.GetHealthyPreferenceList("test_key")
	if len(targets) != 3 {
		t.Fatalf("Expected 3 targets without membership, got %d", len(targets))
	}

	mem := gossip.NewMembership("node1", "127.0.0.1:8001")
	mem.Merge(map[string]gossip.NodeMetadata{
		"node1": {ID: "node1", Address: "127.0.0.1:8001", State: gossip.Alive},
		"node2": {ID: "node2", Address: "127.0.0.1:8002", State: gossip.Alive},
		"node3": {ID: "node3", Address: "127.0.0.1:8003", State: gossip.Alive},
		"node4": {ID: "node4", Address: "127.0.0.1:8004", State: gossip.Alive},
		"node5": {ID: "node5", Address: "127.0.0.1:8005", State: gossip.Alive},
	})
	rm.SetMembership(mem)

	targets = rm.GetHealthyPreferenceList("test_key")
	if len(targets) != 3 {
		t.Fatalf("Expected 3 targets with healthy membership, got %d", len(targets))
	}

	for _, target := range targets {
		if target.IsFallback {
			t.Errorf("Expected no fallbacks, got fallback %s", target.Address)
		}
	}


	top3 := ring.GetReplicas("test_key", 3)
	deadAddr := top3[0]


	var deadID string
	for id, meta := range mem.GetAllNodes() {
		if meta.Address == deadAddr {
			deadID = id
			break
		}
	}


	mem.Nodes[deadID].State = gossip.Dead

	targets = rm.GetHealthyPreferenceList("test_key")
	if len(targets) != 3 {
		t.Fatalf("Expected 3 targets even with ONE dead, got %d", len(targets))
	}


	var hasFallback bool
	for _, target := range targets {
		if target.IsFallback {
			hasFallback = true
			if target.HandOffFor != deadAddr {
				t.Errorf("Expected HandOffFor to be %s, got %s", deadAddr, target.HandOffFor)
			}
		}
		if target.Address == deadAddr {
			t.Errorf("Expected dead address %s to not be in the targets list", deadAddr)
		}
	}

	if !hasFallback {
		t.Fatalf("Expected a fallback node to be included")
	}
}
