package gossip

import (
	"testing"
	"time"
)

func TestMembership_Merge(t *testing.T) {
	m := NewMembership("node1", "localhost:8001")
	m.IncrementHeartbeat()

	// Ensure local state represents node1 correctly
	state := m.GetAllNodes()
	if len(state) != 1 {
		t.Fatalf("Expected 1 node, got %d", len(state))
	}

	incomingState := map[string]NodeMetadata{
		"node2": {
			ID:        "node2",
			Address:   "localhost:8002",
			State:     Alive,
			Heartbeat: 5,
		},
	}

	m.Merge(incomingState)

	newState := m.GetAllNodes()
	if len(newState) != 2 {
		t.Fatalf("Expected merge to result in 2 nodes, got %d", len(newState))
	}

	if node2, ok := newState["node2"]; !ok || node2.Heartbeat != 5 {
		t.Errorf("Merged state incorrect for node2")
	}
}

func TestMembership_CheckFailures(t *testing.T) {
	m := NewMembership("node1", "localhost:8001")
	m.SuspectTimeout = 100 * time.Millisecond
	m.DeadTimeout = 200 * time.Millisecond

	// Add a node
	m.Nodes["node2"] = &NodeMetadata{
		ID:        "node2",
		Address:   "localhost:8002",
		State:     Alive,
		Heartbeat: 1,
		LastSeen:  time.Now().Add(-150 * time.Millisecond), // Set past SuspectTimeout but before Dead
	}

	m.CheckFailures()

	if m.Nodes["node2"].State != Suspect {
		t.Errorf("Expected node2 to be Suspect due to timeout, got %v", m.Nodes["node2"].State)
	}

	// Wait to push it to dead territory
	time.Sleep(100 * time.Millisecond)
	m.CheckFailures()

	if m.Nodes["node2"].State != Dead {
		t.Errorf("Expected node2 to be Dead, got %v", m.Nodes["node2"].State)
	}
}
