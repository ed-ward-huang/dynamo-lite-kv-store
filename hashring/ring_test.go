package hashring

import (
	"testing"
)

func TestConsistentHash_AddNode(t *testing.T) {
	ch := NewConsistentHash(3) // 3 vnodes

	ch.AddNode("nodeA")
	if len(ch.keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(ch.keys))
	}
	if len(ch.hashMap) != 3 {
		t.Errorf("expected 3 entries in hashMap, got %d", len(ch.hashMap))
	}

	ch.AddNode("nodeB")
	if len(ch.keys) != 6 {
		t.Errorf("expected 6 keys, got %d", len(ch.keys))
	}
}

func TestConsistentHash_RemoveNode(t *testing.T) {
	ch := NewConsistentHash(3)
	ch.AddNode("nodeA")
	ch.AddNode("nodeB")
	
	ch.RemoveNode("nodeA")
	if len(ch.keys) != 3 {
		t.Errorf("expected 3 keys after removal, got %d", len(ch.keys))
	}

	// Verify only nodeB hashes remain
	for _, k := range ch.keys {
		if ch.hashMap[k] != "nodeB" {
			t.Errorf("expected nodeB, got %s", ch.hashMap[k])
		}
	}
}

func TestConsistentHash_GetNode(t *testing.T) {
	ch := NewConsistentHash(100) // Many vnodes for better distribution
	nodes := []string{"node1", "node2", "node3"}
	
	for _, n := range nodes {
		ch.AddNode(n)
	}

	// Just a simple check that it returns something valid
	node := ch.GetNode("my_test_key")
	valid := false
	for _, n := range nodes {
		if node == n {
			valid = true
			break
		}
	}
	if !valid {
		t.Errorf("returned node %s is not in cluster", node)
	}
}

func TestConsistentHash_GetReplicas(t *testing.T) {
	ch := NewConsistentHash(100)
	ch.AddNode("nodeA")
	ch.AddNode("nodeB")
	ch.AddNode("nodeC")
	ch.AddNode("nodeD")
	ch.AddNode("nodeE")

	replicas := ch.GetReplicas("foo_key", 3)
	if len(replicas) != 3 {
		t.Errorf("expected 3 replicas, got %d", len(replicas))
	}

	// Ensure all returned replicas are distinct physical nodes
	seen := make(map[string]bool)
	for _, r := range replicas {
		if seen[r] {
			t.Errorf("duplicate replica node %s returned", r)
		}
		seen[r] = true
	}

	// Edge case: Requesting more replicas than nodes
	replicasTooMany := ch.GetReplicas("foo_key", 10)
	if len(replicasTooMany) != 5 {
		t.Errorf("expected 5 replicas (max nodes), got %d", len(replicasTooMany))
	}
}
