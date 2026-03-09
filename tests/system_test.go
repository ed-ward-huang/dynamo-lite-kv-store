package tests

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/mini-dynamo/client"
	"github.com/mini-dynamo/gossip"
	"github.com/mini-dynamo/hashring"
	"github.com/mini-dynamo/node"
)

func setupCluster(t *testing.T, numNodes int, N, W, R int) ([]*node.Server, *client.Client, func()) {
	// Initialize hash ring
	ch := hashring.NewConsistentHash(10)

	var servers []*node.Server
	var endpoints []string

	basePort := 9000
	for i := 0; i < numNodes; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		endpoints = append(endpoints, addr)
		ch.AddNode(addr)
	}

	for i := 0; i < numNodes; i++ {
		id := fmt.Sprintf("Node%d", i)


		m := gossip.NewMembership(id, endpoints[i])
		g := gossip.NewGossiper(m)
		g.Start()
		
		s := node.NewServer(id, endpoints[i], ch, N, W, R)
		s.Replicate.SetMembership(m)
		
		servers = append(servers, s)
		
		go func(srv *node.Server) {
			if err := srv.Start(); err != nil && err != http.ErrServerClosed {
				t.Logf("Server %s stopped: %v", srv.ID, err)
			}
		}(s)
	}

	// Give servers to boot
	time.Sleep(200 * time.Millisecond)

	apiClient := client.NewClient(endpoints)

	cleanup := func() {
		for _, s := range servers {
			s.Stop()
		}

	}

	return servers, apiClient, cleanup
}

func TestSystem_Correctness_PutGet(t *testing.T) {
	_, apiClient, cleanup := setupCluster(t, 5, 3, 2, 2)
	defer cleanup()

	key := "test_key_1"
	val := "hello_world"

	err := apiClient.Put(key, val)
	if err != nil {
		t.Fatalf("Expected successful Put, got: %v", err)
	}

	results, err := apiClient.Get(key)
	if err != nil {
		t.Fatalf("Expected successful Get, got: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected exactly 1 version, got %d", len(results))
	}

	if results[0] != val {
		t.Fatalf("Expected '%s', got '%s'", val, results[0])
	}
}

func TestSystem_Reproducibility_ConcurrentWrites(t *testing.T) {
	servers, apiClient, cleanup := setupCluster(t, 5, 3, 2, 2)
	defer cleanup()

	key := "shared_counter"

	// Create two isolated clients hitting different nodes to simulate a partition boundary
	clientA := client.NewClient([]string{servers[0].Address})
	clientB := client.NewClient([]string{servers[1].Address})

	// Both write concurrently without seeing each other's vector clocks
	valA := "versionA"
	valB := "versionB"

	err1 := clientA.Put(key, valA)
	err2 := clientB.Put(key, valB)

	if err1 != nil || err2 != nil {
		t.Fatalf("Concurrent writes should succeed independently")
	}

	time.Sleep(100 * time.Millisecond) // buffer for replication

	// Now read from the general client
	results, err := apiClient.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected exactly 2 concurrent versions to be preserved, got %d", len(results))
	}

	foundA, foundB := false, false
	for _, r := range results {
		if r == valA { foundA = true }
		if r == valB { foundB = true }
	}

	if !foundA || !foundB {
		t.Fatalf("Expected versions '%s' and '%s' to be in results, got %v", valA, valB, results)
	}
}

func TestSystem_FailureHandling_NodeGoesDown(t *testing.T) {
	servers, apiClient, cleanup := setupCluster(t, 5, 3, 2, 2)
	defer cleanup()

	key := "resilience_key"
	val := "resilient_data"


	replicas := servers[0].Replicate.GetPreferenceList(key)
	

	targetAddr := replicas[0]
	for _, s := range servers {
		if s.Address == targetAddr {
			s.Stop()
			break
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Since W=2, and we have 2 remaining healthy replicas (out of 3), the write should still succeed.
	// The cluster will utilize Hinted Handoff logging locally.
	err := apiClient.Put(key, val)
	if err != nil {
		t.Fatalf("Expected Put to succeed despite 1 node failure (W=2), got: %v", err)
	}


	time.Sleep(50 * time.Millisecond)

	// Get should also succeed since R=2 and 2 nodes have it.
	results, err := apiClient.Get(key)
	if err != nil {
		t.Fatalf("Expected Get to succeed despite 1 node failure (R=2), got: %v", err)
	}

	if len(results) != 1 || results[0] != val {
		t.Fatalf("Expected to read '%s', got %v", val, results)
	}
}

func TestSystem_FailureHandling_QuorumFails(t *testing.T) {
	servers, apiClient, cleanup := setupCluster(t, 5, 3, 3, 3) // N=3, W=3, R=3 (Strict Quorum)
	defer cleanup()

	key := "strict_key"

	replicas := servers[0].Replicate.GetPreferenceList(key)
	
	// Kill one replica
	targetAddr := replicas[0]
	for _, s := range servers {
		if s.Address == targetAddr {
			s.Stop()
			break
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Since W=3 and only 2 replicas are alive, writing to this key should immediately fail.
	err := apiClient.Put(key, "data")
	if err == nil {
		t.Fatalf("Expected Put to fail because strict quorum (W=3) cannot be met")
	}
}

func TestSystem_HintedHandoff_AndRecovery(t *testing.T) {
	servers, apiClient, cleanup := setupCluster(t, 5, 3, 2, 2) // N=3, W=2, R=2
	defer cleanup()

	key := "handoff_key"
	val := "handoff_data"

	// Find the preference list for this key
	replicas := servers[0].Replicate.GetPreferenceList(key)
	
	// Kill exactly one replica node mapping to this key
	targetAddr := replicas[0]
	var deadServer *node.Server
	for _, s := range servers {
		if s.Address == targetAddr {
			deadServer = s
			s.Stop()
			break
		}
	}

	// Wait for Gossip to mark it as Dead (Suspect=5s, Dead=15s in config, but we can just wait 6s to trigger suspect/handoff proxying)
	// Actually, wait, the default membership timeout is 5s for suspect. Let's sleep 6s.
	t.Logf("Waiting 6s for Gossip to mark node %s as Suspect...", targetAddr)
	time.Sleep(6 * time.Second)

	// Since W=2, and we have 2 remaining healthy replicas (out of 3), the write should still succeed.
	// But crucially, a 3rd fallback node should accept the hint because N=3.
	err := apiClient.Put(key, val)
	if err != nil {
		t.Fatalf("Expected Put to succeed despite 1 node failure (W=2), got: %v", err)
	}

	// Verify the dead node doesn't have it (it's dead)
	
	// Now revive the dead node
	t.Logf("Reviving node %s...", targetAddr)
	go func(srv *node.Server) {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			t.Logf("Server %s stopped: %v", srv.ID, err)
		}
	}(deadServer)

	// Wait for gossip to see it alive (heartbeat merges), and then for the HandoffManager ticker (5s) to fire
	t.Logf("Waiting 8s for Gossip to discover it's alive and HandoffManager to forward the hint...")
	time.Sleep(8 * time.Second)

	// Now read DIRECTLY from the previously dead node to see if the background forwarder sent the hint!
	directClient := client.NewClient([]string{targetAddr})
	results, err := directClient.Get(key)
	if err != nil {
		t.Fatalf("Expected Get directly from revived node to succeed (hint forwarded!), got: %v", err)
	}

	if len(results) != 1 || results[0] != val {
		t.Fatalf("Expected to read '%s' from revived node, got %v", val, results)
	}
}
