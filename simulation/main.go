package main

import (
	"fmt"
	"log"
	"time"

	"github.com/mini-dynamo/client"
	"github.com/mini-dynamo/gossip"
	"github.com/mini-dynamo/hashring"
	"github.com/mini-dynamo/node"
)

func main() {
	log.Println("Starting Mini Dynamo-Lite Simulation...")

	const R = 2
	const W = 2
	const N = 3 // Replication factor

	nodes := []struct {
		id   string
		addr string
	}{
		{"NodeA", "127.0.0.1:8001"},
		{"NodeB", "127.0.0.1:8002"},
		{"NodeC", "127.0.0.1:8003"},
		{"NodeD", "127.0.0.1:8004"},
		{"NodeE", "127.0.0.1:8005"},
	}

	// 1. Initialize hash ring and cluster topology
	ch := hashring.NewConsistentHash(10) // 10 vnodes each
	for _, n := range nodes {
		ch.AddNode(n.addr)
	}

	// 2. Start servers and gossipers
	endpoints := make([]string, len(nodes))
	for i, n := range nodes {
		endpoints[i] = n.addr

		// Setup Gossip
		m := gossip.NewMembership(n.id, n.addr)
		g := gossip.NewGossiper(m)
		g.Start()

		// Setup Node Server
		server := node.NewServer(n.id, n.addr, ch, N, W, R)
		server.Replicate.SetMembership(m) // INJECT MEMBERSHIP
		
		go func(addr string, s *node.Server) {
			if err := s.Start(); err != nil {
				log.Fatalf("Server %s failed: %v", addr, err)
			}
		}(n.addr, server)
	}

	// Give servers a moment to start up
	time.Sleep(1 * time.Second)
	log.Println("Cluster is up and running.")

	// 3. Initialize API client
	apiClient := client.NewClient(endpoints)

	// --- TEST SCENARIO 1: Normal Distributed Write & Read ---
	log.Println("\n--- Scenario 1: Normal Quorum Write/Read ---")
	err := apiClient.Put("user123_profile", `{"name": "Alice", "age": 30}`)
	if err != nil {
		log.Fatalf("Failed to put: %v", err)
	}

	val, err := apiClient.Get("user123_profile")
	if err != nil {
		log.Fatalf("Failed to get: %v", err)
	}
	fmt.Printf("Get Result: %v\n", val)


	// --- TEST SCENARIO 2: Concurrent Writers (Conflict Generation) ---
	log.Println("\n--- Scenario 2: Concurrent Writes & Conflict Generation ---")
	
	// We manually send requests to two DIFFERENT coordinators directly to simulate 
	// split-brain partition writes where they increment different vector clocks concurrently
	clientB := client.NewClient([]string{"127.0.0.1:8002"})
	clientC := client.NewClient([]string{"127.0.0.1:8003"})

	// They write to the same key simultaneously
	clientB.Put("shared_key", "Version B from Client 1")
	clientC.Put("shared_key", "Version C from Client 2")

	time.Sleep(1 * time.Second) // allow async replication propagation

	// Now we read, and the resolver logic should detect concurrent versions and return BOTH.
	conflictVals, _ := apiClient.Get("shared_key")
	fmt.Printf("Resolved Concurrent Get Results:\n")
	for i, v := range conflictVals {
		fmt.Printf(" Version %d: %v\n", i+1, v)
	}
	
	// --- TEST SCENARIO 3: Node Failure & Hinted Handoff ---
	// NOTE: Because we are running all endpoints in the same process loop here via goroutines,
	// hard killing a single node requires structural hooks we omitted. But in an actual
	// process-based test, killing it would demonstrate Hinted Handoff logic firing in the 
	// Writer Coordinator. The code we wrote for hinted handoff logging will emit in terminal
	// if an endpoint goes down but W is met.

	log.Println("\nSimulation completed successfully.")
}
