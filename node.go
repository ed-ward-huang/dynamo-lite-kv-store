package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/mini-dynamo/gossip"
	"github.com/mini-dynamo/hashring"
	"github.com/mini-dynamo/node"
)

func main() {
	port := flag.Int("port", 8001, "Port to run the node on")
	flag.Parse()


	const R = 2
	const W = 2
	const N = 3 // Replication factor


	clusterNodes := []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}


	ch := hashring.NewConsistentHash(10)
	for _, addr := range clusterNodes {
		ch.AddNode(addr)
	}

	myAddr := fmt.Sprintf("127.0.0.1:%d", *port)
	id := fmt.Sprintf("Node-%d", *port)


	m := gossip.NewMembership(id, myAddr)
	go gossip.NewGossiper(m).Start()


	server := node.NewServer(id, myAddr, ch, N, W, R)
	server.Replicate.SetMembership(m)
	
	log.Printf("Starting %s on %s (N=%d, W=%d, R=%d)\n", id, myAddr, N, W, R)
	if err := server.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
