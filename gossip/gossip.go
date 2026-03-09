package gossip

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type Gossiper struct {
	Membership   *Membership
	Interval     time.Duration
	client       *http.Client
	stopChannels chan struct{}
}

func NewGossiper(m *Membership) *Gossiper {
	return &Gossiper{
		Membership:   m,
		Interval:     1 * time.Second,
		client:       &http.Client{Timeout: 500 * time.Millisecond},
		stopChannels: make(chan struct{}),
	}
}

func (g *Gossiper) Start() {
	go g.gossipLoop()
	go g.failureDetectionLoop()
}

func (g *Gossiper) Stop() {
	close(g.stopChannels)
}

func (g *Gossiper) gossipLoop() {
	ticker := time.NewTicker(g.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			g.Membership.IncrementHeartbeat()

			peer, err := g.Membership.GetRandomPeer()
			if err != nil {
				continue
			}

			g.sendGossip(peer.Address)

		case <-g.stopChannels:
			return
		}
	}
}

func (g *Gossiper) sendGossip(address string) {
	state := g.Membership.GetAllNodes()
	
	body, err := json.Marshal(state)
	if err != nil {
		log.Printf("Gossip encode error: %v\n", err)
		return
	}

	url := "http://" + address + "/gossip"
	resp, err := g.client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func (g *Gossiper) HandleGossipHTTP(w http.ResponseWriter, r *http.Request) {
	var incomingState map[string]NodeMetadata
	if err := json.NewDecoder(r.Body).Decode(&incomingState); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	g.Membership.Merge(incomingState)
	w.WriteHeader(http.StatusOK)
}

func (g *Gossiper) failureDetectionLoop() {
	ticker := time.NewTicker(g.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			g.Membership.CheckFailures()
		case <-g.stopChannels:
			return
		}
	}
}
