package gossip

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// NodeState indicates the health of a node
type NodeState int

const (
	Alive NodeState = iota
	Suspect
	Dead
)

// NodeMetadata holds information about a specific node's state
type NodeMetadata struct {
	ID        string
	Address   string
	State     NodeState
	Heartbeat int64
	LastSeen  time.Time
}

// Membership maintains the cluster's view of the world
type Membership struct {
	mu           sync.RWMutex
	LocalID      string
	LocalAddress string
	Nodes        map[string]*NodeMetadata

	// Configuration
	SuspectTimeout time.Duration
	DeadTimeout    time.Duration
}

// NewMembership creates a new membership tracker
func NewMembership(id, address string) *Membership {
	m := &Membership{
		LocalID:        id,
		LocalAddress:   address,
		Nodes:          make(map[string]*NodeMetadata),
		SuspectTimeout: 5 * time.Second,
		DeadTimeout:    15 * time.Second,
	}


	m.Nodes[id] = &NodeMetadata{
		ID:        id,
		Address:   address,
		State:     Alive,
		Heartbeat: 0,
		LastSeen:  time.Now(),
	}

	return m
}

// IncrementHeartbeat bumps the local node's heartbeat
func (m *Membership) IncrementHeartbeat() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if me, ok := m.Nodes[m.LocalID]; ok {
		me.Heartbeat++
		me.LastSeen = time.Now()
	}
}

// GetAllNodes returns a snapshot of the membership list
func (m *Membership) GetAllNodes() map[string]NodeMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := make(map[string]NodeMetadata)
	for id, meta := range m.Nodes {
		snapshot[id] = *meta // value copy
	}
	return snapshot
}

// GetRandomPeer selects a random alive peer to gossip with
func (m *Membership) GetRandomPeer() (NodeMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var peers []NodeMetadata
	for id, meta := range m.Nodes {
		if id != m.LocalID && meta.State == Alive {
			peers = append(peers, *meta)
		}
	}

	if len(peers) == 0 {
		return NodeMetadata{}, fmt.Errorf("no alive peers available")
	}

	return peers[rand.Intn(len(peers))], nil
}

// Merge updating the local membership view based on another node's state
func (m *Membership) Merge(incoming map[string]NodeMetadata) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, incomingMeta := range incoming {
		if id == m.LocalID {
			continue // Don't let others overwrite our own heartbeat
		}

		localMeta, exists := m.Nodes[id]
		if !exists {

			m.Nodes[id] = &NodeMetadata{
				ID:        incomingMeta.ID,
				Address:   incomingMeta.Address,
				State:     incomingMeta.State,
				Heartbeat: incomingMeta.Heartbeat,
				LastSeen:  time.Now(),
			}
			continue
		}

		// Only accept strictly newer heartbeats
		if incomingMeta.Heartbeat > localMeta.Heartbeat {
			localMeta.Heartbeat = incomingMeta.Heartbeat
			localMeta.State = Alive
			localMeta.LastSeen = time.Now()
		}
	}
}

// CheckFailures iterates over nodes and flags them as Suspect/Dead based on timeouts
func (m *Membership) CheckFailures() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	for id, meta := range m.Nodes {
		if id == m.LocalID {
			continue
		}

		elapsed := now.Sub(meta.LastSeen)

		if meta.State == Alive && elapsed > m.SuspectTimeout {
			meta.State = Suspect
		} else if meta.State == Suspect && elapsed > m.DeadTimeout {
			meta.State = Dead
		}
	}
}
