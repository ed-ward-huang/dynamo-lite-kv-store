package gossip

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGossiper_Merge(t *testing.T) {
	m1 := NewMembership("node1", "127.0.0.1:8001")
	
	incoming := map[string]NodeMetadata{
		"node2": {ID: "node2", Address: "127.0.0.1:8002", State: Alive, Heartbeat: 1},
	}
	
	m1.Merge(incoming)
	nodes := m1.GetAllNodes()
	
	if len(nodes) != 2 {
		t.Fatalf("Expected 2 nodes after merge, got %d", len(nodes))
	}
	
	if nodes["node2"].State != Alive || nodes["node2"].Address != "127.0.0.1:8002" {
		t.Errorf("Expected node2 to be correctly merged, got %v", nodes["node2"])
	}
}

func TestGossiper_HandleGossipHTTP(t *testing.T) {
	m := NewMembership("node1", "127.0.0.1:8001")
	g := NewGossiper(m)

	incomingState := map[string]NodeMetadata{
		"node3": {ID: "node3", Address: "127.0.0.1:8003", State: Suspect, Heartbeat: 5},
	}
	body, _ := json.Marshal(incomingState)

	req := httptest.NewRequest(http.MethodPost, "/gossip", strings.NewReader(string(body)))
	resp := httptest.NewRecorder()

	g.HandleGossipHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK, got %d", resp.Code)
	}

	nodes := m.GetAllNodes()
	if nodes["node3"].State != Suspect {
		t.Errorf("Expected node3 to be Suspect, got %v", nodes["node3"].State)
	}
}

