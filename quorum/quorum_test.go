package quorum

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mini-dynamo/conflict"
)

func setupTestNode(t *testing.T, data string, succeed bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !succeed {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
			return
		}

		if r.Method == http.MethodGet {
			vc := conflict.NewVectorClock()
			vc.Increment("nodeA")
			
			body := []conflict.VersionedData{
				{
					Value:       data,
					VectorClock: vc,
					Timestamp:   time.Now().UnixNano(),
				},
			}
			
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(body)
			return
		}
	}))
}

func TestWriteCoordinator(t *testing.T) {
	node1 := setupTestNode(t, "", true)
	defer node1.Close()
	node2 := setupTestNode(t, "", true)
	defer node2.Close()
	nodeFail := setupTestNode(t, "", false)
	defer nodeFail.Close()

	wc := NewWriteCoordinator(3, 2) // N=3, W=2

	// Strip "http://" for the client helper functions which append it
	nodes := []NodeTarget{
		{Address: node1.URL[7:]}, 
		{Address: node2.URL[7:]}, 
		{Address: nodeFail.URL[7:]},
	}

	data := conflict.VersionedData{Value: "test"}

	err := wc.Write(nodes, "key1", data)
	if err != nil {
		t.Errorf("Expected write to succeed with W=2 (2 nodes succeeded), got err: %v", err)
	}

	// Test Failed Quorum
	wcFail := NewWriteCoordinator(3, 3) // Strict quorum required
	errFail := wcFail.Write(nodes, "strict_key", data)
	if errFail == nil {
		t.Errorf("Expected write to fail with W=3 (1 node failed)")
	}
}

func TestReadCoordinator(t *testing.T) {
	node1 := setupTestNode(t, "data1", true)
	defer node1.Close()
	node2 := setupTestNode(t, "data1", true) // Returns identical vector clock
	defer node2.Close()
	nodeFail := setupTestNode(t, "data_fail", false)
	defer nodeFail.Close()

	rc := NewReadCoordinator(3, 2) // N=3, R=2
	
	nodes := []string{
		node1.URL[7:],
		node2.URL[7:],
		nodeFail.URL[7:],
	}

	res, err := rc.Read(nodes, "key1")
	if err != nil {
		t.Fatalf("Expected read to succeed, got %v", err)
	}

	if len(res) != 1 {
		t.Fatalf("Expected conflict resolver to deduplicate identical versions, got %d", len(res))
	}
	if res[0].Value != "data1" {
		t.Errorf("Expected 'data1', got %s", res[0].Value)
	}
}
