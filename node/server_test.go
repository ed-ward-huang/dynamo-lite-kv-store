package node

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mini-dynamo/conflict"
	"github.com/mini-dynamo/hashring"
)

func TestServer_HandleKV(t *testing.T) {
	ring := hashring.NewConsistentHash(3)
	ring.AddNode("127.0.0.1:8001")
	srv := NewServer("node1", "127.0.0.1:8001", ring, 1, 1, 1)


	putReq := httptest.NewRequest(http.MethodPut, "/kv/test_key?direct=true", strings.NewReader(`"test_value"`))
	putReq.Header.Set("Content-Type", "application/json")
	putResp := httptest.NewRecorder()
	
	srv.handleKV(putResp, putReq)
	if putResp.Code != http.StatusCreated {
		t.Fatalf("Expected status 201 Created for PUT, got %d", putResp.Code)
	}


	getReq := httptest.NewRequest(http.MethodGet, "/kv/test_key?direct=true", nil)
	getResp := httptest.NewRecorder()

	srv.handleKV(getResp, getReq)
	if getResp.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK for GET, got %d", getResp.Code)
	}

	var data []conflict.VersionedData
	if err := json.Unmarshal(getResp.Body.Bytes(), &data); err != nil {
		t.Fatalf("Failed to parse response body: %v", err)
	}

	if len(data) != 1 || data[0].Value != `"test_value"` {
		t.Errorf("Expected value '\"test_value\"', got %v", data)
	}


	getReq2 := httptest.NewRequest(http.MethodGet, "/kv/missing_key?direct=true", nil)
	getResp2 := httptest.NewRecorder()
	srv.handleKV(getResp2, getReq2)
	if getResp2.Code != http.StatusNotFound {
		t.Fatalf("Expected 404 Not Found for missing key, got %d", getResp2.Code)
	}
}

func TestServer_HandleHandoff(t *testing.T) {
	ring := hashring.NewConsistentHash(3)
	srv := NewServer("node1", "127.0.0.1:8001", ring, 1, 1, 1)

	hint := HandoffHint{
		TargetNode: "127.0.0.1:8002",
		Key:        "handoff_key",
		Data: conflict.VersionedData{
			Value: "handoff_data",
		},
	}

	body, _ := json.Marshal(hint)
	req := httptest.NewRequest(http.MethodPost, "/kv/handoff", bytes.NewReader(body))
	resp := httptest.NewRecorder()

	srv.handleHandoff(resp, req)
	if resp.Code != http.StatusCreated {
		t.Fatalf("Expected 201 Created for handoff, got %d", resp.Code)
	}


	versions, err := srv.Hints.Get("127.0.0.1:8002:::handoff_key")
	if err != nil || len(versions) == 0 {
		t.Fatalf("Expected hint to be stored correctly")
	}
	if versions[0].Value != "handoff_data" {
		t.Errorf("Expected value 'handoff_data', got %s", versions[0].Value)
	}
}
