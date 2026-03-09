package node

import (
	"testing"
	"time"

	"github.com/mini-dynamo/conflict"
)

func TestInMemoryStorage_PutGet(t *testing.T) {
	storage := NewInMemoryStorage()

	vc := conflict.NewVectorClock()
	vc.Increment("nodeA")

	data := conflict.VersionedData{
		Value:       "test_value",
		VectorClock: vc,
		Timestamp:   time.Now().UnixNano(),
	}

	err := storage.Put("my_key", data)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	retrieved, err := storage.Get("my_key")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if len(retrieved) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(retrieved))
	}

	if retrieved[0].Value != "test_value" {
		t.Errorf("Expected 'test_value', got %s", retrieved[0].Value)
	}
}

func TestInMemoryStorage_ResolveOnPut(t *testing.T) {
	storage := NewInMemoryStorage()

	// Version 1 from node A
	vc1 := conflict.NewVectorClock()
	vc1.Increment("nodeA")
	v1 := conflict.VersionedData{Value: "v1", VectorClock: vc1, Timestamp: 100}
	
	// Version 2 from node A (supersedes v1)
	vc2 := vc1.Clone()
	vc2.Increment("nodeA")
	v2 := conflict.VersionedData{Value: "v2", VectorClock: vc2, Timestamp: 101}

	// Version 3 from node B (concurrent to v2)
	vc3 := vc1.Clone()
	vc3.Increment("nodeB")
	v3 := conflict.VersionedData{Value: "v3", VectorClock: vc3, Timestamp: 102}

	storage.Put("key", v1)
	storage.Put("key", v2) // Should overwrite v1
	
	res, _ := storage.Get("key")
	if len(res) != 1 || res[0].Value != "v2" {
		t.Fatalf("Expected v2 to supersede v1, got %v", res)
	}

	storage.Put("key", v3) // Should result in concurrent state with v2
	res, _ = storage.Get("key")
	if len(res) != 2 {
		t.Fatalf("Expected 2 concurrent versions, got %v", len(res))
	}
}

func TestInMemoryStorage_Delete(t *testing.T) {
	storage := NewInMemoryStorage()
	storage.Put("test", conflict.VersionedData{Value: "val"})
	
	err := storage.Delete("test")
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	_, err = storage.Get("test")
	if err == nil {
		t.Errorf("Expected error getting deleted key")
	}
}
