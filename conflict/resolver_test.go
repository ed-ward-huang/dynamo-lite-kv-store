package conflict

import (
	"testing"
)

func TestResolver_Resolve(t *testing.T) {
	resolver := NewResolver()

	v1 := VersionedData{
		Value:       "data1",
		VectorClock: VectorClock{"A": 1},
		Timestamp:   100,
	}

	v2 := VersionedData{
		Value:       "data2",
		VectorClock: VectorClock{"A": 2}, // Supersedes v1
		Timestamp:   101,
	}

	v3 := VersionedData{
		Value:       "data3",
		VectorClock: VectorClock{"A": 1, "B": 1}, // Concurrent with v2
		Timestamp:   102,
	}

	// 1. Single version
	res := resolver.Resolve([]VersionedData{v1})
	if len(res) != 1 {
		t.Errorf("Expected 1 result for single version")
	}

	// 2. Linear history
	res = resolver.Resolve([]VersionedData{v1, v2})
	if len(res) != 1 || res[0].Value != "data2" {
		t.Errorf("Expected data2 to supersede data1")
	}

	// 3. Concurrent branches
	res = resolver.Resolve([]VersionedData{v2, v3})
	if len(res) != 2 {
		t.Errorf("Expected 2 results for concurrent versions, got %d", len(res))
	}

	// 4. Mixed (v1 superseded by v2, and v2 concurrent with v3)
	res = resolver.Resolve([]VersionedData{v1, v2, v3})
	if len(res) != 2 {
		t.Errorf("Expected 2 results for mixed versions, got %d", len(res))
	}

	foundV2, foundV3 := false, false
	for _, v := range res {
		if v.Value == "data2" {
			foundV2 = true
		}
		if v.Value == "data3" {
			foundV3 = true
		}
	}
	if !foundV2 || !foundV3 {
		t.Errorf("Expected to retain data2 and data3, got %v", res)
	}
}
