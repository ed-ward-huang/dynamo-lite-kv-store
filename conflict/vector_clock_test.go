package conflict

import "testing"

func TestVectorClock_Compare(t *testing.T) {
	vc1 := NewVectorClock()
	vc2 := NewVectorClock()

	// Empty clocks should be equal
	if vc1.Compare(vc2) != Equal {
		t.Errorf("Expected Equal for empty clocks")
	}

	// vc1 increments nodeA (vc1 > vc2)
	vc1.Increment("nodeA")
	if vc1.Compare(vc2) != After {
		t.Errorf("Expected vc1 to be After vc2")
	}
	if vc2.Compare(vc1) != Before {
		t.Errorf("Expected vc2 to be Before vc1")
	}

	// vc2 increments nodeA to catch up
	vc2.Increment("nodeA")
	if vc1.Compare(vc2) != Equal {
		t.Errorf("Expected Equal clocks")
	}

	// vc1 increments nodeB, vc2 increments nodeC (Concurrent)
	vc1.Increment("nodeB")
	vc2.Increment("nodeC")
	
	if vc1.Compare(vc2) != Concurrent {
		t.Errorf("Expected Concurrent clocks")
	}
}

func TestVectorClock_Merge(t *testing.T) {
	vc1 := VectorClock{"nodeA": 2, "nodeB": 1}
	vc2 := VectorClock{"nodeA": 1, "nodeB": 2, "nodeC": 1}

	merged := vc1.Merge(vc2)

	if merged["nodeA"] != 2 {
		t.Errorf("Merge expected nodeA=2, got %d", merged["nodeA"])
	}
	if merged["nodeB"] != 2 {
		t.Errorf("Merge expected nodeB=2, got %d", merged["nodeB"])
	}
	if merged["nodeC"] != 1 {
		t.Errorf("Merge expected nodeC=1, got %d", merged["nodeC"])
	}
}
