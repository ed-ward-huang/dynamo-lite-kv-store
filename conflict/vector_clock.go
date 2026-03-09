package conflict

import "fmt"

type Order int

const (
	Equal Order = iota
	Before
	After
	Concurrent
)

type VectorClock map[string]int

func NewVectorClock() VectorClock {
	return make(VectorClock)
}

func (vc VectorClock) Clone() VectorClock {
	clone := make(VectorClock)
	for k, v := range vc {
		clone[k] = v
	}
	return clone
}

func (vc VectorClock) Increment(node string) {
	vc[node]++
}

func (vc VectorClock) Compare(other VectorClock) Order {
	isBefore := false
	isAfter := false

	for node, v1 := range vc {
		v2 := other[node]
		if v1 < v2 {
			isBefore = true
		} else if v1 > v2 {
			isAfter = true
		}
	}

	for node, v2 := range other {
		if _, exists := vc[node]; !exists && v2 > 0 {
			isBefore = true
		}
	}

	if isBefore && isAfter {
		return Concurrent
	} else if isBefore {
		return Before
	} else if isAfter {
		return After
	}

	return Equal
}

func (vc VectorClock) Merge(other VectorClock) VectorClock {
	merged := NewVectorClock()
	
	for k, v := range vc {
		merged[k] = v
	}

	for k, v := range other {
		if merged[k] < v {
			merged[k] = v
		}
	}

	return merged
}

func (vc VectorClock) String() string {
	return fmt.Sprintf("%v", map[string]int(vc))
}
