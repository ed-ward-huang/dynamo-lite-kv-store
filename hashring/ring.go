package hashring

import (
	"crypto/sha1"
	"fmt"
	"sort"
	"sync"
)

type ConsistentHash struct {
	mu           sync.RWMutex
	vnodes       int               
	keys         []uint32          
	hashMap      map[uint32]string 
	physicalNodes map[string]bool   
}

func NewConsistentHash(vnodes int) *ConsistentHash {
	return &ConsistentHash{
		vnodes:        vnodes,
		keys:          make([]uint32, 0),
		hashMap:       make(map[uint32]string),
		physicalNodes: make(map[string]bool),
	}
}

func (c *ConsistentHash) hash(data []byte) uint32 {
	hash := sha1.Sum(data)
	return uint32(hash[3]) | uint32(hash[2])<<8 | uint32(hash[1])<<16 | uint32(hash[0])<<24
}

func (c *ConsistentHash) AddNode(node string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.physicalNodes[node] {
		return
	}

	c.physicalNodes[node] = true

	for i := 0; i < c.vnodes; i++ {
		vnodeKey := fmt.Sprintf("%s-%d", node, i)
		hash := c.hash([]byte(vnodeKey))

		c.keys = append(c.keys, hash)
		c.hashMap[hash] = node
	}

	sort.Slice(c.keys, func(i, j int) bool {
		return c.keys[i] < c.keys[j]
	})
}

func (c *ConsistentHash) RemoveNode(node string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.physicalNodes[node] {
		return
	}

	delete(c.physicalNodes, node)

	newKeys := make([]uint32, 0)
	
	for i := 0; i < c.vnodes; i++ {
		vnodeKey := fmt.Sprintf("%s-%d", node, i)
		hash := c.hash([]byte(vnodeKey))
		delete(c.hashMap, hash)
	}

	for _, k := range c.keys {
		if c.hashMap[k] != "" {
			newKeys = append(newKeys, k)
		}
	}

	c.keys = newKeys
}

func (c *ConsistentHash) GetNode(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.keys) == 0 {
		return ""
	}

	hash := c.hash([]byte(key))

	idx := sort.Search(len(c.keys), func(i int) bool {
		return c.keys[i] >= hash
	})

	if idx == len(c.keys) {
		idx = 0
	}

	return c.hashMap[c.keys[idx]]
}

func (c *ConsistentHash) GetReplicas(key string, n int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.keys) == 0 {
		return nil
	}
	
	if n > len(c.physicalNodes) {
		n = len(c.physicalNodes)
	}

	hash := c.hash([]byte(key))
	idx := sort.Search(len(c.keys), func(i int) bool {
		return c.keys[i] >= hash
	})

	if idx == len(c.keys) {
		idx = 0
	}

	replicas := make([]string, 0, n)
	seenNodes := make(map[string]bool)

	for i := 0; i < len(c.keys); i++ {
		currIdx := (idx + i) % len(c.keys)
		node := c.hashMap[c.keys[currIdx]]

		if !seenNodes[node] {
			seenNodes[node] = true
			replicas = append(replicas, node)
			if len(replicas) == n {
				break
			}
		}
	}

	return replicas
}
