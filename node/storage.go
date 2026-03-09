package node

import (
	"fmt"
	"sync"
	"time"

	"github.com/mini-dynamo/conflict"
)

type StorageEngine interface {
	Put(key string, data conflict.VersionedData) error
	Get(key string) ([]conflict.VersionedData, error)
	Delete(key string) error
}

type InMemoryStorage struct {
	mu   sync.RWMutex
	data map[string][]conflict.VersionedData
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		data: make(map[string][]conflict.VersionedData),
	}
}

func (s *InMemoryStorage) Put(key string, newData conflict.VersionedData) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if newData.Timestamp == 0 {
		newData.Timestamp = time.Now().UnixNano()
	}

	existing, ok := s.data[key]
	if !ok {
		s.data[key] = []conflict.VersionedData{newData}
		return nil
	}

	existing = append(existing, newData)
	
	resolver := conflict.NewResolver()
	resolved := resolver.Resolve(existing)
	
	s.data[key] = resolved

	return nil
}

func (s *InMemoryStorage) Get(key string) ([]conflict.VersionedData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	copied := make([]conflict.VersionedData, len(versions))
	copy(copied, versions)

	return copied, nil
}

func (s *InMemoryStorage) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
	return nil
}

func (s *InMemoryStorage) GetAllKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}
