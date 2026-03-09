package quorum

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mini-dynamo/conflict"
)

type WriteCoordinator struct {
	N int
	W int
}

func NewWriteCoordinator(n, w int) *WriteCoordinator {
	return &WriteCoordinator{
		N: n,
		W: w,
	}
}

// NodeTarget is duplicated here to avoid a circular dependency with 'node'
// (Normally we'd pull this out to a shared 'types' package, but this works for mini-dynamo)
type NodeTarget struct {
	Address     string
	IsFallback  bool
	HandOffFor  string
}

func (wc *WriteCoordinator) Write(targets []NodeTarget, key string, data conflict.VersionedData) error {
	if len(targets) < wc.W {
		return fmt.Errorf("not enough nodes available to satisfy write quorum %d", len(targets))
	}

	var wg sync.WaitGroup
	
	acks := make(chan bool, len(targets))
	errs := make(chan error, len(targets))

	for _, target := range targets {
		wg.Add(1)
		go func(t NodeTarget) {
			defer wg.Done()
			
			var err error
			if t.IsFallback {
				err = sendHintRequest(t.Address, t.HandOffFor, key, data)
			} else {
				err = sendPutRequest(t.Address, key, data)
			}

			if err != nil {
				errs <- fmt.Errorf("node %s write failed (fallback=%v): %v", t.Address, t.IsFallback, err)
			} else {
				acks <- true
			}
		}(target)
	}

	timeout := time.After(3 * time.Second)

	successfulWrites := 0
	failedWrites := 0
	var lastErr error

	for {
		if successfulWrites >= wc.W {
			return nil
		}

		select {
		case <-acks:
			successfulWrites++
			if successfulWrites >= wc.W {
				return nil
			}
		case err := <-errs:
			failedWrites++
			log.Printf("Hint: write failed, triggering Hinted Handoff retry logic. Err: %v\n", err)
			lastErr = err
			
			if len(targets) - failedWrites < wc.W {
				return fmt.Errorf("write quorum failed: collected %d/%d acks, %d failures. Last err: %v", 
					successfulWrites, wc.W, failedWrites, lastErr)
			}
			
		case <-timeout:
			return fmt.Errorf("write timeout: collected %d/%d acks. Last error: %v", 
				successfulWrites, wc.W, lastErr)
		}
	}
}
