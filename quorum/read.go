package quorum

import (
	"fmt"
	"sync"
	"time"

	"github.com/mini-dynamo/conflict"
)

type ReadCoordinator struct {
	N int
	R int
}

func NewReadCoordinator(n, r int) *ReadCoordinator {
	return &ReadCoordinator{
		N: n,
		R: r,
	}
}

func (rc *ReadCoordinator) Read(nodes []string, key string) ([]conflict.VersionedData, error) {
	if len(nodes) < rc.R {
		return nil, fmt.Errorf("not enough nodes available to satisfy read quorum")
	}

	var wg sync.WaitGroup
	
	resultsChan := make(chan []conflict.VersionedData, len(nodes))
	errsChan := make(chan error, len(nodes))

	for _, node := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			
			versions, err := sendGetRequest(addr, key)
			if err != nil {
				errsChan <- err
			} else {
				resultsChan <- versions
			}
		}(node)
	}

	timeout := time.After(3 * time.Second)
	successfulReads := 0
	failedReads := 0
	
	var allVersions []conflict.VersionedData
	var lastErr error

	for {
		if successfulReads >= rc.R {
			break
		}

		select {
		case versions := <-resultsChan:
			allVersions = append(allVersions, versions...)
			successfulReads++
			if successfulReads >= rc.R {
				goto ResolvePoint
			}
		case err := <-errsChan:
			failedReads++
			lastErr = err
			
			if len(nodes) - failedReads < rc.R {
				return nil, fmt.Errorf("read quorum failed: collected %d/%d responses, %d failures. Last err: %v", 
					successfulReads, rc.R, failedReads, lastErr)
			}
			
		case <-timeout:
			return nil, fmt.Errorf("read timeout: collected %d/%d responses. Last err: %v", 
				successfulReads, rc.R, lastErr)
		}
	}

ResolvePoint:
	if len(allVersions) == 0 {
		return nil, fmt.Errorf("key not found in any replica")
	}

	resolver := conflict.NewResolver()
	resolvedVersions := resolver.Resolve(allVersions)

	return resolvedVersions, nil
}
