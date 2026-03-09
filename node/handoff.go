package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/mini-dynamo/gossip"
)

type HandoffManager struct {
	Hints      StorageEngine
	Membership *gossip.Membership
	client     *http.Client
	stopChan   chan struct{}
}

func NewHandoffManager(hints StorageEngine, m *gossip.Membership) *HandoffManager {
	return &HandoffManager{
		Hints:      hints,
		Membership: m,
		client:     &http.Client{Timeout: 2 * time.Second},
		stopChan:   make(chan struct{}),
	}
}

func (hm *HandoffManager) Start() {
	go hm.forwardLoop()
}

func (hm *HandoffManager) Stop() {
	close(hm.stopChan)
}

func (hm *HandoffManager) forwardLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.processHints()
		case <-hm.stopChan:
			return
		}
	}
}

func (hm *HandoffManager) processHints() {
	// Keys are structured as: "target_addr:::actual_key"
	var allKeys []string
	
	if inMem, ok := hm.Hints.(*InMemoryStorage); ok {
		allKeys = inMem.GetAllKeys()
	}

	nodesState := hm.Membership.GetAllNodes()

	for _, hintKey := range allKeys {
		parts := strings.SplitN(hintKey, ":::", 2)
		if len(parts) != 2 {
			continue
		}

		targetAddr := parts[0]
		actualKey := parts[1]

		// Check if the target is alive
		alive := false
		for _, meta := range nodesState {
			if meta.Address == targetAddr && meta.State == gossip.Alive {
				alive = true
				break
			}
		}

		if alive {

			hm.forwardHint(hintKey, targetAddr, actualKey)
		}
	}
}

func (hm *HandoffManager) forwardHint(hintKey, targetAddr, actualKey string) {
	versions, err := hm.Hints.Get(hintKey)
	if err != nil || len(versions) == 0 {
		return
	}


	dataToForward := versions[0] 

	// Try to POST it directly to their normal PUT endpoint as a standard direct write
	url := fmt.Sprintf("http://%s/kv/%s?direct=true", targetAddr, actualKey)
	
	body, err := json.Marshal(dataToForward)
	if err != nil {
		return
	}

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := hm.client.Do(req)
	if err != nil {
		log.Printf("Failed to forward handoff to %s: %v\n", targetAddr, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {

		hm.Hints.Delete(hintKey)
		log.Printf("Successfully forwarded Hinted Handoff for key %s to %s\n", actualKey, targetAddr)
	}
}
