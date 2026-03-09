package quorum

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mini-dynamo/conflict"
)

// httpClient is used for communicating with other nodes
var httpClient = &http.Client{
	Timeout: 2 * time.Second,
}

func sendPutRequest(nodeAddr, key string, data conflict.VersionedData) error {
	// nodeAddr might contain "?direct=true" from coordinator loop, so parse it
	url := fmt.Sprintf("http://%s/kv/%s", nodeAddr, key)
	if len(nodeAddr) > 12 && nodeAddr[len(nodeAddr)-12:] == "?direct=true" {
        addrOnly := nodeAddr[:len(nodeAddr)-12]
        url = fmt.Sprintf("http://%s/kv/%s?direct=true", addrOnly, key)
    }

	body, err := json.Marshal(data)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("node %s responded with status %d: %s", nodeAddr, resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// targetNode is the node the data was actually meant for
func sendHintRequest(fallbackAddr string, targetNode string, key string, data conflict.VersionedData) error {
	url := fmt.Sprintf("http://%s/kv/handoff", fallbackAddr) // No direct=true needed for handoff endpoint

	payload := struct {
		TargetNode string                 `json:"target_node"`
		Key        string                 `json:"key"`
		Data       conflict.VersionedData `json:"data"`
	}{
		TargetNode: targetNode,
		Key:        key,
		Data:       data,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("node %s rejected handoff with status %d: %s", fallbackAddr, resp.StatusCode, string(bodyBytes))
	}

	return nil
}

func sendGetRequest(nodeAddr, key string) ([]conflict.VersionedData, error) {
	url := fmt.Sprintf("http://%s/kv/%s", nodeAddr, key)
	if len(nodeAddr) > 12 && nodeAddr[len(nodeAddr)-12:] == "?direct=true" {
        addrOnly := nodeAddr[:len(nodeAddr)-12]
        url = fmt.Sprintf("http://%s/kv/%s?direct=true", addrOnly, key)
    }

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("key not found")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("node %s responded with status %d", nodeAddr, resp.StatusCode)
	}

	var versions []conflict.VersionedData
	if err := json.NewDecoder(resp.Body).Decode(&versions); err != nil {
		return nil, err
	}

	return versions, nil
}
