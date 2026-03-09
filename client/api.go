package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	Endpoints  []string
	HTTPClient *http.Client
}

func NewClient(endpoints []string) *Client {
	return &Client{
		Endpoints: endpoints,
		HTTPClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *Client) getRandomEndpoint() string {
	return c.Endpoints[0]
}

func (c *Client) Put(key, value string) error {
	endpoint := c.getRandomEndpoint()
	url := fmt.Sprintf("http://%s/kv/%s", endpoint, key)

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBufferString(value))
	if err != nil {
		return err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *Client) Get(key string) ([]string, error) {
	endpoint := c.getRandomEndpoint()
	url := fmt.Sprintf("http://%s/kv/%s", endpoint, key)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("key not found")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var rawData []struct {
		Value string `json:"Value"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rawData); err != nil {
		return nil, err
	}

	var results []string
	for _, v := range rawData {
		results = append(results, v.Value)
	}

	return results, nil
}
