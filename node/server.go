package node

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/mini-dynamo/conflict"
	"github.com/mini-dynamo/hashring"
	"github.com/mini-dynamo/quorum"
)

type Server struct {
	ID         string
	Address    string
	Storage    StorageEngine
	Hints      StorageEngine
	Ring       *hashring.ConsistentHash
	WriteCo    *quorum.WriteCoordinator
	ReadCo     *quorum.ReadCoordinator
	Replicate  *ReplicationManager
	httpServer *http.Server
}

func NewServer(id, address string, ring *hashring.ConsistentHash, n, w, r int) *Server {
	return &Server{
		ID:        id,
		Address:   address,
		Storage:   NewInMemoryStorage(),
		Hints:     NewInMemoryStorage(),
		Ring:      ring,
		WriteCo:   quorum.NewWriteCoordinator(n, w),
		ReadCo:    quorum.NewReadCoordinator(n, r),
		Replicate: NewReplicationManager(ring, n),
	}
}

func (s *Server) Start() error {
	// Start Handoff Manager
	handoffManager := NewHandoffManager(s.Hints, s.Replicate.Membership)
	handoffManager.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/kv/handoff", s.handleHandoff)

	s.httpServer = &http.Server{
		Addr:    s.Address,
		Handler: mux,
	}

	log.Printf("Node %s starting on %s\n", s.ID, s.Address)
	return s.httpServer.ListenAndServe()
}

func (s *Server) Stop() {
	if s.httpServer != nil {
		s.httpServer.Close()
	}
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/kv/"):]
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	if r.URL.Query().Get("direct") == "true" {
		versions, err := s.Storage.Get(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(versions)
		return
	}

	replicas := s.Replicate.GetPreferenceList(key)
	
	readAddrs := make([]string, len(replicas))
	for i, r := range replicas {
		readAddrs[i] = r + "?direct=true"
	}

	versions, err := s.ReadCo.Read(readAddrs, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(versions)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var incoming conflict.VersionedData
	if err := json.Unmarshal(body, &incoming); err != nil {
		incoming = conflict.VersionedData{
			Value:       string(body),
			VectorClock: conflict.NewVectorClock(),
		}
	}

	if incoming.VectorClock == nil {
		incoming.VectorClock = conflict.NewVectorClock()
	}

	if r.URL.Query().Get("direct") == "true" {
		err = s.Storage.Put(key, incoming)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		return
	}

	incoming.VectorClock.Increment(s.ID)
	targets := s.Replicate.GetHealthyPreferenceList(key)
	
	quorumTargets := make([]quorum.NodeTarget, len(targets))
	for i, t := range targets {
		quorumTargets[i] = quorum.NodeTarget{
			Address:    t.Address + "?direct=true",
			IsFallback: t.IsFallback,
			HandOffFor: t.HandOffFor,
		}
	}

	err = s.WriteCo.Write(quorumTargets, key, incoming)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Successfully coordinated write for key: %s\n", key)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	err := s.Storage.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// target node => key => versions
type HandoffHint struct {
	TargetNode string                   `json:"target_node"`
	Key        string                   `json:"key"`
	Data       conflict.VersionedData   `json:"data"`
}

func (s *Server) handleHandoff(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var hint HandoffHint
	if err := json.Unmarshal(body, &hint); err != nil {
		http.Error(w, "Invalid hint payload", http.StatusBadRequest)
		return
	}

	// We prefix the key in our Hints storage with the target node so we can look it up later
	// key format: "target_addr:::actual_key"
	hintKey := fmt.Sprintf("%s:::%s", hint.TargetNode, hint.Key)
	
	err = s.Hints.Put(hintKey, hint.Data)
	if err != nil {
		http.Error(w, "Failed to store hint", http.StatusInternalServerError)
		return
	}

	log.Printf("Stored Hinted Handoff for node %s, key %s\n", hint.TargetNode, hint.Key)
	w.WriteHeader(http.StatusCreated)
}
