package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

func (r *Raft) StartHTTP(addr string) {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /cache/{key}", r.handleGet)
	mux.HandleFunc("PUT /cache/{key}", r.handlePut)
	mux.HandleFunc("DELETE /cache/{key}", r.handleDelete)
	mux.HandleFunc("GET /status", r.handleStatus)

	log.Printf("Node %d HTTP server on %s", r.id, addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func (r *Raft) handleGet(w http.ResponseWriter, req *http.Request) {
	key := req.PathValue("key")

	value, ok := r.cache.Get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(value)
}

func (r *Raft) handlePut(w http.ResponseWriter, req *http.Request) {
	key := req.PathValue("key")

	value, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	var ttl time.Duration
	if ttlStr := req.URL.Query().Get("ttl"); ttlStr != "" {
		secs, err := strconv.Atoi(ttlStr)
		if err != nil {
			http.Error(w, "invalid ttl", http.StatusBadRequest)
			return
		}
		ttl = time.Duration(secs) * time.Second
	}

	ch := r.appendEntry(Command{Op: "put", Key: key, Value: value, TTL: ttl})

	select {
	case err := <-ch:
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusCreated)
	case <-time.After(3 * time.Second):
		http.Error(w, "commit timeout", http.StatusGatewayTimeout)
	}
}

func (r *Raft) handleDelete(w http.ResponseWriter, req *http.Request) {
	key := req.PathValue("key")

	ch := r.appendEntry(Command{Op: "delete", Key: key})

	select {
	case err := <-ch:
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	case <-time.After(3 * time.Second):
		http.Error(w, "commit timeout", http.StatusGatewayTimeout)
	}
}

func (r *Raft) handleStatus(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	role := r.role
	term := r.currTerm
	r.mu.Unlock()

	roleStr := "follower"
	if role == 0 {
		roleStr = "leader"
	} else if role == 1 {
		roleStr = "candidate"
	}

	status := map[string]interface{}{
		"id":         r.id,
		"role":       roleStr,
		"term":       term,
		"cache_size": r.cache.Len(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}
