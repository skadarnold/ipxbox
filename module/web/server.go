// HTTP + Server-Sent Events for the web dashboard.
//
// Copyright (C) 2025-2026 Daniel Arnold. Part of a community fork of ipxbox.
// GPL-2.0-or-later.
//
// stdlib only — no WebSocket or charting dependencies. The page is pushed a
// fresh topology snapshot every ~1.5s over SSE so liveness stays current.

package web

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

//go:embed index.html
var indexHTML []byte

type server struct {
	tr  *tracker
	mux *http.ServeMux
}

func newServer(tr *tracker) *server {
	s := &server{tr: tr, mux: http.NewServeMux()}
	s.mux.HandleFunc("/", s.handleIndex)
	s.mux.HandleFunc("/api/topology", s.handleTopology)
	s.mux.HandleFunc("/events", s.handleEvents)
	return s
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Write(indexHTML)
}

func (s *server) handleTopology(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.tr.snapshot())
}

// handleEvents streams topology snapshots to the browser over SSE.
func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	send := func() {
		if data, err := json.Marshal(s.tr.snapshot()); err == nil {
			fmt.Fprintf(w, "event: topology\ndata: %s\n\n", data)
			flusher.Flush()
		}
	}
	send() // populate immediately

	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			send()
		}
	}
}
