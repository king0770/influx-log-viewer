package web

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/influxdata/influx-log-viewer/internal/influx"
	"github.com/influxdata/influx-log-viewer/internal/logstore"
)

//go:embed static/index.html
var indexHTML []byte

type Searcher interface {
	Search(context.Context, influx.SearchOptions) ([]logstore.Entry, error)
}
type Server struct {
	searcher Searcher
	hub      *Hub
	ready    func() bool
	requests atomic.Uint64
	streams  atomic.Int64
}

func NewServer(searcher Searcher, hub *Hub, ready func() bool) *Server {
	if ready == nil {
		ready = func() bool { return true }
	}
	return &Server{searcher: searcher, hub: hub, ready: ready}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.index)
	mux.HandleFunc("GET /api/v1/logs", s.logs)
	mux.HandleFunc("GET /api/v1/stream", s.stream)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("GET /metrics", s.metrics)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) index(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	limit := 200
	if raw := v.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 1000 {
			http.Error(w, "limit must be 1..1000", 400)
			return
		}
		limit = n
	}
	var since time.Duration
	if raw := v.Get("since"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 || d > 365*24*time.Hour {
			http.Error(w, "invalid since", 400)
			return
		}
		since = d
	}
	o := influx.SearchOptions{Query: v.Get("q"), Namespace: v.Get("namespace"), Pod: v.Get("pod"), Container: v.Get("container"), Stream: v.Get("stream"), Limit: limit, Since: since}
	rows, err := s.searcher.Search(r.Context(), o)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if rows == nil {
		rows = []logstore.Entry{}
	}
	writeJSON(w, 200, rows)
}

type liveFilters struct {
	query, namespace, pod, container, stream string
	since                                    time.Duration
}

func (f liveFilters) matches(entry logstore.Entry) bool {
	if (f.namespace != "" && entry.Namespace != f.namespace) ||
		(f.pod != "" && entry.Pod != f.pod) ||
		(f.container != "" && entry.Container != f.container) ||
		(f.stream != "" && entry.Stream != f.stream) {
		return false
	}
	if f.since > 0 && entry.Timestamp.Before(time.Now().Add(-f.since)) {
		return false
	}
	message := strings.ToLower(entry.Message)
	for _, term := range strings.Fields(f.query) {
		excluded := strings.HasPrefix(term, "-") && len(term) > 1
		if excluded {
			if strings.Contains(message, strings.ToLower(strings.TrimPrefix(term, "-"))) {
				return false
			}
			continue
		}
		if !strings.Contains(message, strings.ToLower(term)) {
			return false
		}
	}
	return true
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	var since time.Duration
	if raw := v.Get("since"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 || d > 365*24*time.Hour {
			http.Error(w, "invalid since", http.StatusBadRequest)
			return
		}
		since = d
	}
	filters := liveFilters{
		query:     v.Get("q"),
		namespace: v.Get("namespace"),
		pod:       v.Get("pod"),
		container: v.Get("container"),
		stream:    v.Get("stream"),
		since:     since,
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(200)
	flusher.Flush()
	ch, cancel := s.hub.Subscribe(r.Header.Get("Last-Event-ID"))
	defer cancel()
	s.streams.Add(1)
	defer s.streams.Add(-1)
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			_, _ = fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case event, open := <-ch:
			if !open {
				return
			}
			if !filters.matches(event.Entry) {
				continue
			}
			payload, _ := json.Marshal(event.Entry)
			if _, err := fmt.Fprintf(w, "id: %s\nevent: log\ndata: %s\n\n", event.ID, payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "# TYPE log_viewer_http_requests_total counter\nlog_viewer_http_requests_total %d\n# TYPE log_viewer_sse_connections gauge\nlog_viewer_sse_connections %d\n", s.requests.Load(), s.streams.Load())
}
