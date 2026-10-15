package web

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influx-log-viewer/internal/influx"
	"github.com/influxdata/influx-log-viewer/internal/logstore"
)

func TestHubPublishesAndReplaysAfterID(t *testing.T) {
	h := NewHub(4)
	h.Publish(logstore.Entry{Message: "one"})
	h.Publish(logstore.Entry{Message: "two"})
	ch, cancel := h.Subscribe("1")
	defer cancel()
	select {
	case event := <-ch:
		if event.ID != "2" || event.Entry.Message != "two" {
			t.Fatalf("unexpected: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestHubSlowSubscriberDoesNotBlock(t *testing.T) {
	h := NewHub(1)
	_, cancel := h.Subscribe("")
	defer cancel()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			h.Publish(logstore.Entry{Message: "x"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publish blocked")
	}
}

type fakeSearcher struct {
	options influx.SearchOptions
	rows    []logstore.Entry
	err     error
}

func (f *fakeSearcher) Search(_ context.Context, o influx.SearchOptions) ([]logstore.Entry, error) {
	f.options = o
	return f.rows, f.err
}

func TestLogsHandlerValidatesAndReturnsJSON(t *testing.T) {
	f := &fakeSearcher{rows: []logstore.Entry{{Message: "found"}}}
	s := NewServer(f, NewHub(8), func() bool { return true })
	r := httptest.NewRequest(http.MethodGet, "/api/v1/logs?q=error&limit=20&since=5m&namespace=prod", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 200 || f.options.Query != "error" || f.options.Limit != 20 || f.options.Since != 5*time.Minute {
		t.Fatalf("code=%d opts=%#v body=%s", w.Code, f.options, w.Body.String())
	}
	var rows []logstore.Entry
	if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil || len(rows) != 1 {
		t.Fatalf("invalid JSON: %v %s", err, w.Body.String())
	}
}

func TestLogsHandlerRejectsBadLimit(t *testing.T) {
	s := NewServer(&fakeSearcher{}, NewHub(8), nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/logs?limit=oops", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("got %d", w.Code)
	}
}

func TestMatchesLiveFiltersSupportsExcludedTerms(t *testing.T) {
	filters := liveFilters{query: "ingest -pacha", namespace: "influxdb3"}
	if !filters.matches(logstore.Entry{Namespace: "influxdb3", Message: "ingest completed"}) {
		t.Fatal("expected included live event")
	}
	if filters.matches(logstore.Entry{Namespace: "influxdb3", Message: "pacha ingest completed"}) {
		t.Fatal("expected excluded live event")
	}
	if filters.matches(logstore.Entry{Namespace: "influxdb3", Message: "query completed"}) {
		t.Fatal("expected event missing required term to be filtered")
	}
}

func TestStreamHandlerEmitsSSE(t *testing.T) {
	h := NewHub(8)
	s := httptest.NewServer(NewServer(&fakeSearcher{}, h, nil).Handler())
	defer s.Close()
	resp, err := s.Client().Get(s.URL + "/api/v1/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	h.Publish(logstore.Entry{Message: "live"})
	buf := make([]byte, 512)
	n, err := resp.Body.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	got := string(buf[:n])
	if !strings.Contains(got, "event: log") || !strings.Contains(got, `"message":"live"`) {
		t.Fatalf("unexpected SSE %q", got)
	}
}

func TestEmbeddedUIOffersPersistentColumnReordering(t *testing.T) {
	html := string(indexHTML)
	for _, required := range []string{`draggable="true"`, `data-column="time"`, `data-column="message"`, `log-column-order`} {
		if !strings.Contains(html, required) {
			t.Fatalf("embedded UI is missing column reordering marker %q", required)
		}
	}
}

func TestOperationalEndpoints(t *testing.T) {
	s := NewServer(&fakeSearcher{}, NewHub(8), func() bool { return false })
	h := s.Handler()
	for path, code := range map[string]int{"/healthz": 200, "/readyz": 503, "/metrics": 200, "/": 200} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != code {
			t.Errorf("%s got %d", path, w.Code)
		}
	}
}
