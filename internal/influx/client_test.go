package influx

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influx-log-viewer/internal/logstore"
)

func TestBuildSearchSQL(t *testing.T) {
	q, params, err := BuildSearchSQL(SearchOptions{Query: "error_100%", Namespace: "prod", Pod: "api", Limit: 50, Since: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(q, "error_100") {
		t.Fatalf("query must use parameters: %s", q)
	}
	for _, want := range []string{"$search", "$namespace", "$pod", "LIMIT 50", "INTERVAL '3600 seconds'", "ESCAPE '\\'"} {
		if !strings.Contains(q, want) {
			t.Fatalf("missing %q in %s", want, q)
		}
	}
	if params["search"] != "%error\\_100\\%%" || params["namespace"] != "prod" || params["pod"] != "api" {
		t.Fatalf("unexpected params: %#v", params)
	}
}

func TestBuildSearchSQLSupportsExcludedTerms(t *testing.T) {
	q, params, err := BuildSearchSQL(SearchOptions{Query: "ingest -pacha", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, "message ILIKE $search") || !strings.Contains(q, "NOT (message ILIKE $exclude") {
		t.Fatalf("missing include/exclude predicates in %s", q)
	}
	if params["search"] != "%ingest%" || params["exclude"] != "%pacha%" {
		t.Fatalf("unexpected params: %#v", params)
	}
	if strings.Contains(q, "ingest") || strings.Contains(q, "pacha") {
		t.Fatalf("terms must be parameterized: %s", q)
	}
}

func TestBuildSearchSQLRejectsInvalidOptions(t *testing.T) {
	for _, opts := range []SearchOptions{{Limit: 1001}, {Limit: -1}, {Query: strings.Repeat("x", 513)}, {Since: -time.Second}} {
		if _, _, err := BuildSearchSQL(opts); err == nil {
			t.Fatalf("expected error for %#v", opts)
		}
	}
}

func TestClientQueryParsesInfluxTimestampsWithoutZoneAsUTC(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"time":"2026-08-05T12:00:00.123456789","namespace":"influxdb3","pod":"p","container":"c","stream":"stdout","message":"m"}]`))
	}))
	defer s.Close()
	c, _ := NewClient(s.URL, "", "logs", s.Client())
	rows, err := c.Search(context.Background(), SearchOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 8, 5, 12, 0, 0, 123456789, time.UTC)
	if len(rows) != 1 || !rows[0].Timestamp.Equal(want) {
		t.Fatalf("rows=%#v want=%s", rows, want)
	}
}

func TestClientWriteSendsLineProtocol(t *testing.T) {
	var body, auth, db string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		body, auth, db = string(b), r.Header.Get("Authorization"), r.URL.Query().Get("db")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer s.Close()
	c, err := NewClient(s.URL, "secret", "logs", s.Client())
	if err != nil {
		t.Fatal(err)
	}
	err = c.Write(context.Background(), []logstore.Entry{{Message: "hello", Timestamp: time.Unix(0, 1)}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body, "message=\"hello\"") || auth != "Bearer secret" || db != "logs" {
		t.Fatalf("body=%q auth=%q db=%q", body, auth, db)
	}
}

func TestClientQueryUsesV3EndpointAndNamedParameters(t *testing.T) {
	var path, method string
	var payload struct {
		DB     string         `json:"db"`
		Q      string         `json:"q"`
		Format string         `json:"format"`
		Params map[string]any `json:"params"`
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, method = r.URL.Path, r.Method
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"message":"ok"}]`))
	}))
	defer s.Close()
	c, _ := NewClient(s.URL, "", "logs", s.Client())
	rows, err := c.Search(context.Background(), SearchOptions{Query: "panic", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if path != "/api/v3/query_sql" || method != http.MethodPost || payload.DB != "logs" || payload.Q == "" || payload.Params["search"] != "%panic%" || len(rows) != 1 {
		t.Fatalf("path=%s method=%s payload=%#v rows=%v", path, method, payload, rows)
	}
}
