package influx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influx-log-viewer/internal/logstore"
)

// SearchOptions controls bounded server-side SQL search.
type SearchOptions struct {
	Query, Namespace, Pod, Container, Stream string
	Limit                                    int
	Since                                    time.Duration
}

func likePattern(s string) string {
	r := strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_")
	return "%" + r.Replace(s) + "%"
}

// BuildSearchSQL builds a parameterized DataFusion SQL query and named parameters.
func BuildSearchSQL(o SearchOptions) (string, map[string]any, error) {
	if o.Limit < 0 || o.Limit > 1000 || len(o.Query) > 512 || o.Since < 0 {
		return "", nil, errors.New("invalid search options")
	}
	if o.Limit == 0 {
		o.Limit = 200
	}
	q := `SELECT time, namespace, pod, container, stream, message FROM kubernetes_logs WHERE 1=1`
	params := make(map[string]any, 5)
	add := func(name, clause string, value any) {
		params[name] = value
		q += fmt.Sprintf(clause, name)
	}
	if o.Since > 0 {
		q += fmt.Sprintf(" AND time >= now() - INTERVAL '%d seconds'", int64(o.Since/time.Second))
	}
	if o.Query != "" {
		includeCount, excludeCount := 0, 0
		for _, term := range strings.Fields(o.Query) {
			excluded := strings.HasPrefix(term, "-") && len(term) > 1
			if excluded {
				term = strings.TrimPrefix(term, "-")
				name := "exclude"
				if excludeCount > 0 {
					name += strconv.Itoa(excludeCount + 1)
				}
				excludeCount++
				add(name, ` AND NOT (message ILIKE $%s ESCAPE '\')`, likePattern(term))
				continue
			}
			name := "search"
			if includeCount > 0 {
				name += strconv.Itoa(includeCount + 1)
			}
			includeCount++
			add(name, ` AND message ILIKE $%s ESCAPE '\'`, likePattern(term))
		}
	}
	if o.Namespace != "" {
		add("namespace", ` AND namespace = $%s`, o.Namespace)
	}
	if o.Pod != "" {
		add("pod", ` AND pod = $%s`, o.Pod)
	}
	if o.Container != "" {
		add("container", ` AND container = $%s`, o.Container)
	}
	if o.Stream != "" {
		add("stream", ` AND stream = $%s`, o.Stream)
	}
	q += " ORDER BY time DESC LIMIT " + strconv.Itoa(o.Limit)
	return q, params, nil
}

// Client writes and queries InfluxDB 3 over HTTP.
type Client struct {
	base            *url.URL
	token, database string
	http            *http.Client
}

func NewClient(rawURL, token, database string, hc *http.Client) (*Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, errors.New("invalid InfluxDB URL")
	}
	if database == "" {
		return nil, errors.New("database is required")
	}
	if hc == nil {
		hc = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{base: u, token: token, database: database, http: hc}, nil
}

func (c *Client) request(ctx context.Context, method, path string, query url.Values, body io.Reader) (*http.Response, error) {
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawQuery = query.Encode()
	r, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		r.Header.Set("Authorization", "Bearer "+c.token)
	}
	if body != nil {
		if path == "/api/v3/write_lp" {
			r.Header.Set("Content-Type", "text/plain; charset=utf-8")
		} else {
			r.Header.Set("Content-Type", "application/json")
		}
	}
	return c.http.Do(r)
}

func (c *Client) Write(ctx context.Context, entries []logstore.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	v := url.Values{"db": {c.database}}
	resp, err := c.request(ctx, http.MethodPost, "/api/v3/write_lp", v, strings.NewReader(logstore.BatchLines(entries)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("influx write: %s: %s", resp.Status, b)
	}
	return nil
}

func parseQueryTime(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	return time.ParseInLocation("2006-01-02T15:04:05.999999999", value, time.UTC)
}

func (c *Client) Search(ctx context.Context, o SearchOptions) ([]logstore.Entry, error) {
	q, params, err := BuildSearchSQL(o)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(struct {
		DB     string         `json:"db"`
		Q      string         `json:"q"`
		Format string         `json:"format"`
		Params map[string]any `json:"params,omitempty"`
	}{DB: c.database, Q: q, Format: "json", Params: params})
	if err != nil {
		return nil, err
	}
	resp, err := c.request(ctx, http.MethodPost, "/api/v3/query_sql", nil, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("influx query: %s: %s", resp.Status, b)
	}
	var rawRows []struct {
		Time      string `json:"time"`
		Namespace string `json:"namespace"`
		Pod       string `json:"pod"`
		Container string `json:"container"`
		Stream    string `json:"stream"`
		Message   string `json:"message"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&rawRows); err != nil {
		return nil, err
	}
	rows := make([]logstore.Entry, 0, len(rawRows))
	for _, raw := range rawRows {
		var timestamp time.Time
		if raw.Time != "" {
			timestamp, err = parseQueryTime(raw.Time)
			if err != nil {
				return nil, fmt.Errorf("parse query timestamp %q: %w", raw.Time, err)
			}
		}
		rows = append(rows, logstore.Entry{Timestamp: timestamp, Namespace: raw.Namespace, Pod: raw.Pod, Container: raw.Container, Stream: raw.Stream, Message: raw.Message})
	}
	return rows, nil
}
