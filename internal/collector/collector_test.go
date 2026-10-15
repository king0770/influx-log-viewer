package collector

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influx-log-viewer/internal/logstore"
)

type fakeSource struct {
	mu    sync.Mutex
	calls int
}

func (f *fakeSource) List(context.Context, string, string) ([]Target, error) {
	return []Target{{Namespace: "influxdb3", Pod: "p1", Container: "c1"}}, nil
}
func (f *fakeSource) Follow(_ context.Context, _ Target, _ time.Time) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return io.NopCloser(strings.NewReader("2026-08-05T12:00:00Z hello\n2026-08-05T12:00:00Z hello\n2026-08-05T12:00:01Z world\n")), nil
}

type fakeWriter struct {
	mu      sync.Mutex
	batches [][]logstore.Entry
}

func (f *fakeWriter) Write(_ context.Context, entries []logstore.Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, append([]logstore.Entry(nil), entries...))
	return nil
}

func TestCollectorBatchesAndDeduplicates(t *testing.T) {
	source, writer := &fakeSource{}, &fakeWriter{}
	var published []logstore.Entry
	c := New(source, writer, Config{Namespace: "influxdb3", BatchSize: 2, FlushInterval: 10 * time.Millisecond, ReconnectDelay: time.Hour}, func(e logstore.Entry) { published = append(published, e) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.After(time.Second)
	for {
		writer.mu.Lock()
		count := 0
		for _, b := range writer.batches {
			count += len(b)
		}
		writer.mu.Unlock()
		if count >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	<-done
	writer.mu.Lock()
	defer writer.mu.Unlock()
	total := 0
	for _, b := range writer.batches {
		if len(b) > 2 {
			t.Fatalf("oversized batch")
		}
		total += len(b)
	}
	if total != 2 || len(published) != 2 {
		t.Fatalf("written=%d published=%d batches=%v", total, len(published), writer.batches)
	}
}

func TestCollectorRetriesWithoutOversizedBatches(t *testing.T) {
	sourceData := "2026-08-05T12:00:00Z one\n2026-08-05T12:00:01Z two\n2026-08-05T12:00:02Z three\n2026-08-05T12:00:03Z four\n"
	sourceFollow := sourceWithData{data: sourceData}
	writer := &flakyWriter{failures: 1}
	c := New(sourceFollow, writer, Config{Namespace: "influxdb3", BatchSize: 2, FlushInterval: time.Millisecond, ReconnectDelay: time.Hour}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for writer.written() < 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if writer.written() != 4 {
		t.Fatalf("written=%d", writer.written())
	}
	if writer.maxBatch() > 2 {
		t.Fatalf("retry produced oversized batch: %d", writer.maxBatch())
	}
}

type sourceWithData struct{ data string }

func (s sourceWithData) List(context.Context, string, string) ([]Target, error) {
	return []Target{{Namespace: "influxdb3", Pod: "p1", Container: "c1"}}, nil
}
func (s sourceWithData) Follow(_ context.Context, _ Target, _ time.Time) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.data)), nil
}

type flakyWriter struct {
	mu       sync.Mutex
	failures int
	total    int
	max      int
}

func (w *flakyWriter) Write(_ context.Context, entries []logstore.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(entries) > w.max {
		w.max = len(entries)
	}
	if w.failures > 0 {
		w.failures--
		return io.ErrUnexpectedEOF
	}
	w.total += len(entries)
	return nil
}
func (w *flakyWriter) written() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.total
}
func (w *flakyWriter) maxBatch() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.max
}

func TestConfigDefaults(t *testing.T) {
	got := (Config{}).WithDefaults()
	if got.Namespace != "influxdb3" || got.BatchSize != 500 || got.FlushInterval <= 0 || got.ReconnectDelay <= 0 {
		t.Fatalf("bad defaults: %#v", got)
	}
}
