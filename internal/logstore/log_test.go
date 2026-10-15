package logstore

import (
	"strings"
	"testing"
	"time"
)

func TestPointLineProtocolEscapesValues(t *testing.T) {
	p := Entry{Namespace: "influx db", Pod: "api,pod", Container: "main=one", Stream: "stderr", Message: "hello \"world\"\\next\nline", Timestamp: time.Unix(0, 123)}
	got := p.LineProtocol()
	want := "kubernetes_logs,namespace=influx\\ db,pod=api\\,pod,container=main\\=one,stream=stderr message=\"hello \\\"world\\\"\\\\next\\nline\" 123"
	if got != want {
		t.Fatalf("got %q\nwant %q", got, want)
	}
}

func TestParseKubernetesLogLine(t *testing.T) {
	entry, err := ParseLine("2026-08-05T12:34:56.123456789Z stderr F something happened", "ns", "pod", "container")
	if err != nil {
		t.Fatal(err)
	}
	if entry.Stream != "stderr" || entry.Message != "something happened" || entry.Timestamp.Nanosecond() != 123456789 {
		t.Fatalf("unexpected entry: %#v", entry)
	}
}

func TestParseKubernetesAPITimestampPrefix(t *testing.T) {
	entry, err := ParseLine("2026-08-05T12:34:56Z message from API", "ns", "pod", "container")
	if err != nil {
		t.Fatal(err)
	}
	if entry.Message != "message from API" || entry.Timestamp.Year() != 2026 {
		t.Fatalf("unexpected entry: %#v", entry)
	}
}

func TestParseKubernetesLogLineWithoutPrefixUsesFallback(t *testing.T) {
	fallback := time.Unix(10, 0)
	entry, err := ParseLineAt("plain message", "ns", "pod", "container", fallback)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Message != "plain message" || !entry.Timestamp.Equal(fallback) {
		t.Fatalf("unexpected entry: %#v", entry)
	}
}

func TestBatchLines(t *testing.T) {
	entries := []Entry{{Message: "one", Timestamp: time.Unix(0, 1)}, {Message: "two", Timestamp: time.Unix(0, 2)}}
	if got := BatchLines(entries); strings.Count(got, "\n") != 1 {
		t.Fatalf("expected two lines, got %q", got)
	}
}
