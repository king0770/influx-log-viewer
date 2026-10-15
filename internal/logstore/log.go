package logstore

import (
	"fmt"
	"strings"
	"time"
)

// Entry is one Kubernetes container log record.
type Entry struct {
	Timestamp time.Time `json:"time"`
	Namespace string    `json:"namespace"`
	Pod       string    `json:"pod"`
	Container string    `json:"container"`
	Stream    string    `json:"stream"`
	Message   string    `json:"message"`
}

func escapeTag(s string) string {
	r := strings.NewReplacer("\\", "\\\\", " ", "\\ ", ",", "\\,", "=", "\\=")
	return r.Replace(s)
}

func escapeField(s string) string {
	r := strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n", "\r", "\\r")
	return r.Replace(s)
}

// LineProtocol serializes the entry as escaped InfluxDB line protocol.
func (e Entry) LineProtocol() string {
	return fmt.Sprintf("kubernetes_logs,namespace=%s,pod=%s,container=%s,stream=%s message=\"%s\" %d",
		escapeTag(e.Namespace), escapeTag(e.Pod), escapeTag(e.Container), escapeTag(e.Stream), escapeField(e.Message), e.Timestamp.UnixNano())
}

// ParseLine parses a CRI-formatted Kubernetes log line.
func ParseLine(line, namespace, pod, container string) (Entry, error) {
	return ParseLineAt(line, namespace, pod, container, time.Now().UTC())
}

// ParseLineAt parses a CRI line, using fallback for unprefixed lines.
func ParseLineAt(line, namespace, pod, container string, fallback time.Time) (Entry, error) {
	e := Entry{Timestamp: fallback, Namespace: namespace, Pod: pod, Container: container, Stream: "stdout", Message: line}
	parts := strings.SplitN(line, " ", 4)
	if len(parts) < 2 {
		return e, nil
	}
	ts, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return e, nil
	}
	e.Timestamp = ts
	if len(parts) == 4 && (parts[1] == "stdout" || parts[1] == "stderr") && (parts[2] == "F" || parts[2] == "P") {
		e.Stream, e.Message = parts[1], parts[3]
		return e, nil
	}
	e.Message = strings.TrimPrefix(line, parts[0]+" ")
	return e, nil
}

// BatchLines serializes entries as a newline-delimited write batch.
func BatchLines(entries []Entry) string {
	lines := make([]string, len(entries))
	for i := range entries {
		lines[i] = entries[i].LineProtocol()
	}
	return strings.Join(lines, "\n")
}
