package web

import (
	"strconv"
	"sync"

	"github.com/influxdata/influx-log-viewer/internal/logstore"
)

// Event is a resumable server-sent log event.
type Event struct {
	ID    string         `json:"id"`
	Entry logstore.Entry `json:"entry"`
}

type Hub struct {
	mu          sync.Mutex
	next        uint64
	capacity    int
	history     []Event
	subscribers map[chan Event]struct{}
}

func NewHub(capacity int) *Hub {
	if capacity < 1 {
		capacity = 1
	}
	return &Hub{capacity: capacity, subscribers: make(map[chan Event]struct{})}
}

func (h *Hub) Publish(entry logstore.Entry) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.next++
	e := Event{ID: strconv.FormatUint(h.next, 10), Entry: entry}
	h.history = append(h.history, e)
	if len(h.history) > h.capacity {
		h.history = h.history[len(h.history)-h.capacity:]
	}
	for ch := range h.subscribers {
		select {
		case ch <- e:
		default:
		}
	}
}

func (h *Hub) Subscribe(afterID string) (<-chan Event, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := make(chan Event, h.capacity+16)
	if after, err := strconv.ParseUint(afterID, 10, 64); err == nil {
		for _, e := range h.history {
			id, _ := strconv.ParseUint(e.ID, 10, 64)
			if id > after {
				ch <- e
			}
		}
	}
	h.subscribers[ch] = struct{}{}
	var once sync.Once
	return ch, func() { once.Do(func() { h.mu.Lock(); delete(h.subscribers, ch); close(ch); h.mu.Unlock() }) }
}
