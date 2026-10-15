package collector

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/influxdata/influx-log-viewer/internal/logstore"
)

type Target struct{ Namespace, Pod, Container string }
type Source interface {
	List(context.Context, string, string) ([]Target, error)
	Follow(context.Context, Target, time.Time) (io.ReadCloser, error)
}
type Writer interface {
	Write(context.Context, []logstore.Entry) error
}

type Config struct {
	Namespace, LabelSelector      string
	BatchSize                     int
	FlushInterval, ReconnectDelay time.Duration
}

func (c Config) WithDefaults() Config {
	if c.Namespace == "" {
		c.Namespace = "influxdb3"
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	if c.ReconnectDelay <= 0 {
		c.ReconnectDelay = 2 * time.Second
	}
	return c
}

type Collector struct {
	source  Source
	writer  Writer
	config  Config
	publish func(logstore.Entry)
	mu      sync.Mutex
	seen    map[string]struct{}
}

func New(source Source, writer Writer, config Config, publish func(logstore.Entry)) *Collector {
	if publish == nil {
		publish = func(logstore.Entry) {}
	}
	return &Collector{source: source, writer: writer, config: config.WithDefaults(), publish: publish, seen: make(map[string]struct{})}
}

func (c *Collector) unique(e logstore.Entry) bool {
	key := fmt.Sprintf("%d\x00%s\x00%s\x00%s\x00%s", e.Timestamp.UnixNano(), e.Pod, e.Container, e.Stream, e.Message)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[key]; ok {
		return false
	}
	if len(c.seen) >= 10000 {
		c.seen = make(map[string]struct{})
	}
	c.seen[key] = struct{}{}
	return true
}

func (c *Collector) follow(ctx context.Context, target Target, out chan<- logstore.Entry) {
	var since time.Time
	for ctx.Err() == nil {
		stream, err := c.source.Follow(ctx, target, since)
		if err == nil {
			scanner := bufio.NewScanner(stream)
			scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
			for scanner.Scan() {
				e, _ := logstore.ParseLine(scanner.Text(), target.Namespace, target.Pod, target.Container)
				if e.Timestamp.After(since) {
					since = e.Timestamp
				}
				if c.unique(e) {
					select {
					case out <- e:
					case <-ctx.Done():
						_ = stream.Close()
						return
					}
				}
			}
			_ = stream.Close()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.config.ReconnectDelay):
		}
	}
}

func (c *Collector) Run(ctx context.Context) error {
	entries := make(chan logstore.Entry, c.config.BatchSize*2)
	var wg sync.WaitGroup
	workers := map[Target]struct{}{}
	discover := func() {
		targets, err := c.source.List(ctx, c.config.Namespace, c.config.LabelSelector)
		if err != nil {
			return
		}
		for _, target := range targets {
			if _, ok := workers[target]; ok {
				continue
			}
			workers[target] = struct{}{}
			wg.Add(1)
			go func(t Target) { defer wg.Done(); c.follow(ctx, t, entries) }(target)
		}
	}
	discover()
	poll := time.NewTicker(15 * time.Second)
	defer poll.Stop()
	flush := time.NewTicker(c.config.FlushInterval)
	defer flush.Stop()
	batch := make([]logstore.Entry, 0, c.config.BatchSize)
	write := func() {
		if len(batch) == 0 {
			return
		}
		n := min(len(batch), c.config.BatchSize)
		pending := batch[:n]
		if c.writer.Write(ctx, pending) != nil {
			return
		}
		for _, e := range pending {
			c.publish(e)
		}
		batch = batch[n:]
	}
	for {
		select {
		case <-ctx.Done():
			write()
			wg.Wait()
			return nil
		case <-poll.C:
			discover()
		case <-flush.C:
			write()
		case e := <-entries:
			batch = append(batch, e)
			if len(batch) >= c.config.BatchSize {
				write()
			}
		}
	}
}
