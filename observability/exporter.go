package observability

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// MetricsExporter defines an interface for pushing metrics to external systems.
// Implementations may write to OTLP, InfluxDB, CloudWatch, or local files.
type MetricsExporter interface {
	Export(snapshot *MetricsSnapshot) error
	Close() error
}

// FileExporter periodically writes a JSON snapshot to a file.
// Useful as a fallback when no push-based exporter is configured.
type FileExporter struct {
	mu       sync.Mutex
	path     string
	interval time.Duration
	stopCh   chan struct{}
	source   func() *MetricsSnapshot
}

// NewFileExporter creates a file-based metrics exporter.
func NewFileExporter(path string, interval time.Duration, source func() *MetricsSnapshot) *FileExporter {
	return &FileExporter{
		path:     path,
		interval: interval,
		stopCh:   make(chan struct{}),
		source:   source,
	}
}

// Start begins periodic export. Call in a goroutine.
func (fe *FileExporter) Start() {
	log.Printf("exporter: writing metrics to %s every %s", fe.path, fe.interval)
	ticker := time.NewTicker(fe.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := fe.Export(fe.source()); err != nil {
				log.Printf("exporter: write error: %v", err)
			}
		case <-fe.stopCh:
			return
		}
	}
}

// Stop terminates the periodic export loop.
func (fe *FileExporter) Stop() {
	close(fe.stopCh)
}

// Export writes the snapshot as JSON to the configured file path.
func (fe *FileExporter) Export(snapshot *MetricsSnapshot) error {
	if snapshot == nil {
		return nil
	}
	fe.mu.Lock()
	defer fe.mu.Unlock()
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("exporter: marshal: %w", err)
	}
	tmp := fe.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("exporter: write %s: %w", tmp, err)
	}
	return os.Rename(tmp, fe.path)
}

// Close is a no-op for file exporter but satisfies the interface.
func (fe *FileExporter) Close() error { return nil }
