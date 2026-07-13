package config

import (
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aptshark/gateway/provider"
)

// ChangeEvent describes a single provider-level change.
type ChangeEvent struct {
	Action   string                  // "added", "removed", "updated"
	Provider provider.ProviderConfig
}

// Watcher polls a config file and emits events when it changes.
type Watcher struct {
	mu       sync.Mutex
	path     string
	interval time.Duration
	lastHash [32]byte

	onChange func([]ChangeEvent)

	stopCh chan struct{}
}

// NewWatcher creates a file watcher that polls at the given interval.
func NewWatcher(path string, interval time.Duration) *Watcher {
	return &Watcher{
		path:     path,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// OnChange registers a callback that receives the diff when the file changes.
func (w *Watcher) OnChange(fn func([]ChangeEvent)) {
	w.onChange = fn
}

// Start begins polling. Call in a goroutine.
func (w *Watcher) Start() {
	log.Printf("watcher: monitoring %s (every %s)", w.path, w.interval)
	w.updateHash()
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.check()
		case <-w.stopCh:
			return
		}
	}
}

// Stop terminates the polling loop.
func (w *Watcher) Stop() {
	close(w.stopCh)
}

// ReloadNow forces an immediate re-read and diff, bypassing the ticker.
// Returns the events and any parse error.
func (w *Watcher) ReloadNow() ([]ChangeEvent, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := os.ReadFile(w.path)
	if err != nil {
		return nil, fmt.Errorf("watcher: read %s: %w", w.path, err)
	}
	newCfg, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("watcher: parse %s: %w", w.path, err)
	}
	newHash := sha256.Sum256(data)
	events := w.diff(newCfg)
	w.lastHash = newHash
	return events, nil
}

func (w *Watcher) check() {
	data, err := os.ReadFile(w.path)
	if err != nil {
		log.Printf("watcher: read error: %v", err)
		return
	}
	newHash := sha256.Sum256(data)
	w.mu.Lock()
	defer w.mu.Unlock()
	if newHash == w.lastHash {
		return
	}
	log.Printf("watcher: %s changed, reloading", w.path)
	newCfg, err := Parse(data)
	if err != nil {
		log.Printf("watcher: parse error: %v", err)
		return
	}
	events := w.diff(newCfg)
	w.lastHash = newHash
	if w.onChange != nil && len(events) > 0 {
		w.onChange(events)
	}
}

func (w *Watcher) updateHash() {
	data, err := os.ReadFile(w.path)
	if err != nil {
		return
	}
	w.lastHash = sha256.Sum256(data)
}

// diff compares the new config against the last-known state.
// Since we don't keep the old config in memory (just the hash), we re-parse
// the file and use the new config as ground truth. The caller (Manager)
// applies add/remove accordingly.
func (w *Watcher) diff(newCfg *GatewayConfig) []ChangeEvent {
	var events []ChangeEvent

	// Simple strategy: mark all new providers as "added".
	// The Manager's Bootstrap handles dedup (register is idempotent via name).
	for _, p := range newCfg.Providers {
		if !p.Enabled {
			continue
		}
		events = append(events, ChangeEvent{Action: "added", Provider: p})
	}
	return events
}
