// Package persistence provides JSON file-based state persistence for the Gateway.
// On startup, it restores provider configs, usage stats, and circuit breaker state.
// On shutdown (graceful), it snapshots all state to disk. Periodic auto-save runs
// every 5 minutes as a safety net against unexpected crashes.
package persistence

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// State is the on-disk representation of all persisted Gateway state.
type State struct {
	Version       int                          `json:"version"`
	SavedAt       time.Time                    `json:"saved_at"`
	Providers     []ProviderState              `json:"providers"`
	Usage         map[string]UsageState        `json:"usage"`
	Cache         map[string]CacheEntry        `json:"cache,omitempty"`
}

// ProviderState is the persisted form of a single provider.
type ProviderState struct {
	Name         string   `json:"name"`
	Kind         string   `json:"kind"`
	BaseURL      string   `json:"base_url"`
	APIKey       string   `json:"api_key"`
	Models       []string `json:"models"`
	Enabled      bool     `json:"enabled"`
	Requests     int64    `json:"requests"`
	Errors       int64    `json:"errors"`
	TokenPrompt  int64    `json:"token_prompt"`
	TokenComp    int64    `json:"token_completion"`
}

// UsageState is the persisted usage for a provider.
type UsageState struct {
	Requests int64 `json:"requests"`
	Tokens   int64 `json:"tokens"`
}

// CacheEntry is a serialisable cache item.
type CacheEntry struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"` // JSON-encoded response
	ExpiresAt time.Time `json:"expires_at"`
}

// Store handles JSON file persistence.
type Store struct {
	mu       sync.Mutex
	path     string
	interval time.Duration
	stopCh   chan struct{}
	dirties  func() *State // produces current state on demand
	applyFn  func(*State)  // restores state after load
}

// NewStore creates a persistence store.
// path is the JSON file path.
// dirties is called on save — it should return the current live state.
// applyFn is called after load — it should restore the state into the runtime.
func NewStore(path string, dirties func() *State, applyFn func(*State)) *Store {
	return &Store{
		path:     path,
		interval: 5 * time.Minute,
		stopCh:   make(chan struct{}),
		dirties:  dirties,
		applyFn:  applyFn,
	}
}

// Restore reads the persisted state from disk. Returns nil if the file
// does not exist (first run).
func (s *Store) Restore() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		log.Printf("persistence: no state file at %s (fresh start)", s.path)
		return nil
	}
	if err != nil {
		return fmt.Errorf("persistence: read %s: %w", s.path, err)
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("persistence: parse %s: %w", s.path, err)
	}

	log.Printf("persistence: restored state from %s (saved %s, %d providers)",
		s.path, state.SavedAt.Format(time.RFC3339), len(state.Providers))

	if s.applyFn != nil {
		s.applyFn(&state)
	}
	return nil
}

// Save snapshots the current state to disk.
func (s *Store) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dirties == nil {
		return fmt.Errorf("persistence: no dirties function configured")
	}

	state := s.dirties()
	state.Version = 1
	state.SavedAt = time.Now()

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("persistence: marshal: %w", err)
	}

	// Atomic write: temp file + rename.
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return fmt.Errorf("persistence: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("persistence: rename %s -> %s: %w", tmp, s.path, err)
	}

	log.Printf("persistence: saved state to %s (%d bytes)", s.path, len(data))
	return nil
}

// StartAutoSave begins periodically saving state.
func (s *Store) StartAutoSave() {
	log.Printf("persistence: auto-save every %s to %s", s.interval, s.path)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.Save(); err != nil {
				log.Printf("persistence: auto-save error: %v", err)
			}
		case <-s.stopCh:
			return
		}
	}
}

// Stop terminates the auto-save loop.
func (s *Store) Stop() {
	close(s.stopCh)
}
