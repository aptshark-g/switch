package config

import (
	"sync"
	"time"
)

// ── Canary: progressive config rollout ──

// CanaryStage represents one phase of gradual rollout.
type CanaryStage struct {
	TrafficPercent float64       // percentage of traffic (0.0-1.0)
	Duration       time.Duration // how long to stay at this stage
}

// DefaultCanaryStages returns a standard 4-stage rollout.
func DefaultCanaryStages() []CanaryStage {
	return []CanaryStage{
		{TrafficPercent: 0.01, Duration: 5 * time.Minute},  // 1%
		{TrafficPercent: 0.10, Duration: 5 * time.Minute},  // 10%
		{TrafficPercent: 0.30, Duration: 10 * time.Minute}, // 30%
		{TrafficPercent: 1.0, Duration: 0},                  // 100% (final)
	}
}

// CanaryState tracks the current rollout phase.
type CanaryState int

const (
	CanaryIdle      CanaryState = iota // not rolling out
	CanaryRolling                      // actively rolling out
	CanaryPaused                       // paused due to errors
	CanaryRolledBack                   // aborted
	CanaryComplete                     // fully deployed
)

func (s CanaryState) String() string {
	switch s {
	case CanaryIdle: return "idle"
	case CanaryRolling: return "rolling"
	case CanaryPaused: return "paused"
	case CanaryRolledBack: return "rolled_back"
	case CanaryComplete: return "complete"
	default: return "unknown"
	}
}

// CanaryRollout manages progressive deployment of a config change.
type CanaryRollout struct {
	mu       sync.Mutex
	state    CanaryState
	stages   []CanaryStage
	current  int // current stage index
	started  time.Time
	stageAt  time.Time // when current stage began

	// Error thresholds
	maxErrors      int
	errorCount     int
	errorWindow    time.Duration
	lastErrorCheck time.Time
}

// NewCanaryRollout creates a canary with custom stages.
func NewCanaryRollout(stages []CanaryStage) *CanaryRollout {
	return &CanaryRollout{
		state:       CanaryIdle,
		stages:      stages,
		errorWindow: 1 * time.Minute,
	}
}

// Start begins a new rollout.
func (cr *CanaryRollout) Start() {
	cr.mu.Lock(); defer cr.mu.Unlock()
	cr.state = CanaryRolling
	cr.current = 0
	cr.errorCount = 0
	cr.started = time.Now()
	cr.stageAt = time.Now()
}

// TrafficPercent returns the current traffic percentage (0.0-1.0).
// Returns 1.0 when not rolling out.
func (cr *CanaryRollout) TrafficPercent() float64 {
	cr.mu.Lock(); defer cr.mu.Unlock()
	if cr.state != CanaryRolling || cr.current >= len(cr.stages) {
		if cr.state == CanaryComplete { return 1.0 }
		return 1.0
	}
	return cr.stages[cr.current].TrafficPercent
}

// Advance moves to the next stage if the current one's duration has elapsed.
func (cr *CanaryRollout) Advance() {
	cr.mu.Lock(); defer cr.mu.Unlock()
	if cr.state != CanaryRolling {
		return
	}
	stage := cr.stages[cr.current]
	if stage.Duration > 0 && time.Since(cr.stageAt) < stage.Duration {
		return // not yet
	}
	cr.current++
	if cr.current >= len(cr.stages) {
		cr.state = CanaryComplete
		return
	}
	cr.stageAt = time.Now()
}

// RecordError tracks an error during rollout. If errors exceed threshold, pause.
func (cr *CanaryRollout) RecordError() {
	cr.mu.Lock(); defer cr.mu.Unlock()
	if cr.state != CanaryRolling {
		return
	}
	cr.errorCount++

	// Reset error counter periodically
	if time.Since(cr.lastErrorCheck) > cr.errorWindow {
		cr.errorCount = 0
		cr.lastErrorCheck = time.Now()
	}

	// Pause if too many errors in current window
	if cr.errorCount >= 3 {
		cr.state = CanaryPaused
	}
}

// Rollback aborts the rollout.
func (cr *CanaryRollout) Rollback() {
	cr.mu.Lock(); defer cr.mu.Unlock()
	cr.state = CanaryRolledBack
}

// Resume continues a paused rollout (operator decision).
func (cr *CanaryRollout) Resume() {
	cr.mu.Lock(); defer cr.mu.Unlock()
	if cr.state == CanaryPaused {
		cr.state = CanaryRolling
		cr.errorCount = 0
		cr.stageAt = time.Now()
	}
}

// State returns the current rollout state.
func (cr *CanaryRollout) State() CanaryState {
	cr.mu.Lock(); defer cr.mu.Unlock()
	return cr.state
}

// Snapshot returns diagnostic info.
func (cr *CanaryRollout) Snapshot() map[string]any {
	cr.mu.Lock(); defer cr.mu.Unlock()
	info := map[string]any{
		"state":           cr.state.String(),
		"traffic_percent": 1.0,
		"stage":           cr.current,
		"total_stages":    len(cr.stages),
		"elapsed":         time.Since(cr.started).String(),
		"errors":          cr.errorCount,
	}
	if cr.state == CanaryRolling && cr.current < len(cr.stages) {
		info["traffic_percent"] = cr.stages[cr.current].TrafficPercent
	}
	return info
}
