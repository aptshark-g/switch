package observability

import (
	"math"
	"sync"
	"time"
)

// ── SLO Burn Rate Calculator ──
//
// Reference: Google SRE Workbook, Chapter 5
// Burn rate = current error rate / SLO-budgeted error rate
// Alerts fire when burn rate exhausts the error budget too quickly.

// SLO defines the service level objective.
type SLO struct {
	TargetSuccessRate float64       // e.g. 0.995 (99.5%)
	Window            time.Duration // evaluation window (default 30 days)
}

// SLOConfig holds SLO parameters and alert thresholds.
type SLOConfig struct {
	SLO
	ShortWindow  time.Duration // 1h window for fast burn detection
	LongWindow   time.Duration // 6h window for sustained burn
	PageThreshold float64      // burn rate > this → page (default 14.4)
	TicketThreshold float64    // burn rate > this → ticket (default 6.0)
}

// DefaultSLOConfig returns Google SRE-recommended defaults.
func DefaultSLOConfig() SLOConfig {
	return SLOConfig{
		SLO: SLO{
			TargetSuccessRate: 0.995,
			Window:            30 * 24 * time.Hour,
		},
		ShortWindow:     1 * time.Hour,
		LongWindow:      6 * time.Hour,
		PageThreshold:   14.4, // 2h to exhaust 30d budget
		TicketThreshold: 6.0,  // ~5h to exhaust 30d budget
	}
}

// SLOMonitor tracks error budget and burn rate over sliding windows.
type SLOMonitor struct {
	cfg SLOConfig
	mu  sync.Mutex

	// Sliding window buckets — each bucket holds success/fail counts for a time slice
	shortBuckets []sloBucket
	longBuckets  []sloBucket
	shortIdx     int
	longIdx      int
	bucketSize   time.Duration
	lastAdvance  time.Time

	// Current state
	lastBurnRate float64
	lastAlert    SLOAlertLevel

	// Callbacks
	onAlert func(SLOAlert)
}

type sloBucket struct {
	successes int64
	failures  int64
}

// SLOAlertLevel indicates severity.
type SLOAlertLevel int

const (
	SLOAlertNone   SLOAlertLevel = iota
	SLOAlertTicket
	SLOAlertPage
)

func (l SLOAlertLevel) String() string {
	switch l {
	case SLOAlertTicket: return "ticket"
	case SLOAlertPage: return "page"
	default: return "none"
	}
}

// SLOAlert is emitted when burn rate exceeds thresholds.
type SLOAlert struct {
	Level     SLOAlertLevel `json:"level"`
	BurnRate  float64       `json:"burn_rate"`
	ErrorRate float64       `json:"error_rate"`
	ShortRate float64       `json:"short_window_rate"`
	LongRate  float64       `json:"long_window_rate"`
	BudgetRemaining float64 `json:"budget_remaining"` // fraction of 30d budget left
	Timestamp time.Time     `json:"timestamp"`
}

// NewSLOMonitor creates an SLO monitor with the given config.
func NewSLOMonitor(cfg SLOConfig) *SLOMonitor {
	shortBuckets := 12 // 1h / 5min = 12 buckets
	longBuckets := 36  // 6h / 10min = 36 buckets
	return &SLOMonitor{
		cfg:          cfg,
		shortBuckets: make([]sloBucket, shortBuckets),
		longBuckets:  make([]sloBucket, longBuckets),
		bucketSize:   5 * time.Minute,
		lastAdvance:  time.Now(),
	}
}

// RecordSuccess records a successful request.
func (sm *SLOMonitor) RecordSuccess() {
	sm.record(true)
}

// RecordFailure records a failed request.
func (sm *SLOMonitor) RecordFailure() {
	sm.record(false)
}

func (sm *SLOMonitor) record(success bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.advance()

	if success {
		sm.shortBuckets[sm.shortIdx].successes++
		sm.longBuckets[sm.longIdx].successes++
	} else {
		sm.shortBuckets[sm.shortIdx].failures++
		sm.longBuckets[sm.longIdx].failures++
	}

	// Check burn rate every 30 records (approximate)
	now := time.Now()
	if now.Unix()%30 == 0 {
		sm.evaluate()
	}
}

func (sm *SLOMonitor) advance() {
	now := time.Now()
	elapsed := now.Sub(sm.lastAdvance)
	if elapsed < sm.bucketSize {
		return
	}
	steps := int(elapsed / sm.bucketSize)
	if steps > len(sm.shortBuckets) {
		steps = len(sm.shortBuckets)
	}
	for i := 0; i < steps; i++ {
		sm.shortIdx = (sm.shortIdx + 1) % len(sm.shortBuckets)
		sm.shortBuckets[sm.shortIdx] = sloBucket{}
		sm.longIdx = (sm.longIdx + 1) % len(sm.longBuckets)
		sm.longBuckets[sm.longIdx] = sloBucket{}
	}
	sm.lastAdvance = now
}

func (sm *SLOMonitor) evaluate() {
	// Aggregate windows
	shortTotal, shortFail := sm.aggregate(sm.shortBuckets)
	longTotal, longFail := sm.aggregate(sm.longBuckets)

	// Current error rates
	shortRate := 0.0
	if shortTotal > 0 {
		shortRate = float64(shortFail) / float64(shortTotal)
	}
	longRate := 0.0
	if longTotal > 0 {
		longRate = float64(longFail) / float64(longTotal)
	}

	// Use the higher of short/long for conservatism
	currentErrorRate := math.Max(shortRate, longRate)
	
	// Burn rate = current error rate / budgeted error rate
	budgetedErrorRate := 1.0 - sm.cfg.TargetSuccessRate
	burnRate := 0.0
	if budgetedErrorRate > 0 {
		burnRate = currentErrorRate / budgetedErrorRate
	}

	sm.lastBurnRate = burnRate

	// Calculate remaining budget
	totalRequests := shortTotal + longTotal
	totalBudget := float64(totalRequests) * budgetedErrorRate
	totalErrors := float64(shortFail + longFail)
	budgetRemaining := 1.0
	if totalBudget > 0 {
		budgetRemaining = 1.0 - totalErrors/totalBudget
	}
	budgetRemaining = math.Max(0, budgetRemaining)

	// Alert evaluation
	var level SLOAlertLevel
	switch {
	case burnRate >= sm.cfg.PageThreshold:
		level = SLOAlertPage
	case burnRate >= sm.cfg.TicketThreshold:
		level = SLOAlertTicket
	default:
		level = SLOAlertNone
	}

	if level != SLOAlertNone && level != sm.lastAlert {
		alert := SLOAlert{
			Level:           level,
			BurnRate:        burnRate,
			ErrorRate:       currentErrorRate,
			ShortRate:       shortRate,
			LongRate:        longRate,
			BudgetRemaining: budgetRemaining,
			Timestamp:       time.Now(),
		}
		if sm.onAlert != nil {
			sm.onAlert(alert)
		}
	}
	sm.lastAlert = level
}

func (sm *SLOMonitor) aggregate(buckets []sloBucket) (total, fails int64) {
	for _, b := range buckets {
		total += b.successes + b.failures
		fails += b.failures
	}
	return
}

// OnAlert registers a callback for SLO threshold violations.
func (sm *SLOMonitor) OnAlert(fn func(SLOAlert)) {
	sm.mu.Lock(); defer sm.mu.Unlock()
	sm.onAlert = fn
}

// Snapshot returns the current SLO status.
func (sm *SLOMonitor) Snapshot() SLOAlert {
	sm.mu.Lock(); defer sm.mu.Unlock()
	shortTotal, shortFail := sm.aggregate(sm.shortBuckets)
	longTotal, longFail := sm.aggregate(sm.longBuckets)
	shortRate := 0.0
	if shortTotal > 0 { shortRate = float64(shortFail) / float64(shortTotal) }
	longRate := 0.0
	if longTotal > 0 { longRate = float64(longFail) / float64(longTotal) }
	return SLOAlert{
		Level:     sm.lastAlert,
		BurnRate:  sm.lastBurnRate,
		ShortRate: shortRate,
		LongRate:  longRate,
		Timestamp: time.Now(),
	}
}
