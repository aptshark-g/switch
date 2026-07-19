package provider

import (
	"math/rand/v2"
	"sync"
)

// ── Weighted Router: health × latency × cost × priority ──

// WeightedRouter selects providers using health-aware weighted random selection.
type WeightedRouter struct {
	mu          sync.RWMutex
	weights     map[string]*ProviderWeight
	strategy    RoutingStrategy
}

// ProviderWeight holds dynamic scoring factors for a provider.
type ProviderWeight struct {
	Name          string
	HealthScore   float64 // 0.0 (dead) → 1.0 (perfect)
	LatencyScore  float64 // 0.0 (slow) → 1.0 (fast)
	CostWeight    float64 // 0.0 (expensive) → 1.0 (free)
	Priority      float64 // manual priority from config (0.0-1.0)
	CurrentWeight float64 // combined weight (recalculated)
}

// NewWeightedRouter creates a weighted router with the given strategy.
func NewWeightedRouter(strategy RoutingStrategy) *WeightedRouter {
	return &WeightedRouter{
		weights:  make(map[string]*ProviderWeight),
		strategy: strategy,
	}
}

// UpdateHealth adjusts a provider's health score.
func (wr *WeightedRouter) UpdateHealth(name string, isHealthy bool) {
	wr.mu.Lock(); defer wr.mu.Unlock()
	w := wr.ensure(name)
	if isHealthy {
		w.HealthScore += 0.01 // slow recovery
	} else {
		w.HealthScore -= 0.1 // fast penalty (health_decay)
	}
	w.HealthScore = clamp(w.HealthScore, 0.01, 1.0)
	wr.recalc(w)
}

// UpdateLatency adjusts a provider's latency score based on current RTT.
func (wr *WeightedRouter) UpdateLatency(name string, rttMs, baselineMs float64) {
	wr.mu.Lock(); defer wr.mu.Unlock()
	w := wr.ensure(name)
	if baselineMs > 0 && rttMs > 0 {
		// Score = 1.0 when rtt == baseline; approaches 0 as rtt diverges
		ratio := baselineMs / rttMs
		w.LatencyScore = clamp(ratio, 0.1, 1.0)
	}
	wr.recalc(w)
}

// SetCostWeight sets a provider's cost weight (0=expensive, 1=free).
func (wr *WeightedRouter) SetCostWeight(name string, costWeight float64) {
	wr.mu.Lock(); defer wr.mu.Unlock()
	w := wr.ensure(name)
	w.CostWeight = clamp(costWeight, 0.1, 1.0)
	wr.recalc(w)
}

// SetPriority sets manual priority override.
func (wr *WeightedRouter) SetPriority(name string, priority float64) {
	wr.mu.Lock(); defer wr.mu.Unlock()
	w := wr.ensure(name)
	w.Priority = clamp(priority, 0.0, 1.0)
	wr.recalc(w)
}

// Enable enables a provider for routing.
func (wr *WeightedRouter) Enable(name string) {
	wr.mu.Lock(); defer wr.mu.Unlock()
	w := wr.ensure(name)
	w.HealthScore = 1.0
	wr.recalc(w)
}

// Disable removes a provider from routing (weight=0).
func (wr *WeightedRouter) Disable(name string) {
	wr.mu.Lock(); defer wr.mu.Unlock()
	if w, ok := wr.weights[name]; ok {
		w.HealthScore = 0
		w.CurrentWeight = 0
	}
}

// Select picks a provider using weighted random selection.
// Returns empty string if no provider is available.
func (wr *WeightedRouter) Select(exclude ...string) string {
	wr.mu.RLock(); defer wr.mu.RUnlock()

	excludeSet := make(map[string]bool)
	for _, e := range exclude { excludeSet[e] = true }

	type candidate struct {
		name   string
		weight float64
	}
	var candidates []candidate
	var totalWeight float64

	for name, w := range wr.weights {
		if excludeSet[name] || w.CurrentWeight <= 0 {
			continue
		}
		candidates = append(candidates, candidate{name, w.CurrentWeight})
		totalWeight += w.CurrentWeight
	}

	if totalWeight <= 0 || len(candidates) == 0 {
		return ""
	}

	// Weighted random: pick a random point in [0, totalWeight)
	r := rand.Float64() * totalWeight
	var cumulative float64
	for _, c := range candidates {
		cumulative += c.weight
		if r <= cumulative {
			return c.name
		}
	}
	return candidates[len(candidates)-1].name
}

// SelectAll returns all providers sorted by weight, highest first.
func (wr *WeightedRouter) SelectAll() []string {
	wr.mu.RLock(); defer wr.mu.RUnlock()
	type namedWeight struct {
		name   string
		weight float64
	}
	var list []namedWeight
	for n, w := range wr.weights {
		if w.CurrentWeight > 0 {
			list = append(list, namedWeight{n, w.CurrentWeight})
		}
	}
	// Sort descending
	for i := 0; i < len(list); i++ {
		for j := i + 1; j < len(list); j++ {
			if list[j].weight > list[i].weight {
				list[i], list[j] = list[j], list[i]
			}
		}
	}
	result := make([]string, len(list))
	for i, nw := range list { result[i] = nw.name }
	return result
}

// AllWeights returns snapshots of all provider weights for diagnostics.
func (wr *WeightedRouter) AllWeights() map[string]ProviderWeight {
	wr.mu.RLock(); defer wr.mu.RUnlock()
	out := make(map[string]ProviderWeight, len(wr.weights))
	for k, v := range wr.weights {
		out[k] = *v
	}
	return out
}

func (wr *WeightedRouter) ensure(name string) *ProviderWeight {
	if w, ok := wr.weights[name]; ok { return w }
	w := &ProviderWeight{
		Name:         name,
		HealthScore:  1.0,
		LatencyScore: 1.0,
		CostWeight:   0.5,
		Priority:     0.5,
	}
	wr.weights[name] = w
	wr.recalc(w)
	return w
}

func (wr *WeightedRouter) recalc(w *ProviderWeight) {
	// Combined: geometric mean of all scores
	// Priority gets double weight (×2 multipliers)
	multipliers := w.HealthScore * w.LatencyScore * w.CostWeight * w.Priority * w.Priority
	w.CurrentWeight = clamp(multipliers, 0.0, 1.0)
}

func clamp(v, lo, hi float64) float64 {
	if v < lo { return lo }
	if v > hi { return hi }
	return v
}
