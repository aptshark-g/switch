package server

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/aptshark/gateway/config"
	"github.com/aptshark/gateway/observability"
	"github.com/aptshark/gateway/prefix"
	"github.com/aptshark/gateway/provider"
)

// ── 智能路由（2026-08-21）────────────────────────────────────────────
// 两层结构（吸收 cortiq-gateway / leyline / RouteLLM / LiteLLM 的成熟模式）:
//   ① 意图/复杂度规则层: X-Intent / X-Complexity header + provider.yaml
//      routing.rules（分类 → 确定性策略, 首个命中生效）;
//   ② 池内加权随机: priority 分层（one-api） × weight × health × latency
//      × cost（LiteLLM 健康过滤 + 设计文档 §2.1）。
// 规则未命中 → 回落池内加权; 规则 provider 不可用 → 保留覆盖回落池。

type routingCandidate struct {
	name     string
	priority int
	score    float64
}

// routeDecision 一次请求的路由决策。
type routeDecision struct {
	provider   string // 空 = 回落池
	model      string // 空 = 沿用请求 model
	thinking   any    // nil = 沿用请求 thinking
	by         string // "rule:<name>" | "weighted" | "none"
	intent     string
	complexity string
	affinityPicked bool // B-2: 命中亲和, 请求完成需 Dec 负载计数
}

// SetRoutingRules 热更新路由规则（watcher 重载 provider.yaml 后调用）。
func (s *Server) SetRoutingRules(rules []config.RoutingRule) {
	s.routingMu.Lock()
	defer s.routingMu.Unlock()
	s.routingRules = rules
}

// routeRequest 智能路由主入口: 规则层优先, 未命中 → 池内加权随机。
func (s *Server) routeRequest(r *http.Request, req *provider.GenerateRequest) routeDecision {
	intent := strings.TrimSpace(r.Header.Get("X-Intent"))
	complexity := strings.TrimSpace(r.Header.Get("X-Complexity"))
	if rule, ok := s.matchRule(intent, complexity, req.Model); ok {
		d, _ := s.applyRule(rule)
		d.intent, d.complexity = intent, complexity
		return d
	}
	d := routeDecision{by: "weighted", intent: intent, complexity: complexity}
	if s.affinity != nil {
		if p, reason := s.affinityPick(r, req); p != "" {
			d.provider = p
			d.by = "affinity:" + reason
			d.affinityPicked = true
		}
	}
	if d.provider == "" {
		d.provider = s.selectFromPool()
	}
	return d
}

// affinityPick B-2: 一致哈希亲和（key = FP + tenant）, 过载溢出走 sticky。
func (s *Server) affinityPick(r *http.Request, req *provider.GenerateRequest) (string, string) {
	s.rebuildAffinityRing()
	tenant := "anon"
	if h := r.Header.Get("Authorization"); len(h) > 7 {
		tenant = h[7:]
	}
	tree := prefix.FingerprintRequest(req)
	key := tree.FP + ":" + tenant
	p, reason := s.affinity.Decide(key, s.affinityHealthy)
	if p != "" && reason == "overflow" {
		s.affinity.RecordOverflow(key, p)
	}
	if p != "" {
		s.affinity.Inc(p)
	}
	return p, reason
}

func (s *Server) affinityHealthy(name string) bool {
	snap, ok := s.providerSnapshot(name)
	return ok && snap.Active && snap.KeyConfigured &&
		snap.Circuit != provider.CircuitOpen
}

// rebuildAffinityRing 环按当前 eligible 集合重建（签名变化才重建）。
func (s *Server) rebuildAffinityRing() {
	eligible := s.poolEligible()
	var sig strings.Builder
	nodes := make([]prefix.AffinityNode, 0, len(eligible))
	names := make([]string, 0, len(eligible))
	wmap := make(map[string]int, len(eligible))
	for _, p := range eligible {
		w := 1
		if cfg, ok := s.manager.Config(p.Name); ok && cfg.Weight > 0 {
			w = cfg.Weight
		}
		wmap[p.Name] = w
		names = append(names, p.Name)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(&sig, "%s:%d,", n, wmap[n])
		nodes = append(nodes, prefix.AffinityNode{Name: n, Weight: wmap[n]})
	}
	s.affinityMu.Lock()
	defer s.affinityMu.Unlock()
	sigStr := sig.String()
	if sigStr != s.affinitySig {
		s.affinitySig = sigStr
		s.affinity.SetRing(nodes)
	}
}

// matchRule 按顺序取首个命中规则; 空匹配字段 = 通配。
func (s *Server) matchRule(intent, complexity, model string) (config.RoutingRule, bool) {
	s.routingMu.RLock()
	rules := s.routingRules
	s.routingMu.RUnlock()
	for _, rule := range rules {
		if !matchList(rule.Match.Intent, intent) {
			continue
		}
		if !matchList(rule.Match.Complexity, complexity) {
			continue
		}
		if rule.Match.Model != "" && rule.Match.Model != model {
			continue
		}
		return rule, true
	}
	return config.RoutingRule{}, false
}

// applyRule 把规则动作转为决策; provider 不可用时保留覆盖、回落池。
func (s *Server) applyRule(rule config.RoutingRule) (routeDecision, bool) {
	d := routeDecision{
		by:       "rule:" + rule.Name,
		model:    rule.Route.Model,
		thinking: rule.Route.Thinking,
	}
	if rule.Route.Provider != "" {
		if snap, ok := s.providerSnapshot(rule.Route.Provider); !ok ||
			!snap.Active || !snap.KeyConfigured {
			d.by += ":provider-unavailable"
			return d, true
		}
		d.provider = rule.Route.Provider
	}
	return d, true
}

func (s *Server) providerSnapshot(name string) (provider.ProviderSnapshot, bool) {
	for _, p := range s.manager.List() {
		if p.Name == name {
			return p, true
		}
	}
	return provider.ProviderSnapshot{}, false
}

// poolEligible 池内候选: 显式路由池成员; 池空 → 全部 active+key。
func (s *Server) poolEligible() []provider.ProviderSnapshot {
	s.poolMutex.RLock()
	inPool := len(s.routingPool) > 0
	poolCopy := make(map[string]bool, len(s.routingPool))
	for k, v := range s.routingPool {
		poolCopy[k] = v
	}
	s.poolMutex.RUnlock()

	var out []provider.ProviderSnapshot
	for _, p := range s.manager.List() {
		if !p.Active || !p.KeyConfigured {
			continue
		}
		if inPool && !poolCopy[p.Name] {
			continue
		}
		out = append(out, p)
	}
	return out
}

// selectFromPool 池内加权随机: 熔断 OPEN 不进首选（全挂时退化）;
// 最高 priority 层内按 score 加权随机。
func (s *Server) selectFromPool() string {
	eligible := s.poolEligible()
	if len(eligible) == 0 {
		return ""
	}
	pick := eligible
	var healthy []provider.ProviderSnapshot
	for _, p := range eligible {
		if p.Circuit != provider.CircuitOpen {
			healthy = append(healthy, p)
		}
	}
	if len(healthy) > 0 {
		pick = healthy
	}

	sort.Slice(pick, func(i, j int) bool {
		if pick[i].Priority != pick[j].Priority {
			return pick[i].Priority > pick[j].Priority
		}
		return pick[i].Name < pick[j].Name
	})
	tier := pick[0].Priority
	var tierSet []provider.ProviderSnapshot
	for _, p := range pick {
		if p.Priority == tier {
			tierSet = append(tierSet, p)
		}
	}

	minInput := s.minInputPrice(tierSet)
	cands := make([]routingCandidate, 0, len(tierSet))
	for _, p := range tierSet {
		cfg, _ := s.manager.Config(p.Name)
		cands = append(cands, routingCandidate{
			name:     p.Name,
			priority: p.Priority,
			score:    s.effectiveWeight(p, cfg, minInput),
		})
	}
	return weightedPick(cands)
}

// routingCandidates 降级候选链（确定性偏好序: 熔断靠后, 同状态按
// priority desc → score desc → 名字）; 替代原 map 随机序（真实 bug:
// 降级顺序此前不稳定）。
func (s *Server) routingCandidates() []string {
	eligible := s.poolEligible()
	if len(eligible) == 0 {
		return nil
	}
	minInput := s.minInputPrice(eligible)
	sort.SliceStable(eligible, func(i, j int) bool {
		ci := eligible[i].Circuit == provider.CircuitOpen
		cj := eligible[j].Circuit == provider.CircuitOpen
		if ci != cj {
			return !ci
		}
		if eligible[i].Priority != eligible[j].Priority {
			return eligible[i].Priority > eligible[j].Priority
		}
		cfgI, _ := s.manager.Config(eligible[i].Name)
		cfgJ, _ := s.manager.Config(eligible[j].Name)
		si := s.effectiveWeight(eligible[i], cfgI, minInput)
		sj := s.effectiveWeight(eligible[j], cfgJ, minInput)
		if si != sj {
			return si > sj
		}
		return eligible[i].Name < eligible[j].Name
	})
	out := make([]string, len(eligible))
	for i, p := range eligible {
		out[i] = p.Name
	}
	return out
}

// effectiveWeight 有效权重（设计 §2.1: weight = health × latency × cost × weight）:
//
//	health: 探测不健康 → 0.1（保底 10% 流量）; 健康/未知 → 1
//	latency: 1 - p50/5000 钳制 [0.05, 1]; 无数据 → 1
//	cost: 层内最便宜 input 价 / 本家 input 价 钳制 [0.1, 1]; 无 pricing → 1
func (s *Server) effectiveWeight(snap provider.ProviderSnapshot,
	cfg provider.ProviderConfig, tierMinInput float64) float64 {
	w := float64(cfg.Weight)
	if w <= 0 {
		w = 1
	}
	hs := s.getHealth(snap.Name)
	health := 1.0
	if hs != nil && !hs.Healthy {
		health = 0.1
	}
	lat := 1.0
	if hs != nil && hs.LatencyMs > 0 {
		lat = 1.0 - float64(hs.LatencyMs)/5000.0
		if lat < 0.05 {
			lat = 0.05
		}
		if lat > 1 {
			lat = 1
		}
	}
	cost := 1.0
	if tierMinInput > 0 && cfg.Pricing != nil && cfg.Pricing.InputPrice > 0 {
		c := tierMinInput / cfg.Pricing.InputPrice
		if c < 0.1 {
			c = 0.1
		}
		if c > 1 {
			c = 1
		}
		cost = c
	}
	return w * health * lat * cost
}

func (s *Server) minInputPrice(ps []provider.ProviderSnapshot) float64 {
	var m float64
	found := false
	for _, p := range ps {
		cfg, ok := s.manager.Config(p.Name)
		if !ok || cfg.Pricing == nil || cfg.Pricing.InputPrice <= 0 {
			continue
		}
		if !found || cfg.Pricing.InputPrice < m {
			m = cfg.Pricing.InputPrice
			found = true
		}
	}
	if !found {
		return 0
	}
	return m
}

func (s *Server) getHealth(name string) *provider.HealthStatus {
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	return s.healthCache[name]
}

func weightedPick(cands []routingCandidate) string {
	if len(cands) == 0 {
		return ""
	}
	total := 0.0
	for _, c := range cands {
		total += c.score
	}
	if total <= 0 {
		return cands[rand.IntN(len(cands))].name
	}
	x := rand.Float64() * total
	for _, c := range cands {
		x -= c.score
		if x < 0 {
			return c.name
		}
	}
	return cands[len(cands)-1].name
}

// matchList 规则值支持逗号分隔多值（任一命中）; 空 = 通配。
func matchList(ruleVal, requestVal string) bool {
	if ruleVal == "" {
		return true
	}
	r := strings.ToLower(strings.TrimSpace(requestVal))
	for _, part := range strings.Split(ruleVal, ",") {
		if strings.ToLower(strings.TrimSpace(part)) == r {
			return true
		}
	}
	return false
}

// logRouting 决策观测（尊重 DM_GATEWAY_REQUEST_LOG=0 开关, 错误路径总记）。
func (s *Server) logRouting(r *http.Request, d routeDecision, providerName string) {
	if os.Getenv("DM_GATEWAY_REQUEST_LOG") == "0" {
		return
	}
	s.logger.Info(observability.LogEntry{
		RequestID: observability.GetRequestID(r.Context()),
		Provider:  providerName,
		Msg: "routing: " + d.by +
			" intent=" + d.intent +
			" complexity=" + d.complexity,
	})
}
