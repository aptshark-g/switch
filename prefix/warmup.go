package prefix

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/aptshark/gateway/provider"
)

// ── WarmScheduler 心跳预热（基线 §5.2, B-4）────────────────────────
// 触发（同时满足）: 热度（近15min req ≥ N 或 hit_tokens ≥ H）+
// 空闲（距上次真实请求 ≥ idleAfter）+ 配额允许 + 亲和节点可达。
// 预热请求形态: 复用最后消息前缀 + 固定 tail token, max_tokens=1;
// 只发当前亲和节点; 不写 L0; 迟到预热（cached_tokens=0）计入 warm_late。

// WarmupConfig 预热配置。
type WarmupConfig struct {
	Enabled          bool
	TriggerReq       int64         // 近15min req_count ≥ N
	TriggerHitTokens int64         // 或 hit_tokens 累计 ≥ H
	IdleAfter        time.Duration // 距上次真实请求 ≥ 此值才预热（TTL×0.45）
	TailToken        string        // P4 固定尾巴
	GlobalCapRatio   float64       // 预热 input / 全站 input（按全价最坏场景）
	MaxWarmPerTick   int
}

// WarmScheduler 预热调度器（进程内; Redis 队列预留）。
type WarmScheduler struct {
	cfg   WarmupConfig
	prof  *Profiler
	quota *QuotaCoordinator
	// affinityOf: (fp, tenant) → provider; 只预热当前亲和节点（基线原则 5）。
	affinityOf func(fp, tenant string) string
	// generate: 直接上游调用（不经 HTTP 层, 不写 L0/不计数业务指标）。
	generate func(providerName string, req *provider.GenerateRequest) (*provider.GenerateResponse, error)
	// totalInputOf: 全站 input tokens（全局帽校验）。
	totalInputOf func() int64

	warmRequests    atomic.Int64
	warmLate        atomic.Int64
	warmInputTokens atomic.Int64
}

// NewWarmScheduler builds a scheduler (quota 可 nil → 不限）。
func NewWarmScheduler(cfg WarmupConfig, prof *Profiler, quota *QuotaCoordinator,
	affinityOf func(fp, tenant string) string,
	generate func(string, *provider.GenerateRequest) (*provider.GenerateResponse, error),
	totalInputOf func() int64) *WarmScheduler {
	if cfg.TailToken == "" {
		cfg.TailToken = "."
	}
	if cfg.MaxWarmPerTick <= 0 {
		cfg.MaxWarmPerTick = 20
	}
	if cfg.GlobalCapRatio <= 0 {
		cfg.GlobalCapRatio = 0.02
	}
	return &WarmScheduler{
		cfg:          cfg,
		prof:         prof,
		quota:        quota,
		affinityOf:   affinityOf,
		generate:     generate,
		totalInputOf: totalInputOf,
	}
}

// Start 周期执行 Tick。
func (w *WarmScheduler) Start(interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go func() {
		for range time.Tick(interval) {
			if err := w.Tick(); err != nil {
				log.Printf("warmup: tick: %v", err)
			}
		}
	}()
}

// Tick 一轮预热: 从 Profiler 扫热前缀, 满足触发条件则预热。
func (w *WarmScheduler) Tick() error {
	if w == nil || !w.cfg.Enabled || w.affinityOf == nil || w.generate == nil {
		return nil
	}
	now := time.Now()
	candidates := w.prof.Top(w.cfg.MaxWarmPerTick * 10)
	warmed := 0
	for _, rec := range candidates {
		if warmed >= w.cfg.MaxWarmPerTick {
			break
		}
		if !w.hotEnough(rec) {
			continue
		}
		if now.Sub(rec.LastAccess) < w.cfg.IdleAfter {
			continue // 真实流量还在, 无需预热
		}
		if len(rec.LastMessages) == 0 {
			continue // 无载荷（warmup 未启用时 Profiler 不存内容）
		}
		if !w.withinGlobalCap() {
			return nil // 全局帽超限 → 本轮停止
		}
		providerName := w.affinityOf(rec.FP, rec.Tenant)
		if providerName == "" {
			continue
		}
		if w.quota != nil && !w.quota.WarmAllowed(providerName, rec.FP) {
			continue
		}
		if err := w.warmOne(providerName, rec); err != nil {
			log.Printf("warmup: %s err: %v", providerName, err)
			continue
		}
		warmed++
	}
	return nil
}

func (w *WarmScheduler) hotEnough(rec Record) bool {
	if rec.ReqCount >= w.cfg.TriggerReq {
		return true
	}
	if w.cfg.TriggerHitTokens > 0 && rec.HitTokens >= w.cfg.TriggerHitTokens {
		return true
	}
	return false
}

func (w *WarmScheduler) withinGlobalCap() bool {
	total := int64(0)
	if w.totalInputOf != nil {
		total = w.totalInputOf()
	}
	if total <= 0 {
		return true
	}
	return float64(w.warmInputTokens.Load()) <= w.cfg.GlobalCapRatio*float64(total)
}

// warmOne 构造并发送最小预热请求。
func (w *WarmScheduler) warmOne(providerName string, rec Record) error {
	msgs := make([]provider.Message, len(rec.LastMessages))
	for i, m := range rec.LastMessages {
		msgs[i] = m
	}
	// 最后一条替换为固定尾巴（不改变前缀字节）。
	if len(msgs) > 0 {
		last := msgs[len(msgs)-1]
		last.Content = w.cfg.TailToken
		msgs[len(msgs)-1] = last
	}
	req := &provider.GenerateRequest{
		Messages:  msgs,
		MaxTokens: 1,
		Thinking:  map[string]any{"type": "disabled"},
	}
	resp, err := w.generate(providerName, req)
	if err != nil {
		return err
	}
	w.warmRequests.Add(1)
	if resp.Usage != nil {
		w.warmInputTokens.Add(int64(resp.Usage.PromptTokens))
		// 迟到预热: cached=0 → 全价 prefill（预算按最坏场景已含）
		if resp.Usage.Cached() == 0 {
			w.warmLate.Add(1)
		}
	}
	return nil
}

// Counters 观测。
func (w *WarmScheduler) Counters() map[string]int64 {
	return map[string]int64{
		"requests":     w.warmRequests.Load(),
		"late":         w.warmLate.Load(),
		"input_tokens": w.warmInputTokens.Load(),
	}
}

// Quota 暴露配额协调器（server 真实请求优先级用）。
func (w *WarmScheduler) Quota() *QuotaCoordinator { return w.quota }

// TriggerReq 暴露热度阈值（启动日志用）。
func (w *WarmScheduler) TriggerReq() int64 { return w.cfg.TriggerReq }
