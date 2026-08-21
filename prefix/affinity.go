package prefix

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ── 有界负载亲和路由（基线 §5.4, B-2）──────────────────────────────
// 加权虚拟节点一致哈希环 + 过载溢出 + 溢出短时 sticky（防抖动）;
// 冷启动直接走哈希主节点（不打散最值钱的冷启动缓存）;
// 转发亲和只认一个主键（FP + tenant）。

// AffinityNode 环上的一个 provider（weight 来自 provider.yaml）。
type AffinityNode struct {
	Name   string
	Weight int
}

const vnodesPerWeight = 256

type ringVNode struct {
	hash uint64
	name string
}

// Ring 加权虚拟节点一致哈希环。
type Ring struct {
	vnodes  []ringVNode // 按 hash 升序
	weights map[string]int
	total   int
}

// NewRing 构建环; weight<=0 视为 1。
func NewRing(nodes []AffinityNode) *Ring {
	r := &Ring{weights: make(map[string]int, len(nodes))}
	for _, n := range nodes {
		w := n.Weight
		if w <= 0 {
			w = 1
		}
		r.weights[n.Name] = w
		r.total += w * vnodesPerWeight
		for i := 0; i < w*vnodesPerWeight; i++ {
			r.vnodes = append(r.vnodes, ringVNode{
				hash: hashKey(n.Name + "\x00" + itoa(i)),
				name: n.Name,
			})
		}
	}
	sort.Slice(r.vnodes, func(i, j int) bool { return r.vnodes[i].hash < r.vnodes[j].hash })
	return r
}

// Lookup 返回 key 在环上顺时针首个 provider（无节点时返回 ""）。
func (r *Ring) Lookup(key string) string {
	if r == nil || len(r.vnodes) == 0 {
		return ""
	}
	h := hashKey(key)
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})
	if idx == len(r.vnodes) {
		idx = 0
	}
	return r.vnodes[idx].name
}

func (r *Ring) weightOf(name string) int {
	if r == nil {
		return 1
	}
	if w, ok := r.weights[name]; ok && w > 0 {
		return w
	}
	return 1
}

type overflowEntry struct {
	provider string
	until    time.Time
}

// AffinityRouter 有界负载亲和路由（进程内, 单实例; Redis 实现预留）。
type AffinityRouter struct {
	mu          sync.Mutex
	ring        *Ring
	overload    float64 // 过载判定 c 值: inflight > c * weight
	overflowTTL time.Duration
	inflight    map[string]*atomic.Int64
	overflow    map[string]overflowEntry // key → 溢出 provider + TTL
	overflowCnt atomic.Int64
	primaryCnt  atomic.Int64
	fallbackCnt atomic.Int64
}

// NewAffinityRouter creates a router. overload<=0 → 1.25 默认。
func NewAffinityRouter(nodes []AffinityNode, overload float64, overflowTTL time.Duration) *AffinityRouter {
	if overload <= 0 {
		overload = 1.25
	}
	if overflowTTL <= 0 {
		overflowTTL = 60 * time.Second
	}
	a := &AffinityRouter{
		ring:        NewRing(nodes),
		overload:    overload,
		overflowTTL: overflowTTL,
		inflight:    make(map[string]*atomic.Int64),
		overflow:    make(map[string]overflowEntry),
	}
	return a
}

// SetRing 热更新环（provider 增删/权重变化时重建）。
func (a *AffinityRouter) SetRing(nodes []AffinityNode) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ring = NewRing(nodes)
	a.overflow = make(map[string]overflowEntry)
}

// Decide 选择 provider:
//  1. 命中有效溢出 sticky → 返回（reason=overflow）
//  2. 环上顺时针首个 健康 && 未过载 → 返回（reason=primary）
//  3. 全过载/不健康 → ""（reason=fallback, 调用方回落加权随机）
//
// healthy 回调由 server 提供（active+key+熔断未开）。
func (a *AffinityRouter) Decide(key string, healthy func(name string) bool) (string, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ring == nil {
		a.fallbackCnt.Add(1)
		return "", "fallback"
	}
	now := time.Now()
	if e, ok := a.overflow[key]; ok && now.Before(e.until) &&
		healthy != nil && healthy(e.provider) {
		return e.provider, "overflow"
	}
	delete(a.overflow, key)
	if len(a.ring.vnodes) == 0 {
		a.fallbackCnt.Add(1)
		return "", "fallback"
	}
	// 从 key 位置顺时针遍历, 最多整环一圈
	h := hashKey(key)
	start := sort.Search(len(a.ring.vnodes), func(i int) bool {
		return a.ring.vnodes[i].hash >= h
	})
	lastVisited := ""
	for step := 0; step < len(a.ring.vnodes); step++ {
		idx := (start + step) % len(a.ring.vnodes)
		name := a.ring.vnodes[idx].name
		if name == lastVisited {
			continue // 同 provider 连续虚拟节点去重
		}
		lastVisited = name
		if healthy != nil && !healthy(name) {
			continue
		}
		if a.isOverloaded(name) {
			continue
		}
		a.primaryCnt.Add(1)
		return name, "primary"
	}
	a.fallbackCnt.Add(1)
	return "", "fallback"
}

func (a *AffinityRouter) isOverloaded(name string) bool {
	inf := a.inflight[name]
	if inf == nil {
		return false
	}
	return float64(inf.Load()) > a.overload*float64(a.ring.weightOf(name))
}

// RecordOverflow 在溢出决策生效后记录 sticky（由 server 在 Decide 返回
// overflow 时调用; 让同 key 短时内持续走溢出节点, 防来回抖动）。
func (a *AffinityRouter) RecordOverflow(key, providerName string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.overflow[key] = overflowEntry{provider: providerName, until: time.Now().Add(a.overflowTTL)}
	a.overflowCnt.Add(1)
}

// Inc/Dec 请求进行中的负载计数（server 在选中后/完成后调用）。
func (a *AffinityRouter) Inc(name string) {
	a.mu.Lock()
	inf := a.inflight[name]
	if inf == nil {
		inf = &atomic.Int64{}
		a.inflight[name] = inf
	}
	a.mu.Unlock()
	inf.Add(1)
}

func (a *AffinityRouter) Dec(name string) {
	a.mu.Lock()
	inf := a.inflight[name]
	a.mu.Unlock()
	if inf == nil {
		return
	}
	if v := inf.Add(-1); v < 0 {
		inf.Store(0)
	}
}

func (a *AffinityRouter) Inflight(name string) int64 {
	a.mu.Lock()
	inf := a.inflight[name]
	a.mu.Unlock()
	if inf == nil {
		return 0
	}
	return inf.Load()
}

// PrimaryCount/OverflowCount/FallbackCount 决策分布统计。
func (a *AffinityRouter) PrimaryCount() int64  { return a.primaryCnt.Load() }
func (a *AffinityRouter) OverflowCount() int64 { return a.overflowCnt.Load() }
func (a *AffinityRouter) FallbackCount() int64 { return a.fallbackCnt.Load() }

// Snapshot 观测快照（/v1/stats + diagnostics）。
func (a *AffinityRouter) Snapshot() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	inflight := make(map[string]int64, len(a.inflight))
	for k, v := range a.inflight {
		inflight[k] = v.Load()
	}
	return map[string]any{
		"ring_nodes":      len(a.ring.vnodes),
		"overflow_sticky": len(a.overflow),
		"primary":         a.primaryCnt.Load(),
		"overflow":        a.overflowCnt.Load(),
		"fallback":        a.fallbackCnt.Load(),
		"inflight":        inflight,
	}
}

func hashKey(s string) uint64 {
	// 2026-08-22: FNV-64a 对短串雪崩差（"key-N" 类 key 聚簇 → 环分布
	// 严重倾斜, 单测暴露）。换 sha256 截 8 字节, 可靠雪崩; 构建环一次,
	// Lookup 每请求一次, 开销可忽略。
	h := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(h[:8])
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
