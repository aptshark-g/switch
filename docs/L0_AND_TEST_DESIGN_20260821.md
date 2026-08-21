# D1 L0 缓存子系统 + D5 测试策略（小设计）

> 2026-08-21 · 按 `IMPLEMENTATION_ROADMAP` 阶段 0 前置设计，1~2 页。

## D1：L0 响应缓存子系统

现状：`cache.Cache`（内存 LRU，1000 条 / 5min TTL），键 =
`hash(requestCacheKey, provider|model|api_key|X-Context-Hash)`。

### 接线请求合并（Coalescer）

- 位置：`handleChatCompletions` 缓存 miss 后、上游调用前。
- 语义：`cacheKey` 相同且并发 → 只打一次上游，其余等同一结果。
- 超时：`Do(key, fn, timeout=上游超时)`；fn 内部含重试/降级。
- **只对非流式合并**；流式请求各自独立（避免共享 SSE 流）。
- 合并失败（fn err）→ 所有等待者同时拿到 err，走各自降级。

### 分层 TTL（依赖 B-1 Profiler 热度）

- `hot`（热度前缀）30min / `normal` 5min / `cold` 60s 或不缓存。
- 热度判定：Profiler 近 15min `req_count ≥ N` 或 `hit_tokens ≥ H`。
- **warmup 请求禁写 L0**（`X-DM-Warmup: 1` 直接跳过缓存写）。
- 淘汰：TTL 过期 + 容量上限 LRU；容量从 1000 提到 5000（内存 <1MB）。

### 键与隔离

- 键不变（含 provider/model/api_key/X-Context-Hash）——跨租户已隔离。
- 分层 TTL 只改 `expiresAt`，不改键。

## D5：测试策略（分层 + CI 门槛）

| 层 | 覆盖 | 工具 | 门槛 |
|----|------|------|------|
| 单测 | cache/circuit/coalescer/tenant/prefix/routing/provider | `go test` | 新模块必有测试；主干覆盖率 > 60% |
| 集成 | 全链路 正常/流式/熔断/超时/限流/合并 | mock 上游（`cmd/mockupstream`） | 每个接线 PR 跑通关键场景 |
| 压测 | 吞吐回归（缓存命中/未命中/流式） | `gwbench2` | 关键场景不倒退基线 |
| 混沌 | provider 熔断/超时/限流注入 → 溢出/迁回正确 | 脚本注入 | 演练报告 |

- CI 门槛（本地脚本 `scripts/ci.sh`）：`go build ./...` + `go vet` +
  `go test ./... -race` + 覆盖率汇总。
- 数据隔离：测试不得写生产 `data/`（用 `t.TempDir()` + 环境变量）。
