# switch 实现任务路径（去贴金 → 生产级 → 差异化）

> 2026-08-21 · 依据 `PRODUCTION_GAP_AUDIT_20260821.md`（差距）与
> `CLOUD_CACHE_DESIGN_BASELINE_20260821.md`（缓存设计）。
> 每条任务标注：设计是否就绪 / 依赖 / 完成判据（DoD）。

---

## 〇、设计充分性结论

**缓存差异化面（B-1~B-5）：设计就绪**（基线 v1.2 有机制/数据模型/接口/配置）。
**生产级清账面（P0/P1）：6 块缺设计**，实现前必须先出小设计：

| # | 缺口设计 | 影响的任务 |
|---|----------|-----------|
| D1 | L0 缓存子系统（coalescer 接线语义 / 淘汰策略 / 分层 TTL / 预热禁写） | 0.1-C, 2.5 |
| D2 | per-key/per-model 限流接线（MultiRateLimiter 状态机 + 配置 + 热更新） | 0.1-A |
| D3 | 预算/告警（额度模型 / 周期 / 超限动作 / 告警渠道） | 1.1 |
| D4 | 重试与降级（jitter 退避 / RetryBudget 生效 / 上下文窗口降级） | 1.2 |
| D5 | 测试策略（单测/集成/压测分层 + CI 门槛） | 0.2, 3.2 |
| D6 | 多实例状态层（Redis，接口预留、单实例先行） | 1.3（可延后） |

> 规则：**设计先行**——每个工作流开工前，D 项对应小设计先落盘
> （1~2 页即可，不搞重文档），评审后进入实现。缓存基线已覆盖的
> 直接按基线做，不再出设计。

---

## 一、阶段 0：清账（去贴金）——P0

> 目标：README 只宣称真实生效的功能；关键路径有测试。

### 0.1 孤儿组件处置（按「接线 or 删宣称」）

| 项 | 决策 | 前置 | DoD |
|----|------|------|-----|
| 0.1-A MultiRateLimiter | **接线**（per-key/per-model RPM-TPM，真生产需求） | D2 小设计 | server 请求路径按 api_key+model 限流；管理 API 可配 key 限额；单测 |
| 0.1-B RetryBudget | **接线**（防重试风暴） | 无 | `Generate` 重试前 `TryConsume`；超预算直接失败；单测 |
| 0.1-C Coalescer（请求合并） | **接线**（L0 同 key 并发合并） | D1 小设计 | cache miss 时同 key 并发只打一次上游；coalescer 单测 |
| 0.1-D SLO | **接线成真指标+告警钩子** | 无 | `/v1/metrics` 出 burn rate；超阈值触发告警事件（日志+回调接口） |
| 0.1-E canary | **删 README 宣称 / 标实验** | 无 | README 移除或标注 experimental |

### 0.2 测试体系（D5 设计先行）

- cache / circuit / coalescer / tenant / auth / provider / routing / stream 单测。
- 全链路集成测试：mock 上游（已有 `cmd/mockupstream`）跑 正常/流式/熔断/超时 路径。
- 压测回归：`gwbench2` 关键场景进 CI（吞吐不倒退基线）。
- 目标：主干覆盖率 > 60%（`go test -cover` 计量）。

### 0.3 README 宣称纪律

- 功能表只列已接线项 + 一行验证方式；孤儿一律删除。
- 性能数字带日期与工具（已有先例）。

---

## 二、阶段 1：稳定与安全——P1

### 1.1 预算与告警（D3 设计先行）
- per-key/per-tenant 周期额度（日/月），软限告警 + 硬限 429。
- SLO 告警真实触发（复用 0.1-D 的告警钩子）。
- admin 页展示额度/用量/告警状态。

### 1.2 重试与降级语义（D4 设计先行）
- jitter + 指数退避；RetryBudget 生效（0.1-B）。
- 上下文窗口降级：`max_input_tokens` 超限自动换大窗模型/降级 provider。

### 1.3 多实例状态层（D6 设计；可延后，单实例先跑）
- `PrefixStore`/`AffinityRing`/L0 缓存接口的 Redis 实现。
- 亲和表一致性、滚动发布语义。单实例阶段用内存实现，接口不变。

### 1.4 可观测闭环
- 请求级 trace 贯穿（已有 tracing.go，补 handler 全链路）。
- 成本/命中/熔断/预热看板（对接前端 admin）。

---

## 三、阶段 2：差异化面——云缓存感知路由（B-1~B-5）

> 设计就绪（基线 v1.2）。串行依赖：B-1 观测 → B-2 亲和 → B-4 预热。

### 2.1 B-1 PrefixFingerprint + PrefixProfiler（纯观测）
- `prefix/fingerprint.go`：分层指纹树（P0..P3 + `token_len[4]`）。
- `prefix/profiler.go`：命中块归属推断（§5.1）、热度、TTL 倒计时。
- 路由仍走现有加权随机——**先证明「同前缀 miss 由路由打散造成」**。
- DoD：`hit_tokens_by_layer` 指标；`miss_reason=routed_away` 基线值。

### 2.2 B-2 有界负载亲和路由

> ✅ 2026-08-22 已实现（prefix/affinity.go + server 接线）: 加权虚拟节点
> 一致哈希环（sha256 截断, FNV 对短串雪崩差已修）+ 过载溢出（inflight >
> c×weight）+ 溢出 sticky（TTL 防抖）+ 冷启动直接哈希主节点;
> DM_GATEWAY_AFFINITY=1 启用; 8+1 单测; /v1/stats 出 affinity 快照。
> 待办: 完整 DrainBack 状态机（当前溢出 TTL 自然迁回, 无分阶段流量）。

- `prefix/affinity.go`：加权虚拟节点一致哈希 + 过载溢出 + 状态机
  `Bound→Overflow→DrainBack→Bound` + 振荡抑制。
- 冷启动直接走哈希主节点；转发亲和只认 `fp0123`。
- DoD：`affinity_overflow_total` / `migrate_back_total` 指标；回归门禁
  `miss_reason=routed_away` 显著下降。

### 2.3 B-3 固化前缀（DialogMesh 编译器）
- 块顺序契约（P0→P3 只追加不重排）+ 去噪（时间戳/uuid 进 P4）。
- 历史折叠（K 轮摘要模板固定）。
- 网关 drift 检测 + `prefix_drift_detected_total`。
- 跨仓协调：DialogMesh 编译器 golden 测试（0 漂移）。

### 2.4 B-4 WarmScheduler + QuotaCoordinator
- `prefix/warmup.go`：触发（热度×空闲×配额）、`X-DM-Warmup: 1`、
  迟到预热统计、只打亲和节点、双帽（全局 2% / 租户 3%，按全价测算）。
- `prefix/quota.go`：per-prefix 令牌桶（OpenAI 15 rpm 约束），真实优先。
- DoD：`warmup_late_total` / `warmup_token_ratio`；15min 空闲高价值前缀
  命中不归零。

### 2.5 B-5 L0 分层 TTL + 预热禁写（依赖 D1）
- 分层 TTL（hot 30m / normal 5m / cold 60s）；warmup 禁写 L0。
- 与 0.1-C coalescer 共用 L0 子系统。

---

## 四、阶段 3：验收与发布

### 3.1 指标验收（基线 §8 表）
- 业务 `prompt_cache_hit_rate`：观测基线 → 50–60% → 80%+（DeepSeek 先行）。
- 负载偏离 ≤ 20%；预热成本 < 2%；L0 命中维持 ~99%；warmup 不污染。

### 3.2 混沌与压测回归
- 故障注入：provider 熔断/超时/限流 → 溢出/迁回正确性演练。
- 压测回归进 CI（吞吐不倒退）。

### 3.3 文档与发布
- README 全面真实化（接线后重写功能表 + 性能数字 + 缓存能力）。
- 发布 v0.2.0（清账后）/ v0.3.0（差异化后）tag + release。

---

## 五、关键路径与依赖图

```
阶段0（去贴金）                     阶段1（稳定）          阶段2（差异化）
0.1-A 限流接线 ──D2──┐
0.1-B 重试预算 ──────┤
0.1-C 请求合并 ──D1──┼── 1.1 预算告警 ──D3──┐
0.1-D SLO 告警 ──────┤                        ├── 1.4 可观测闭环
0.1-E canary 删宣称 ─┘                        │
0.2 测试体系 ──D5────┴── 1.2 重试降级 ──D4──┘
                             1.3 多实例(延后) ──D6──┐
                                                    │
2.1 B-1 指纹+Profiler ──→ 2.2 B-2 亲和 ──→ 2.4 B-4 预热
                              │                     │
                              2.3 B-3 固化(编译器)   │
                              2.5 B-5 L0 分层 ──D1──┘
```

## 六、建议执行顺序（依赖最小化）

1. **0.1-A/B（限流+重试预算接线）+ 0.1-C 的 D1 设计** —— 独立、见效快。
2. **0.2 测试体系起步**（与接线并行，每接一块补一块测试）。
3. **2.1 B-1 观测**（独立于清账，可并行开）。
4. 0.1-D/E → 1.1 → 1.2（清账+稳定）。
5. **2.2 B-2 亲和**（依赖 B-1 数据支撑）。
6. 2.3 B-3 / 2.4 B-4 / 2.5 B-5（差异化收尾）→ 3.x 验收发布。
