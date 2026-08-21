# 生产级差距审计（诚实版）

> 2026-08-21 · 用户问「之前叫工业生产级是不是贴金」，本文件如实回答。
> 原则：宣称必须可证伪；孤儿组件不配进 README；测试是生产级的底线。

## 一、结论先行

**是的，「工业生产级」此前是贴金。** 证据：

1. README 宣称「请求合并」「SLO 燃烧率」，但对应组件（`cache/coalescer.go`、
   `observability/slo.go`）**从未被 server 调用**——孤儿组件。
2. `provider/rate_limit_multi.go`（per-key/per-model RPM-TPM 多级限流）
   **未被接线**（server 只用 per-provider 限流 + 每日 token 配额）。
3. `RetryBudget`（防重试风暴）在 Manager 里创建，**`TryConsume` 从未被调用**。
4. `config/canary.go`（金丝雀发布）只有定义，无调用方。
5. 全仓测试仅 **2 个文件**（`merge_test.go` + `routing_test.go`）；cache、
   circuit、tenant、provider、auth、admin 均无单测。

「生产级」的底线是：宣称的功能真实生效 + 关键路径有测试 + 故障有可观测性。
现状三条都不满。

## 二、宣称 vs 实现核查表（2026-08-21 逐条代码核对）

| README 宣称 | 实现状态 | 证据 |
|-------------|----------|------|
| 多 Provider 路由（9 家） | ✅ 真实 | `manager.go` 注册 9 厂商 |
| 三层保护（信号量→限流→熔断） | ✅ 真实 | `manager.Generate` 顺序执行 |
| 自适应并发 Gradient2 | ⚠️ 部分 | `concurrency_adaptive.go` 有实现，但需 `adaptive_concurrency: true` 才启用，默认关 |
| 智能路由（规则+加权） | ✅ 真实 | 2026-08-21 实现，单测 10 例 |
| **请求合并** | ❌ **孤儿** | `cache/coalescer.go` 无 server 调用 |
| 诊断端点 | ✅ 真实 | `/v1/diagnostics` |
| 热重载 | ✅ 真实 | watcher 50ms |
| **SLO 燃烧率** | ❌ **孤儿** | `observability/slo.go` 无调用 |
| 流式聚合 | ✅ 真实 | `stream/sse.go` |
| 计费持久化 | ✅ 真实 | CostTracker + JSONL + 重放 |
| per-key 配额 | ⚠️ 部分 | 只有每日 token 配额；per-key **RPM/TPM** 的 MultiRateLimiter 未接线 |
| 错误码目录 | ✅ 真实 | `/v1/error-catalog` |
| admin 页 | ✅ 真实（简陋） | `/admin` |
| 热更新 diff | ✅ 真实 | added/updated/removed |
| 自适应熔断 | ✅ 真实 | circuit.go + 恢复探测 |
| Bearer 认证 | ✅ 真实 | auth.go |

**问题本质**：README 把「写了文件」当「实现了功能」。孤儿组件是历史迭代
留下的半成品，需要「接线 or 删除宣称」二选一，不允许挂着。

## 三、与 LiteLLM 功能对照（诚实差距）

| 能力域 | LiteLLM | switch | 差距 |
|--------|---------|--------|------|
| 厂商覆盖 | 100+ | 9 | 大（但按需扩展即可，非核心） |
| 路由策略 | simple-shuffle / least-busy / latency / usage-based / lowest-cost / 自定义 | 加权随机 + 意图规则 | 中：缺 least-busy / usage-based / 自定义策略接口 |
| 多部署/负载均衡 | 每模型多 deployment + weights + cooldown | routing pool + 加权 | 中：缺 cooldown 语义、部署级健康权重 |
| 重试 | num_retries + 重试预算 + context-window fallback | provider 内 MaxRetries | 中：缺上下文窗口降级回退 |
| 熔断 | 基础 | 滑动窗口 + 恢复探测 | 我们的反而细（自适应熔断） |
| 预算 | max_budget 软/硬限制 + 周期 | 无 | **大**：缺预算系统 |
| 虚拟 key/团队/用户 | 完整（key 加密、团队预算、用户配额） | 仅 Bearer + 每日 token 配额 | **大** |
| 速率限制 | per-model/key/user/team RPM-TPM | per-provider + 每日 token | 中：MultiRateLimiter 孤儿未接线 |
| 缓存 | redis / s3 / 语义缓存 | 进程内 L0 | 大：无分布式缓存 |
| 语义缓存 | 有 | 无 | 大（按需，勿为抄而抄） |
| 守卫/护栏 | guardrails + PII + moderation | 无 | 大（内部系统按需） |
| 观测 | prometheus + otel + callbacks + langfuse | prometheus + tracing | 中：缺外部 sinks/callbacks |
| 批量/嵌入/图像 | batch / embeddings / images / audio | 仅 chat | 大（按需） |
| admin UI | 完整控制台（spend/keys/teams） | 简陋单页 | 中 |
| 分布式状态 | redis 全状态 | 进程内 | 大（多实例必须补） |
| **前缀缓存感知路由** | **无此设计** | 基线已设计（固化/预热/滚雪球/亲和） | **我们领先** |
| **意图/复杂度路由** | 无 | 已实现 | **我们领先** |
| **吞吐性能** | Python 网关（百~千 req/s） | Go，实测 13.9K~25.8K req/s | **我们领先** |

## 四、差距分级（要达到「名副其实生产级」）

### P0（不补就是贴金——必须先做）

1. **孤儿组件处置**：请求合并 / SLO / MultiRateLimiter / RetryBudget / canary
   ——接线 or 从 README 删除。推荐：接线 MultiRateLimiter（per-key RPM/TPM
   是真生产需求）与 RetryBudget；请求合并与 SLO 先接线（工作量小）；
   canary 删宣称或标记实验。
2. **测试体系**：cache、circuit、coalescer、tenant、auth、provider 单测；
   全链路集成测试（mock 上游）；压测回归进 CI。目标：覆盖率主干 > 60%。
3. **README 宣称纪律**：功能表只列已接线项 + 验证方式；孤儿一律移除。

### P1（生产稳定与安全）

4. **预算与告警**：per-key/per-tenant 预算（周期额度）+ 超限告警（SLO
   接线成真告警，而非计算器）。
5. **重试语义**：jitter + 退避 + RetryBudget 真正生效 + 上下文窗口降级
   （长上下文请求自动换大窗模型）。
6. **多实例路径**：Redis 状态层设计（亲和表/Profiler/预热队列/L0 共享），
   单实例默认内存、多实例一键切换——基线已预留接口，补实现。
7. **可观测闭环**：请求级 trace 贯穿、成本/命中/熔断看板、错误率 SLO
   告警真实触发。

### P2（按需，不盲目追 LiteLLM）

8. 虚拟 key/团队模型（对外多租户才需要）；语义缓存（确定性强语义才值）；
   embeddings/batch 端点（DialogMesh 有需求再开）；guardrails（对外才需）。

## 五、可行性判断：能不能对标超越？

**全面对标 LiteLLM 是陷阱**：它是社区数年、100+ 厂商、企业版（SSO/审计/
告警）的庞然大物。逐项复刻 = 永远在追，且追的是别人的战场。

**我们真正能赢的轴（已具备基础，缺的是把设计做完）**：

1. **云厂商前缀缓存感知路由** —— LiteLLM 没有；我们的基线（固化前缀/心跳
   预热/滚雪球/有界负载亲和）实现后就是**业界第一梯队**（对标 llm-d 的
   87.4% 命中路径，且我们是云厂商网关不是 K8s 本地池）。
2. **吞吐** —— Go 原生，已实测 13.9K~25.8K req/s，是 LiteLLM Python 网关
   的 10~100 倍量级。这是硬性能壁垒。
3. **意图/复杂度路由** —— LiteLLM 无；我们已实现，配合 DialogMesh 是
   差异化卖点。

**「超越」的正确定义**：不是功能数量超 LiteLLM，而是「在我们服务的主场景
（DialogMesh + 云缓存命中 + 高吞吐）上做到最优」。功能面按需补 P0/P1，
差异化面把基线实现完。

## 六、设计文档未覆盖清单（当前基线缺什么）

1. **L0 缓存子系统设计**：请求合并（coalescer）接线的语义、淘汰策略、
   分层 TTL 落地、预热禁写。
2. **预算/告警子系统设计**：额度模型、周期、超限动作、告警渠道。
3. **限流模型完整设计**：per-key/per-model RPM-TPM 接线方案（MultiRateLimiter
   状态机 + 热更新）。
4. **重试与降级语义**：jitter 退避、上下文窗口降级、重试预算生效条件。
5. **多实例部署设计**：Redis 状态层、亲和表一致性、L0 共享、滚动发布。
6. **安全加固**：key 加密存储、审计日志、admin 权限分级。
7. **验证与混沌策略**：故障注入、熔断演练、压测回归、回滚预案。
8. **测试策略**：单测/集成/压测分层与 CI 门槛。

---

**一句话**：贴金时代结束。先把 P0 清账（孤儿接线/删宣称 + 测试体系），
再按 P1 补稳定与安全，差异化面（缓存感知路由）按基线走完——这样「生产级」
才有资格说出口。
