# Switch 网关 × 云厂商上下文缓存：综合设计方案（吸收版）

> 整合两版讨论稿，形成可直接指导开发的设计基线。  
> 核心目标：在云厂商前缀缓存机制下，通过**固化前缀、心跳预热、滚雪球、有界负载亲和路由**，实现高缓存命中率与负载均衡的共存。

---

## 1. 背景与问题定义

云厂商上下文缓存是**前缀精确匹配**：从 token 0 开始，最长一致段才能命中，命中后输入 token 享受折扣。这决定了网关层必须解决两个核心矛盾：

- **前缀稳定性**：同一逻辑上下文在所有请求中必须逐字节一致，否则缓存失效。
- **缓存局部性与负载均衡的冲突**：同前缀需固定到同一 provider/实例以保命中，但又要避免单节点过载。

网关需要在**不修改请求内容**的前提下，通过路由亲和、后台预热、前缀观测与反馈，最大化命中率并控制成本。

---

## 2. 云厂商上下文缓存机制（实现口径）

| 厂商 | 匹配单位 | 最小前缀 | TTL | 关键限制/特性 | 网关必须做的 | 网关不能做的 |
|------|----------|----------|-----|----------------|--------------|--------------|
| **DeepSeek** | 64-token 对齐，从 0 起最长一致段 | ~1024 tokens | ~5min 空闲 | 自动，零配置，命中折扣大（历史面板 96% 案例） | 亲和 + 3min 级预热 | 打散到多账号/多端点 |
| **OpenAI** | 引擎按前 ~256 token 哈希路由；128-token 递增块 | 1024–2048 tokens（模型相关） | 5–10min 无活动；显式 ttl 默认 30m | 每前缀建议 ≤15 req/min；支持显式 breakpoint (`prompt_cache_options`) | 前缀亲和（与云侧同向）；每前缀 ≤15 rpm 协调 | 同前缀打到不同 key/端点；预热抢真实配额 |
| **Anthropic** | 手动 `cache_control` breakpoint（最多 4 个） | ≥1024 tokens | 5m 默认 / 1h 可选 | 写有 25% 附加费；命中 -90%（5m）/ -75%（1h） | 直连协议注入 breakpoint | 当 OpenAI 兼容处理 |
| **Gemini** | 隐式 ≥32k；显式 cached content | 隐式大、显式灵活 | 1h（隐式） | 显式有存储费 | 长上下文才值得做 | 短前缀硬预热 |
| **本地 vLLM/SGLang** | GPU 上 prefix/radix KV，节点内存 | 1 token 起即有收益 | 直到显存驱逐 | **实例级**亲和，比云更严格 | 实例级 sticky；若有外部 KV 池（LMCache/Mooncake）可放宽 | 同前缀轮询到不同 Pod |

**统一规律**：自动或半自动 / 前缀精确 / 命中打折。云侧缓存与网关响应缓存（L0）正交：一个省 prefill 和钱，一个省整次 RTT。

---

## 3. 设计原则

1. **编译器拥有前缀，网关拥有局部性**：DialogMesh 负责块顺序与去噪；网关负责指纹、亲和、预热、观测。
2. **真实流量 > 预热 > 统计**：任何预热不得挤占 OpenAI 15 rpm、租户 TPM、本地显存。
3. **亲和是软状态，正确性不依赖它**：亲和表丢失只降命中率，不影响可用性。
4. **有界负载优先于绝对粘滞**：单节点过载时允许受控溢出，溢出后渐迁回，禁止抖动。
5. **预热只打当前亲和节点**：禁止同时预热多个 provider（写成本 + 配额浪费）。
6. **计费与指标隔离 warmup**：`X-DM-Warmup: 1` 不进业务命中率、不进租户账单（或独立科目）。
7. **网关禁止静默改写前缀**：固化在编译器完成，网关只做检测、指纹、统计、告警。

---

## 4. 总体架构

在现有 switch 的规则路由中嵌入 **Prefix 子系统**，不推翻已有规则层。

```
请求 → Auth/租户 → RPM/TPM 限流 → L0 响应缓存
         ↓ miss
   Prefix 只读检测 + 指纹树
         ↓
   规则层（意图/复杂度 → provider 组）
         ↓
   亲和层（有界负载一致哈希）
         ↓
   节点层（健康/权重/延迟/成本）
         ↓
   厂商适配（DeepSeek 透传 / OpenAI breakpoint / 本地实例）
         ↓
   上游 LLM
         ↓
   TokenUsage 归一 + 前缀命中回写 Profiler
         ↓
   按价值写 L0 + 返回
```

**新增模块**：

- `prefix/fingerprint.go`：分层指纹、只读检测
- `prefix/profiler.go`：命中块分布、热度、TTL 倒计时
- `prefix/affinity.go`：有界负载一致哈希、绑定、溢出、迁回
- `prefix/warmup.go`：WarmScheduler + `X-DM-Warmup`
- `prefix/quota.go`：每前缀/每 key 配额令牌桶，真实请求优先

状态存储：亲和表、Profiler、预热队列放 Dragonfly/Redis；进程内 L1 只做热指纹缓存。网关实例保持无状态。

---

## 5. 核心机制设计

### 5.1 固化前缀（Prefix Stabilization）

**分层块契约**（滚雪球的物理结构）：

```
P0 全局稳定块   系统提示 + 平台工具定义 + 平台知识     全租户共享
P1 租户块       人格 / 租户知识 / 合规条款             同租户共享
P2 项目块       项目语料 / 约束 / 工具子集             同项目共享
P3 会话块       折叠后的对话历史                       同会话共享
P4 本轮输入     user 本轮 + 动态时间/request_id         不可缓存尾
```

铁律：

- P0→P3 只追加、不插入、不重排；新知识用稳定 id 排序追加。
- 时间戳、uuid、trace_id、价格、库存只能进 P4 或 header。
- 工具列表按 name 排序，知识块按稳定 id 排序（编译器做，网关校验）。
- 会话块必须折叠：超过阈值（默认 4k–8k tokens）用稳定模板摘要占位；摘要更新频率每 K 轮一次（建议 K=4~8），避免每轮打穿缓存。

**哈希体系**：

| 哈希 | 用途 |
|------|------|
| `X-Context-Hash` | 仅 L0 响应缓存，来自 DialogMesh 编译上下文 |
| `PrefixFingerprint` | 对各层 canonical bytes 的 sha256，再组指纹树，用于亲和、Profiler、预热 |
| `PrefixTokenFingerprint`（可选） | 用目标模型 tokenizer 对 P0..P3 编码后哈希，用于跨格式对齐（阶段 C） |

指纹树示例：`fp0123 = sha256(fp012 || P3)` 为主亲和键。各层独立计数，以定位哪一层在滚雪球或拖后腿。

**网关侧只读检测**：
- 检测 system 块多余空白、工具顺序未排序、P4 噪声泄漏到前缀。
- 打指标 `prefix_drift_detected_total{layer,reason}`。
- **禁止**重排、改文本、剥时间戳转发。

### 5.2 心跳预热（Heartbeat Prewarm）

**触发条件**（同时满足）：
- 热度：近 15min 内 `req_count ≥ N` 或 `hit_tokens 累计 ≥ H`
- 空闲：距上次真实请求 ≥ TTL * 0.45（DeepSeek ~2-2.5min）
- 亲和节点健康且未熔断
- 协调器中仍有预热配额
- 非 `no_warmup` / 含 PII / 一次性前缀

**预热请求形态**：
- 复用 P0..P3 原样 + P4 固定 1 token 尾巴（如 `"."`），`max_tokens=1`，`stream=false`
- Header：`X-DM-Warmup: 1`
- **不写 L0**，**不计入业务缓存命中率**
- 只发往当前亲和 provider/实例

**QuotaCoordinator**（解决 OpenAI 15 rpm 节流）：
- 每个 `(provider, api_key, prefix_fp)` 一个令牌桶，容量=厂商建议 rpm
- `Acquire(real)` 立即扣，桶空则真实请求排队/限流；`Acquire(warm)` 仅当桶剩余 > 30% 且全局预热预算未超时允许
- 全局帽：预热 input tokens < 全站 input 2%；单租户预热成本 ≤ 该租户真实 input 成本 3%
- 超帽先停 P3 预热，再停 P2，P0 最后停

**频率**：
- DeepSeek：每 180s ± jitter
- OpenAI：每 180–240s，且受 15 rpm 桶约束
- 本地：按 KV 驱逐策略，更勤但低优先级

**成本归属**：
- P0 全局：平台
- P1 租户：租户
- P2 项目：租户/项目
- P3 会话：租户；已结束会话立即停预热

### 5.3 滚雪球（Snowball Accumulation）

目标：单位前缀长度的命中 token × 折扣最大化，而非无限变长。

- 上层共享面大，应放置真正稳定的内容；易变内容下沉。
- Profiler 输出 `hit_tokens_by_layer` 和 `miss_reason`，反馈编译器：
  - P2 命中低 + drift 高 → 项目块太杂
  - P3 很长但增量命中小 → 折叠策略需要调整
- 历史折叠：最近 K 轮原文保留，更早的变成稳定模板摘要；摘要模板固定字段，禁止模型自由发挥；更新频率每 K 轮一次。
- 敏感与合规：租户可声明 `cacheable: false`；PII 检测命中则不预热、不写 L0；亲和键含 `tenant_id` 禁止跨租户串缓存。

### 5.4 有界负载亲和路由（解决负载均衡 × 高命中）

**决策：直接上有界负载一致哈希（加权虚拟节点环 + 过载溢出），不用简单 map。**

**三级路由**：

1. **规则层**（已有）：意图/复杂度 → provider 组 / 模型档位。
2. **亲和层**（新增）：
   - 键：`hash(fp0123 + tenant_id + group_id)`
   - 算法：加权虚拟节点环（每 provider 按 weight 放 256*weight 个节点），查找从 `h(fp)` 顺时针第一个未过载且健康的节点。
   - 过载定义：`inflight 或 token/s 或 rpm > c * 应承担份额`，`c=1.25` 起步。
   - 过载则取环上下一个，形成溢出链。
3. **节点层**（已有加强）：仅当冷前缀、亲和表 miss、主节点过载/熔断时使用组内健康/权重/延迟/成本选择。

**冷启动**：新前缀直接走哈希主节点，**不是先加权随机再绑定**，避免打散最值钱的冷启动缓存。

**溢出与渐迁回**：

```
状态机：Bound → Overflow → DrainBack → Bound
```

- 主节点熔断/过载 → 溢出到溢出链，给短期 sticky（TTL=厂商 TTL 量级）。
- 主节点连续健康 ≥ T_recover → 进入迁回。
- 迁回用流量分数：2min 10% → 5min 30% → 10min 70% → 100%。
- 迁回期间允许对 P0/P1 高价值层在目标节点做一次预热，然后逐步切。
- 来回振荡抑制：10min 内同一前缀最多迁回 1 次。
- 组内全挂 → 走规则层的跨组 fallback（已有智能路由），可用性优先。

**多前缀块与主亲和**：一个请求有多层指纹，但转发只能有一个主亲和。**转发亲和只认 `fp0123`**；Profiler 仍按层统计；预热只跟主亲和节点。

---

## 6. 与 L0 响应缓存的协同

| 请求类型 | L0 响应缓存 | 上游前缀缓存 |
|----------|-------------|--------------|
| 普通业务 | 可写，TTL 按前缀热度 5–30min | 正常 |
| Warmup | **禁止写** | 只为保上游/KV 热 |
| 流式 | 结束后可写完整响应 | 正常 |
| 含 P4 强动态 | L0 易 miss，缩短 TTL 或跳过 | P0–P3 仍可命中 |

分层 TTL：高价值前缀 L0 TTL 30min，普通 5min，低价值 60s 或不缓存。

---

## 7. 数据模型

```text
PrefixRecord {
  fp0123, fp0, fp01, fp012,
  tenant_id, project_id, session_id?,
  group_id,                 // 规则层组
  primary_provider,         // 亲和主
  overflow_provider?,
  bind_expire_at,           // 随热度续期
  last_real_at, last_warm_at,
  req_count_window,
  hit_tokens, miss_tokens, input_tokens,
  last_hit_block_tokens,    // 云侧返回的 cached tokens
  drift_flags,
  warmup_eligible,
  cacheable,
}

AffinityRing {
  group_id → weighted vnodes | maglev table
  load_vector[provider]     // inflight / tps，进程内 + Redis 近似
}

QuotaBucket {
  key = (provider, api_key, fp)
  rpm, reserved_real, warmup_tokens_used
}
```

续期：每次真实请求把 `bind_expire_at` 续到 `now + max(厂商TTL, 15min)`。

---

## 8. 观测、指标与验收

**关键指标**：

- `prompt_cache_hit_rate`（业务，不含 warmup）：DeepSeek 目标 **稳定 ≥ 80%**，冲 96% 案例
- `prompt_cache_hit_tokens` / `miss_tokens` 按 `layer`、`provider`、`tenant`
- `cache_miss_reason{drift, routed_away, ttl, below_min, throttle, cold}`
- `affinity_overflow_total` / `affinity_migrate_back_total`
- `warmup_requests` / `warmup_token_ratio`（< 2%）
- `prefix_drift_detected_total`
- `load_skew`（组内吞吐偏离）
- `routed_away_miss_ratio`（亲和是否在帮倒忙）

**验收表**：

| 指标 | 目标 |
|------|------|
| 业务 `prompt_cache_hit_rate` | DeepSeek ≥ 80%，15min 空闲不归零 |
| `miss_reason=routed_away` | 亲和上线后显著下降，作为回归门禁 |
| 预热成本 | < 2% input tokens，单租户 < 3% 自身成本 |
| 负载偏离 | 健康节点 ≤ 20%（排除冷启动/迁回窗口） |
| L0 命中 | 维持现网 ~99%，warmup 不污染 |
| 前缀漂移 | 编译器 golden 测试 0 漂移；网关 drift 只来自违规调用方 |

---

## 9. 失败模式与降级

| 场景 | 行为 |
|------|------|
| Dragonfly 短暂不可用 | 进程内环 + 本地指纹缓存；失去跨实例亲和一致性，允许命中下降，服务不停 |
| Profiler 落后 | 预热少做，不错做 |
| 协调器桶丢失 | 预热暂停，真实流量不受影响 |
| 某 provider 熔断 | 溢出；命中率允许掉，错误率不允许掉 |
| 编译器发版导致 P0 变化 | 视为新指纹，旧预热立即停；发版窗口命中下跌可预期 |
| 调用方破坏前缀 | 告警 + 不预热；网关不“修复” |

---

## 10. 安全与隔离

- 预热请求使用与业务相同的 api_key 隔离，禁止用平台超级 key 替租户预热。
- 指纹可存；前缀原文默认不落 Profiler，只落 hash + 长度 + 层；排障需要原文时走审计通道、短 TTL、脱敏。
- Warmup 必须走与业务相同的 Guardrails，防止被当免费探针。
- 多租户：亲和键含 `tenant_id`；L0 已隔离，保持。

---

## 11. 落地路线图

**阶段 A（已就绪）**  
规则层 + 池内加权随机；`prompt_cache_hit_rate` 透传。

**阶段 B（核心，建议 2 个迭代）**

1. `PrefixFingerprint` 树 + PrefixProfiler（只观测，路由仍随机）——证明打散确实造成 miss。
2. 组内有界负载一致哈希 + 溢出/迁回；`routed_away` 指标作为门禁。
3. DialogMesh 块顺序/去噪 golden 测试；网关 drift 检测。
4. WarmScheduler + QuotaCoordinator；DeepSeek 先行。
5. L0 按热度分层 TTL；warmup 禁写 L0。

**阶段 C（进阶）**

- OpenAI `prompt_cache_options` / 显式 ttl
- Anthropic 直连 4 breakpoint（P0/P1/P2/P3）
- 本地实例级 KV 亲和（若尚未与 GPU 调度打通，这是本地池最大性能杠杆）
- token 级指纹预警
- 成本报表；折叠策略自动调参

**厂商顺序**：DeepSeek 主链路验证全部机制 → OpenAI 配额协调器（15 rpm 硬约束）→ 本地 KV 亲和（若本地流量大可与 OpenAI 对调）→ Anthropic/Gemini。

---

## 12. 待讨论问题结论（已拍板）

1. **前缀由谁固化**：请求侧（DialogMesh）负责，网关只读检测。网关强改字节风险不可接受。
2. **预热成本归属**：P0 平台，P1–P3 租户；独立科目；双帽（全局 2% / 租户 3%）。
3. **chash vs map**：直接上加权虚拟节点 + 有界负载。前缀少时 chash 退化为近似 sticky，前缀多时仍稳定；避免二次迁移。
4. **15 rpm**：QuotaCoordinator 真实优先，预热只用保留后的余量；不够则停预热。
5. **Anthropic/Gemini**：本期不做主路径；接口预留，DeepSeek 验证后再扩。本地 KV 亲和按流量决定是否插队。

---

## 13. 实现最小接口

```text
FingerprintMessages(msgs) -> FingerprintTree
DetectDrift(msgs) -> []DriftFlag                 // 不修改 msgs

PickProvider(ctx, tree, group) -> Decision
    Decision { primary, chosen, overflow, reason }

Profiler.Observe(tree, usage, decision)
WarmScheduler.Tick()                             // 拉热名单，问 Coordinator 要令牌
Coordinator.Acquire(fp, kind=real|warm) -> ok
```

规则层输出 `group` 后只调 `PickProvider`；节点层健康/权重继续复用现有池。

---

**一句话收束**：云侧缓存只认从 token 0 开始的铁前缀；网关能做的只有三件事——让编译器把前缀焊死、让同一前缀在有界负载下粘在同一节点、在 TTL 死掉前用让路给真实流量的最小请求续命。其余一切（随机打散、网关改字、多节点同时预热）都会把滚雪球推回原点。