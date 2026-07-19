# Gateway Runtime — 业务链设计 · v2.0 工业级迭代

> 版本: v2.0 | 2026-07-18
> v1.0→v2.0: 断路器粒度升级、自适应并发、请求合并、加权路由、重试风暴保护、可观测性增强

---

## v1.0 现状评估

| 维度 | v1.0 | 差距 | 优先级 |
|------|------|------|:----:|
| 断路器 | 固定 5次→30s→half-open | 无滑动窗口, 无 per-endpoint, 无慢请求熔断 | P0 |
| 并发控制 | 固定槽位信号量 | 无法自适应负载变化 | P0 |
| 故障转移 | 线性遍历 for providers | 无加权路由, 无优先级 | P1 |
| 缓存 | 简单 map 无 TTL | 无淘汰策略, 无请求合并(coalescing) | P1 |
| 重试 | 无显式保护 | 无指数退避, 无 jitter, 无重试预算 | P0 |
| 限流 | IP 级别 | 无 per-key / per-model 粒度 | P1 |
| 可观测性 | 基础计数器 | 无 SLO 燃烧率, 无延迟分位数, 无追踪传播 | P2 |
| 配置 | 5s 轮询 YAML | 无灰度/feature flag | P2 |

---

## 迭代 1 — P0: 断路器 + 自适应并发 + 重试保护

### 1.1 断路器升级

```
v1.0 (当前):
  连续5次失败 → open(30s) → half-open → 成功→close
  问题: 固定阈值, 全 Provider 一刀切

v2.0 目标:
  滑动窗口断路器 (参考 Resilience4j / Hystrix):
    ┌─────────────────────────────────────────┐
    │ 滑动窗口 (默认 60s, 10 buckets)          │
    │                                         │
    │ Bucket[0..9]: 每6秒一个桶               │
    │ 每个桶记录: success / failure / timeout  │
    │                                         │
    │ 判定:                                    │
    │  慢请求率 > 50% → OPEN (慢熔断)          │
    │  失败率 > 50%   → OPEN (错误熔断)        │
    │  请求量 < 阈值   → 不触发 (冷启动保护)    │
    │                                         │
    │ half-open:                               │
    │  渐进放量 (非一次1个):                    │
    │    第1波 1 req  → 成功? → 第2波 3 req    │
    │    第2波 3 req  → 成功? → 第3波 10 req   │
    │    第3波 10 req → 成功? → CLOSED         │
    │    任何波失败    → 立即 OPEN              │
    └─────────────────────────────────────────┘

per-endpoint 断路器 (新增):
  不再一刀切 Provider。
  Provider "deepseek" 下有:
    /chat/completions → 独立的断路器
    /models          → 独立的断路器
    (健康检查不受断路器影响)

配置:
  circuit_breaker:
    sliding_window_size: 60s
    failure_rate_threshold: 0.5
    slow_call_rate_threshold: 0.5
    slow_call_duration_threshold: 10s   # >10s 算慢请求
    wait_duration_in_open: 30s
    permitted_in_half_open: [1, 3, 10]  # 渐进放量
    min_calls_before_evaluation: 10     # 冷启动保护
```

### 1.2 自适应并发 (Gradient2)

```
v1.0:
  max_concurrency: 10  (固定槽位)

v2.0: Gradient2 自适应 (参考 Envoy):
  
  算法:
    concurrency = minRTT / currentRTT × current_concurrency
    
    追踪 minRTT (最小往返时间)
    当 currentRTT 接近 minRTT → 说明并发未饱和 → 可以加
    当 currentRTT 远离 minRTT → 说明过载 → 减并发
    
  伪代码:
    if currentRTT > 2 × minRTT:
      target = concurrency / 2  (快速缩容)
    elif currentRTT > 1.3 × minRTT:
      target = concurrency × 0.9  (慢速缩容)
    else:
      target = concurrency + 1  (慢速扩容)
    
    concurrency = clamp(target, min_concurrency, max_concurrency)

配置:
  adaptive_concurrency:
    enabled: true
    min_concurrency: 2
    max_concurrency: 100
    gradient: gradient2
    min_rtt_window: 30          # 追踪最近30s的最优延迟
    sample_interval: 100ms       # 每100ms采样一次
```

### 1.3 重试保护

```
v1.0:
  max_retries: 1  (全局, 无退避)

v2.0:
  指数退避 + jitter + 重试预算:
    第1次重试: 100ms  + random(0, 50ms)
    第2次重试: 200ms  + random(0, 100ms)
    第3次重试: 400ms  + random(0, 200ms)
    ...
    硬上限: 3 次 + 总重试预算 10/min

  请求对冲 (Hedging):
    非重试, 而是同时发 N 个请求
    取第一个成功响应
    适用场景: p99 延迟高但成功率高的 Provider

配置:
  retry:
    max_attempts: 3
    initial_backoff: 100ms
    max_backoff: 2s
    backoff_multiplier: 2.0
    jitter: true
    retry_budget_per_min: 10     # 超预算→停止重试→直接返回错误
    retryable_status: [429, 500, 502, 503, 504]
  hedging:
    enabled: false               # 默认关闭, 对成本敏感
    max_hedge_requests: 2
    hedge_delay: 500ms           # 500ms后发第2个请求
```

---

## 迭代 2 — P1: 加权路由 + 请求合并 + 多级限流

### 2.1 加权路由

```
v1.0:
  ?provider=deepseek → 就它
  没指定 → providers[0]

v2.0:
  健康分数 + 权重 + 优先级:

  每个 Provider 有:
    health_score:   故障率→分数, 断路器OPEN→0
    latency_score:  p50延迟→分数
    cost_weight:    pricing→权重
    priority:       手动设置的优先级

  路由选择 (加权随机):
    weight = health_score × latency_score × cost_weight × priority
    weight = max(weight, 0.01)  # 不为0, 保底流量

  流控:
    weight < 0.1  → 仅 10% 流量
    weight < 0.01 → 仅 1% 流量 (探测器)
    weight = 0    → 完全摘除

配置:
  routing:
    strategy: weighted_random    # weighted_random | round_robin | latency_based
    health_decay: 0.1            # 单次失败扣分
    health_recovery: 0.01        # 每秒恢复
    min_weight: 0.01             # 保底探测流量
```

### 2.2 请求合并 (Coalescing)

```
问题: 10 个客户端同时问 "什么是微服务?"
      缓存未命中 → 10 次上游调用 → 浪费

方案:
  并发请求合并:
    相同 cacheKey → 第1个请求正常发
    第2..N 个请求 → 等待第1个返回 → 共享结果

  实现:
    coalescer:
      pending: map[cacheKey] → []chan response
      
      func handle(req):
        key = hash(req)
        if coalescer.pending[key] exists:
          ch = new channel
          coalescer.pending[key] = append(ch)
          return <-ch  (阻塞等第1个结果)
        else:
          coalescer.pending[key] = [new channel]
          resp = upstream.call(req)
          for ch in coalescer.pending[key]:
            ch <- resp
          delete(coalescer.pending, key)
          return resp
```

### 2.3 多级限流

```
v1.0:
  IP 级别令牌桶

v2.0: 三级限流:
  Level 1: Per API Key
    每个客户端 Key 独立的 RPM/TPM
  Level 2: Per Model
    同一模型的总限流
  Level 3: Per Provider
    所有请求到此 Provider 的总限流
  
  判定: 任一级超限 → 429

配置:
  rate_limits:
    - key: "client-key-1"
      rpm: 100
      tpm: 500000
    - key: "client-key-2"  
      rpm: 1000
      tpm: 5000000
  model_rate_limits:
    "deepseek-chat": {rpm: 1000, tpm: 10000000}
```

---

## 迭代 3 — P2: 可观测性 + 配置灰度

### 3.1 SLO 燃烧率告警

```
定义 SLO:
  成功响应率:    99.5% (30天)
  p99 延迟:      < 10s

燃烧率 (Burn Rate):
  burn_rate = 实际错误率 / SLO允许的错误率
  
  例: SLO 允许 0.5% 错误率
      当前 2% 错误率
      → burn_rate = 2/0.5 = 4x
      
  告警阈值 (参考 Google SRE):
    burn_rate > 14.4x → 页级告警 (2小时内耗尽30天预算)
    burn_rate > 6x    → ticket 告警
    burn_rate > 1x    → 仅记录

实现:
  滑动窗口: 1h / 6h / 30d
  每个窗口独立计算 burn_rate
  任一窗口超阈值 → 告警
```

### 3.2 分布式追踪传播

```
当前: 无上下文传播

v2.0:
  请求链路追踪:
    Client → Gateway → Provider → Upstream

  传播:
    Client: X-Trace-ID: "abc123"
    Gateway: 接收 → 追加 span → 转发给 Provider
    Response: X-Trace-ID 返回

  存储:
    追踪数据 → /v1/metrics 暴露
    支持导出到 Jaeger / Zipkin
```

### 3.3 配置灰度

```
v1.0:
  修改 provider.yaml → 5s 后全局生效

v2.0: 渐进式配置变更:
  
  canary:
    新配置 → 仅 1% 流量 → 观察 5min
    → 无异常 → 10% → 30% → 100%
    → 有异常 → 自动回滚

  feature_flags:
    experimental_routing: false  (0% traffic)
    adaptive_concurrency: 50%    (50% traffic)
```

---

## 迭代 4 — P3: 多租户 + 成本管理

### 4.1 租户隔离

```
每个 API Key → 独立的:
  - 限流配额
  - Token 用量追踪
  - 成本报告
  - 模型白名单 (可限制某 Key 只能用 fast 模型)

API:
  GET /v1/usage?api_key=xxx → 该 Key 的用量
```

### 4.2 成本跟踪

```
per-request 计费:
  cost = prompt_tokens × input_price + completion_tokens × output_price

按 Provider + Model + API Key 聚合:
  GET /v1/stats → {
    "deepseek": {
      "total_cost": 0.57,
      "by_model": {
        "deepseek-chat": {"tokens": 450000, "cost": 0.10},
      }
    }
  }
```

---

## 迭代路线图

| 迭代 | 内容 | 影响范围 | 估时 |
|:----:|------|---------|:----:|
| 1 | 滑动窗口断路器 + Gradient2 + 重试保护 | circuit.go, concurrency.go, openai.go | 3d |
| 2 | 加权路由 + 请求合并 + 多级限流 | manager.go, api.go, cache.go | 3d |
| 3 | SLO 告警 + 追踪传播 + 配置灰度 | observability/, config/ | 2d |
| 4 | 多租户 + 成本管理 | auth.go, api.go | 2d |

---

## 与 DialogMesh 的协议绑定（预留）

```
DialogMesh → switch gateway:
  POST /v1/chat/completions
  Header: X-API-Key: <dialogmesh-key>
  Header: X-Trace-ID: <uuid>
  
Switch gateway → DialogMesh 回调 (未来):
  Webhook: 模型列表变更通知
  Webhook: 成本超预算告警
```
