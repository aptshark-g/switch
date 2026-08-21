# switch — LLM API Gateway

> Go 构建的工业级 LLM API 转接器
>
> 滑动窗口断路器 · Gradient2 自适应并发 · 加权路由 · 请求合并 · SLO 告警

## 简介

统一封装 9+ 大模型厂商（DeepSeek / OpenAI / Anthropic / Gemini / Kimi /
Groq / OpenRouter / LM Studio / Ollama）, 一套 OpenAI 兼容接口同时对接云端
商用与本地离线模型。内置**四大生产级机制**: 滑动窗口+自适应熔断、健康巡检
与缓存、加权负载路由、自适应并发限流; 并具备 SSE 流式聚合、逐 key/per-model
计费持久化、per-key 配额、错误码目录、毫秒级热更新、Bearer 认证等网关要素。

**性能（自研 Go 压测工具 gwbench / gwbench2 + mock 上游实测, 2026-08）**:

- 缓存命中路径: 单实例 **14.8K~25.8K req/s**（64~256 并发）, p50 3ms,
  命中率 99.5%, 命中响应 ~2ms;
- 真实上游链路（未命中, 异步日志保观测）: 常驻 128 并发 **13.9K req/s**,
  0 失败, 平均延迟 9.1ms;
- 长上下文流式 SSE: 300 并发流 0 失败; 错误注入下熔断自动恢复;
- 资源: RSS ~29MB; 热更新生效 <100ms（50ms 轮询）。

> 注: 早前「3.4K-22.8K / p50 0.567ms」为纯缓存命中路径在旧同步日志实现下的
> 数据; 日志改为异步批量写入后吞吐整体提升 2.6~9×（详见 docs/BENCH_20260820.md）。

---

## 📚 文档导航

| 文档 | 内容 |
|------|------|
| [架构总览（2026-08-22）](docs/ARCHITECTURE_20260822.md) | 请求流/智能路由/前缀缓存/可靠性/观测 全图 + 模块/配置/端点清单 |
| [设计基线 v1.2](docs/CLOUD_CACHE_DESIGN_BASELINE_20260821.md) | 云厂商上下文缓存全量设计（固化前缀/预热/滚雪球/亲和） |
| [实现路线图](docs/IMPLEMENTATION_ROADMAP_20260821.md) | 阶段 0/1/2/3 任务与完成状态 |
| [生产级差距审计](docs/PRODUCTION_GAP_AUDIT_20260821.md) | 宣称 vs 实现核查 + LiteLLM 对照 + P0/P1/P2 |
| [压测报告](docs/BENCH_20260820.md) | 2026-08-20 性能实测（缓存/未命中/流式/错误注入） |
| [网关设计演进](docs/BUSINESS_CHAIN_01_GATEWAY.md) | 网关能力演进与 P1 设计 |
| [DialogMesh 绑定](docs/BINDING_DIALOGMESH.md) | 与 DialogMesh 的接线设计 |

---

## 功能

| 能力 | 说明 |
|------|------|
| 🔀 **多 Provider 路由** | DeepSeek / OpenAI / Anthropic / Gemini / LMStudio / Ollama |
| 🛡️ **三层保护** | 信号量 → 速率限制 → 滑动窗口断路器 |
| ⚡ **自适应并发** | Gradient2 算法, 根据延迟梯度动态调整 |
| 🎯 **智能路由** | 意图/复杂度规则层（X-Intent + X-Complexity → routing.rules, cortiq-gateway/leyline 模式）+ 池内加权随机（priority 分层 × weight × health × latency × cost, 2026-08-21 实测） |
| 🧭 **前缀亲和路由** | B-2 一致哈希亲和（FP+tenant → provider）+ 过载溢出 sticky（DM_GATEWAY_AFFINITY=1 启用, 2026-08-22） |
| 🔥 **心跳预热** | B-4 WarmScheduler（热度×空闲×配额触发, 只预热亲和节点, 迟到计数）+ QuotaCoordinator 真实优先（DM_GATEWAY_PREFIX_WARMUP=1, 2026-08-22） |
| 🔗 **请求合并** | 同 cacheKey 并发请求 → 1 次上游调用（2026-08-21 接线） |
| 📊 **诊断端点** | /v1/diagnostics — 每个 Provider 的 key/base_url/circuit 状态 |
| 🔄 **热重载** | 5s 轮询 provider.yaml + Admin API |
| 🔧 **SLO 燃烧率** | burn rate / alert level 进 /v1/stats, 超阈值告警日志（2026-08-21 接线） |
| 🔁 **流式聚合** | SSE tool_call 按 index 合并碎片/空 arguments（空回复根因修复） |
| 💰 **计费持久化** | CostTracker JSONL 落盘 + 重放, per-key / per-model 精细化分摊 |
| 🔑 **per-key 配额** | 租户级 token 配额 + 每日用量 |
| 📕 **错误码目录** | /v1/error-catalog — 错误码 → 含义 → 处置建议 |
| 🩹 **admin 页** | 零依赖控制台: providers / 计费 / 用量 / 配额 |
| ⚡ **热更新 diff** | 50ms 生效, added/updated/removed 明细 |
| 🛡️ **自适应熔断** | 3-5 次失败窗口, 恢复探测, 按成功率自动调整 |
| 🧭 **Bearer 认证** | API key + admin token（env 注入, 密钥不入库） |

---

## 快速启动

```bash
cd switch
# 配置 API Key
# 编辑 provider.yaml → deepseek.api_key: sk-xxx

# 启动
./gateway.exe

# 验证
curl http://localhost:8080/v1/health
curl http://localhost:8080/v1/diagnostics
```

---

## API

| 路径 | 说明 |
|------|------|
| `POST /v1/chat/completions` | LLM 调用入口 |
| `GET /v1/health` | 健康检查 |
| `GET /v1/diagnostics` | 每 Provider 详细状态 |
| `GET /v1/providers` | 供应商列表 |
| `PUT /v1/admin/providers/{name}` | 软编码修改配置 |
| `POST /v1/admin/reload` | 热重载 |

---

## 性能（实测）

压测（2026-08-20 实测, 本机 Windows; 完整报告 docs/BENCH_20260820.md）:

- **缓存命中**: 4210 req/s（64 并发）, 命中率 99.5%, 命中响应 ~2ms vs
  未命中真实上游 ~1.08s
- **缓存命中上限**（异步日志优化后）: c=256 **25813 req/s**（p50 3ms）
- **未命中路径**（真实上游链路）: 7189 req/s（c=256, 异步日志保观测）
- **高并发稳定**: 128 并发常驻 10s = 2607 req/s, **0 失败**, RSS ~29MB
- **长上下文流式 SSE**: 300 并发流 0 失败, 19500 事件完整收齐
- **错误场景**: 50% 429 注入 → 正确分类 + 熔断自动恢复
- 此前压测: 3.4K – 22.8K req/s（纯缓存命中转发, p50 < 1ms）

**缓存与隔离**: 缓存键 = `provider|model|api_key|X-Context-Hash`
（跨租户/模型/Provider 隔离）; 上游前缀缓存命中（OpenAI cached_tokens /
DeepSeek prompt_cache_hit / Anthropic cache_read）透传统计
（`/v1/stats.prompt_cache_hit_rate`）。

---

## 下载 / 构建

- **Release 二进制**: 打 tag（`v*`）自动触发 CI 构建 Windows/Linux/macOS 产物
  （见 `.github/workflows/release.yml`）。
- **源码构建**: 需要 Go 1.26+

```bash
go build -o gateway ./cmd/gateway     # Linux/macOS
go build -o gateway.exe ./cmd/gateway # Windows
```

---

## 与 DialogMesh 绑定


switch 是 DialogMesh v6 的 LLM 代理层。一键启动：

```bash
cd DialogMesh
python scripts/start.py  # 自动检测并启动 switch
```

[绑定设计 →](docs/BINDING_DIALOGMESH.md) · [网关设计 →](docs/BUSINESS_CHAIN_01_GATEWAY.md)
