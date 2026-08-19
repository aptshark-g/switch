# switch — LLM API Gateway

> Go 构建的工业级 LLM API 转接器
>
> 滑动窗口断路器 · Gradient2 自适应并发 · 加权路由 · 请求合并 · SLO 告警

---

## 功能

| 能力 | 说明 |
|------|------|
| 🔀 **多 Provider 路由** | DeepSeek / OpenAI / Anthropic / Gemini / LMStudio / Ollama |
| 🛡️ **三层保护** | 信号量 → 速率限制 → 滑动窗口断路器 |
| ⚡ **自适应并发** | Gradient2 算法, 根据延迟梯度动态调整 |
| 🎯 **加权路由** | health_score × latency × cost × priority |
| 🔗 **请求合并** | 同 key 并发请求 → 1 次上游调用 |
| 📊 **诊断端点** | /v1/diagnostics — 每个 Provider 的 key/base_url/circuit 状态 |
| 🔄 **热重载** | 5s 轮询 provider.yaml + Admin API |
| 📈 **SLO 燃烧率** | 短窗口(1h) + 长窗口(6h) 双检测 |
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

`cmd/gwbench` 压测（2026-08-19 实测, 本机 Windows）:

- **热缓存吞吐**: 5000 请求 / 1.19s = **4210 req/s**, p50 11.3ms / p99 49.4ms,
  命中率 **99.5%**（缓存命中响应 ~2ms vs 未命中真实上游 ~1.08s）
- 此前压测: **3.4K – 22.8K req/s**, 转发 p50 < 1ms
- 负载超限时 shedder 自动丢弃/排队, 不拖垮上游

**缓存与隔离**: 响应缓存命中率 99.5%; 缓存键 =
`provider|model|api_key|X-Context-Hash`（跨租户/模型/Provider 隔离）;
上游前缀缓存命中（OpenAI cached_tokens / DeepSeek prompt_cache_hit /
Anthropic cache_read）透传统计（`/v1/stats.prompt_cache_hit_rate`）。

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
