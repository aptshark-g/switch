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

## 与 DialogMesh 绑定

switch 是 DialogMesh v6 的 LLM 代理层。一键启动：

```bash
cd DialogMesh
python scripts/start.py  # 自动检测并启动 switch
```

[绑定设计 →](docs/BINDING_DIALOGMESH.md) · [网关设计 →](docs/BUSINESS_CHAIN_01_GATEWAY.md)
