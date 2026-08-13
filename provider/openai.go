package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
	"github.com/aptshark/gateway/observability"
)

// OpenAIProvider implements Provider for any OpenAI-compatible API.
// This covers OpenAI, DeepSeek, Kimi, Groq, vLLM, Ollama, and any
// proxy that speaks the /v1/chat/completions protocol.
type OpenAIProvider struct {
	cfg    ProviderConfig
	client *http.Client
}

// NewOpenAIProvider constructs an OpenAIProvider from the given config.
func NewOpenAIProvider(cfg ProviderConfig) (*OpenAIProvider, error) {
	if cfg.Proxy != "" {
		// Proxy support can be added via http.ProxyURL
		transport := &http.Transport{
			MaxIdleConns:    20,
			IdleConnTimeout: 90 * time.Second,
		}
		return &OpenAIProvider{
			cfg: cfg,
			client: &http.Client{
				Timeout:   cfg.Timeout(),
				Transport: transport,
			},
		}, nil
	}
	return &OpenAIProvider{
		cfg: cfg,
		client: &http.Client{
			Timeout:   cfg.Timeout(),
			Transport: sharedTransport,
		},
	}, nil
}

func (p *OpenAIProvider) Name() string   { return p.cfg.Name }
func (p *OpenAIProvider) Config() *ProviderConfig { return &p.cfg }

func (p *OpenAIProvider) Generate(ctx context.Context, req *GenerateRequest) (*GenerateResponse, error) {
	// one-api 风格（2026-08-13）: 上游恒流式 — 长生成下流式逐块
	// 送达, 避免非流式整包缓冲 + 固定超时截断; 客户端要非流式时
	// 网关内聚合。
	if !req.Stream {
		return p.generateViaStream(ctx, req)
	}
	body, err := p.buildRequestBody(req, false)
	if err != nil {
		return nil, fmt.Errorf("openai: marshal request: %w", err)
	}
	httpReq, err := p.newRequest(ctx, body)
	if err != nil {
		return nil, err
	}
	if tc := observability.GetTraceContext(ctx); tc != nil { child := tc.NewChildSpan(); observability.InjectTraceHeaders(child, httpReq) }
	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("openai: http do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, p.parseError(resp)
	}
	return p.parseResponse(resp.Body)
}

// generateViaStream 用流式上游聚合出完整响应（客户端非流式路径）。
func (p *OpenAIProvider) generateViaStream(ctx context.Context,
	req *GenerateRequest) (*GenerateResponse, error) {
	// 2026-08-13 修复: buildRequestBody 的 Stream = stream && req.Stream,
	// 客户端非流式请求直接传 req 会把上游也变成非流式 → SSE 解析不到
	// 任何行 → 聚合恒空。内部拷贝并强制 stream=true。
	streamReq := *req
	streamReq.Stream = true
	ch, err := p.GenerateStream(ctx, &streamReq)
	if err != nil {
		return nil, err
	}
	var content strings.Builder
	var reasoning strings.Builder
	var toolCalls []ToolCall
	var finishReason string
	var usage *TokenUsage
	id, model := "", req.Model
	for chunk := range ch {
		if chunk.Error != nil {
			return nil, fmt.Errorf("openai: stream: %w", chunk.Error)
		}
		if chunk.ID != "" {
			id = chunk.ID
		}
		if chunk.Model != "" {
			model = chunk.Model
		}
		content.WriteString(chunk.Delta.Content)
		reasoning.WriteString(chunk.Delta.ReasoningContent)
		if len(chunk.Delta.ToolCalls) > 0 {
			toolCalls = append(toolCalls, chunk.Delta.ToolCalls...)
		}
		if chunk.FinishReason != "" {
			finishReason = chunk.FinishReason
		}
		if chunk.Usage != nil {
			usage = chunk.Usage
		}
	}
	msg := Message{Role: "assistant", Content: content.String(),
		ToolCalls: toolCalls}
	// 推理模型: content 空但 reasoning 非空 → 透出（与 parseResponse 同规则）
	if msg.Content == "" && reasoning.Len() > 0 &&
		os.Getenv("REASONING_FALLBACK") != "0" {
		msg.Content = reasoning.String()
	}
	return &GenerateResponse{
		ID:      id,
		Model:   model,
		Choices: []Choice{{Index: 0, Message: msg, FinishReason: finishReason}},
		Usage:   usage,
	}, nil
}

func (p *OpenAIProvider) GenerateStream(ctx context.Context, req *GenerateRequest) (<-chan *StreamChunk, error) {
	body, err := p.buildRequestBody(req, true)
	if err != nil {
		return nil, fmt.Errorf("openai: marshal request: %w", err)
	}
	// 2026-08-13: 连接阶段重试（one-api/LiteLLM 风格）— 网络错误/5xx
	// 在首个 chunk 前重试（max_retries 默认 2, 退避+jitter）; 流开始后
	// 不重试（避免客户端收到重复片段）。
	attempts := 1 + max(0, p.cfg.MaxRetries)
	var resp *http.Response
	var respErr error
	for attempt := 0; attempt < attempts; attempt++ {
		httpReq, err := p.newRequest(ctx, body)
		if err != nil {
			return nil, err
		}
		httpReq.Header.Set("Accept", "text/event-stream")
		if tc := observability.GetTraceContext(ctx); tc != nil {
			child := tc.NewChildSpan()
			observability.InjectTraceHeaders(child, httpReq)
		}
		resp, respErr = p.client.Do(httpReq)
		if respErr == nil && resp.StatusCode == http.StatusOK {
			break
		}
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		if attempt+1 < attempts {
			backoff := time.Duration(300*(1<<attempt)) * time.Millisecond
			backoff += time.Duration(rand.Intn(150)) * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	if respErr != nil {
		return nil, fmt.Errorf("openai: http do: %w", respErr)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, p.parseError(resp)
	}
	ch := make(chan *StreamChunk, 64)
	go p.readSSEStream(resp.Body, ch)
	return ch, nil
}

// Health performs a lightweight liveness check.
func (p *OpenAIProvider) Health(ctx context.Context) *HealthStatus {
	start := time.Now()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimSuffix(p.cfg.BaseURL, "/")+"/models", nil)
	if p.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)
	}
	resp, err := p.client.Do(req)
	elapsed := time.Since(start).Milliseconds()
	status := &HealthStatus{
		LatencyMs: elapsed,
		LastCheck: time.Now(),
	}
	if err != nil {
		status.Error = err.Error()
		return status
	}
	defer resp.Body.Close()
	status.Healthy = resp.StatusCode == http.StatusOK
	return status
}

// --- internals ---

type openaiChatRequest struct {
	Model          string          `json:"model"`
	Messages       []Message       `json:"messages"`
	MaxTokens      int             `json:"max_tokens,omitempty"`
	Temperature    float64         `json:"temperature,omitempty"`
	TopP           float64         `json:"top_p,omitempty"`
	Stream         bool            `json:"stream"`
	Stop           []string        `json:"stop,omitempty"`
	Tools          []ToolDef       `json:"tools,omitempty"`
	ToolChoice     any             `json:"tool_choice,omitempty"`
	ResponseFormat *ResponseFormat `json:"response_format,omitempty"`
	Thinking       any             `json:"thinking,omitempty"`
}

type openaiChatResponse struct {
	ID      string             `json:"id"`
	Model   string             `json:"model"`
	Choices []openaiChatChoice `json:"choices"`
	Usage   *TokenUsage        `json:"usage,omitempty"`
}

type openaiChatChoice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

type openaiErrorResponse struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

func (p *OpenAIProvider) buildRequestBody(req *GenerateRequest, stream bool) ([]byte, error) {
	model := p.cfg.ResolveModel(req.Model)
	// 推理开关三层: 请求级 thinking > 厂商级配置 thinking > 默认(思考开)
	thinking := req.Thinking
	if thinking == nil {
		thinking = p.cfg.Thinking
	}
	body := openaiChatRequest{
		Model:          model,
		Messages:       req.Messages,
		MaxTokens:      req.MaxTokens,
		Temperature:    req.Temperature,
		TopP:           req.TopP,
		Stream:         stream && req.Stream,
		Stop:           req.Stop,
		Tools:          req.Tools,
		ResponseFormat: req.ResponseFormat,
		Thinking:       thinking,
	}
	if req.ToolChoice != "" {
		body.ToolChoice = req.ToolChoice
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	// Merge extra_body from config.
	if len(p.cfg.ExtraBody) > 0 {
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		for k, v := range p.cfg.ExtraBody {
			m[k] = v
		}
		raw, _ = json.Marshal(m)
	}
	return raw, nil
}

func (p *OpenAIProvider) newRequest(ctx context.Context, body []byte) (*http.Request, error) {
	url := strings.TrimSuffix(p.cfg.BaseURL, "/") + "/v1/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)
	}
	for k, v := range p.cfg.ExtraHeaders {
		req.Header.Set(k, v)
	}
	return req, nil
}

func (p *OpenAIProvider) parseResponse(r io.Reader) (*GenerateResponse, error) {
	var raw openaiChatResponse
	if err := json.NewDecoder(r).Decode(&raw); err != nil {
		return nil, fmt.Errorf("openai: decode response: %w", err)
	}
	choices := make([]Choice, len(raw.Choices))
	for i, c := range raw.Choices {
		msg := c.Message
		// DeepSeek V4 推理模型（2026-08-13, 社区同款问题 go#11142 /
		// SkillClaw#70 / vllm#50753）: 思维链与正文共享 max_tokens,
		// 密集输出时 thinking 吃光预算 → content="" + finish=length,
		// 答案滞留在 reasoning_content。修复: content 空时透出
		// reasoning_content（REASONING_FALLBACK=0 可关）。
		if msg.Content == "" && msg.ReasoningContent != "" &&
			os.Getenv("REASONING_FALLBACK") != "0" {
			msg.Content = msg.ReasoningContent
		}
		choices[i] = Choice{
			Index:        c.Index,
			Message:      msg,
			FinishReason: c.FinishReason,
		}
	}
	return &GenerateResponse{
		ID:      raw.ID,
		Model:   raw.Model,
		Choices: choices,
		Usage:   raw.Usage,
	}, nil
}

func (p *OpenAIProvider) parseError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var errResp openaiErrorResponse
	msg := string(body)
	if json.Unmarshal(body, &errResp) == nil && errResp.Error.Message != "" {
		msg = errResp.Error.Message
	}
	// 2026-08-13: 带状态码+消息分类（上游 4xx 不再落 UNKNOWN）
	return ClassifyErrorWithMessage(p.cfg.Name, resp.StatusCode, msg)
}

// --- SSE streaming ---

type openaiStreamChunk struct {
	ID      string               `json:"id"`
	Model   string               `json:"model"`
	Choices []openaiStreamChoice `json:"choices"`
	Usage   *TokenUsage          `json:"usage,omitempty"`
}

type openaiStreamChoice struct {
	Index        int    `json:"index"`
	Delta        Delta  `json:"delta"`
	FinishReason string `json:"finish_reason"`
}

func (p *OpenAIProvider) readSSEStream(body io.ReadCloser, ch chan<- *StreamChunk) {
	defer close(ch)
	defer body.Close()
	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		const prefix = "data: "
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		payload := line[len(prefix):]
		if payload == "[DONE]" {
			return
		}
		var raw openaiStreamChunk
		if err := json.Unmarshal([]byte(payload), &raw); err != nil {
			ch <- &StreamChunk{Error: fmt.Errorf("openai: decode sse: %w", err)}
			return
		}
		for _, c := range raw.Choices {
			ch <- &StreamChunk{
				ID:           raw.ID,
				Model:        raw.Model,
				Delta:        c.Delta,
				FinishReason: c.FinishReason,
				Usage:        raw.Usage,
			}
		}
	}
}

// Ensure interface compliance at compile time.
var (
	_ Provider       = (*OpenAIProvider)(nil)
	_ StreamProvider = (*OpenAIProvider)(nil)
)
// sharedTransport is the HTTP transport shared by all providers.
// It has tuned connection pool settings for high-throughput scenarios.
var sharedTransport = &http.Transport{
	MaxIdleConns:        200,
	MaxIdleConnsPerHost: 100,
	MaxConnsPerHost:     200,
	IdleConnTimeout:     90 * time.Second,
	TLSHandshakeTimeout: 10 * time.Second,
	DisableCompression:  false,
	// 2026-08-13: 连接超时与响应超时分离 — 死连接 5s 快速失败,
	// 长生成由 client.Timeout（120s 默认）负责。
	DialContext: (&net.Dialer{Timeout: 5 * time.Second,
		KeepAlive: 30 * time.Second}).DialContext,
}

// Ensure sync import is used.



