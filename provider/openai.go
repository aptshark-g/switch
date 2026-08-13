package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

func (p *OpenAIProvider) GenerateStream(ctx context.Context, req *GenerateRequest) (<-chan *StreamChunk, error) {
	body, err := p.buildRequestBody(req, true)
	if err != nil {
		return nil, fmt.Errorf("openai: marshal request: %w", err)
	}
	httpReq, err := p.newRequest(ctx, body)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Accept", "text/event-stream")
	if tc := observability.GetTraceContext(ctx); tc != nil { child := tc.NewChildSpan(); observability.InjectTraceHeaders(child, httpReq) }
	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("openai: http do: %w", err)
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
	if json.Unmarshal(body, &errResp) == nil && errResp.Error.Message != "" {
		return fmt.Errorf("openai: %d %s: %s", resp.StatusCode, errResp.Error.Type, errResp.Error.Message)
	}
	return fmt.Errorf("openai: %d %s", resp.StatusCode, string(body))
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
}

// Ensure sync import is used.



