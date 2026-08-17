package stream

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/aptshark/gateway/provider"
)

type SSEWriter struct {
	w       io.Writer
	rc      *http.ResponseController
}

func NewSSEWriter(w http.ResponseWriter) (*SSEWriter, error) {
	// 2026-08-17 FIX: 中间件链包装 ResponseWriter 后 w.(http.Flusher)
	// 断言失败 → 网关流式恒 500 "SSE not supported"。Go 1.20+ 标准做法
	// http.NewResponseController(w).Flush() 支持任意包装层透传。
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	if err := rc.Flush(); err != nil {
		return nil, fmt.Errorf("stream: flush: %w", err)
	}
	return &SSEWriter{w: w, rc: rc}, nil
}

func (s *SSEWriter) Send(event string, data any) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("stream: marshal sse data: %w", err)
	}
	if event != "" {
		fmt.Fprintf(s.w, "event: %s\n", event)
	}
	fmt.Fprintf(s.w, "data: %s\n\n", payload)
	return s.rc.Flush()
}

func (s *SSEWriter) SendDone() {
	fmt.Fprintf(s.w, "data: [DONE]\n\n")
	_ = s.rc.Flush()
}

func PipeStream(s *SSEWriter, ch <-chan *provider.StreamChunk) {
	for chunk := range ch {
		if chunk.Error != nil {
			_ = s.Send("error", map[string]string{"message": chunk.Error.Error()})
			return
		}
		payload := map[string]any{
			"id":    chunk.ID,
			"model": chunk.Model,
			"choices": []map[string]any{
				{
					"index":         0,
					"delta":         chunk.Delta,
					"finish_reason": chunk.FinishReason,
				},
			},
		}
		if chunk.Usage != nil {
			payload["usage"] = chunk.Usage
		}
		_ = s.Send("", payload)
	}
	s.SendDone()
}
