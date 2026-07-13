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
	flusher http.Flusher
}

func NewSSEWriter(w http.ResponseWriter) (*SSEWriter, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("stream: ResponseWriter does not support flushing")
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	return &SSEWriter{w: w, flusher: flusher}, nil
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
	s.flusher.Flush()
	return nil
}

func (s *SSEWriter) SendDone() {
	fmt.Fprintf(s.w, "data: [DONE]\n\n")
	s.flusher.Flush()
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
