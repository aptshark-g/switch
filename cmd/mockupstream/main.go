// mockupstream — 本地 OpenAI 兼容 mock 上游（网关压测用）。
// modes: normal(即时小响应) / stream(长 SSE 流, 模拟长上下文) / error(错误注入)
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

var usageTmpl = map[string]any{
	"prompt_tokens": 8000, "completion_tokens": 512, "total_tokens": 8512,
}

func writeChat(w http.ResponseWriter, content string) {
	payload := map[string]any{
		"id": "mock-1", "object": "chat.completion", "model": "mock-model",
		"choices": []map[string]any{{"index": 0, "message": map[string]any{
			"role": "assistant", "content": content}, "finish_reason": "stop"}},
		"usage": usageTmpl,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func main() {
	port := flag.Int("port", 9001, "listen port")
	mode := flag.String("mode", "normal", "normal | stream | error")
	chunks := flag.Int("chunks", 64, "SSE chunk count (stream mode)")
	delayMs := flag.Int("delay", 0, "per-chunk delay ms (stream mode)")
	errRate := flag.Float64("error-rate", 0.0, "0..1 error probability")
	flag.Parse()

	http.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if rand.Float64() < *errRate {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"mock upstream rate limited","code":"RATE_LIMITED"}}`))
			return
		}
		if *mode == "stream" {
			fl, _ := w.(http.Flusher)
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			for i := 0; i < *chunks; i++ {
				delta := fmt.Sprintf(`{"id":"mock-s","object":"chat.completion.chunk","model":"mock-model","choices":[{"index":0,"delta":{"content":"chunk-%d-"},"finish_reason":null}]}`, i)
				_, _ = fmt.Fprintf(w, "data: %s\n\n", delta)
				if fl != nil {
					fl.Flush()
				}
				if *delayMs > 0 {
					time.Sleep(time.Duration(*delayMs) * time.Millisecond)
				}
			}
			_, _ = fmt.Fprintf(w, "data: %s\n\n", `{"id":"mock-s","object":"chat.completion.chunk","model":"mock-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":8000,"completion_tokens":512,"total_tokens":8512}}`)
			return
		}
		writeChat(w, "mock normal response")
	})

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("mockupstream %s on %s (chunks=%d delay=%dms errRate=%.2f)", *mode, addr, *chunks, *delayMs, *errRate)
	log.Fatal(http.ListenAndServe(addr, nil))
}
