package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aptshark/gateway/config"
	"github.com/aptshark/gateway/provider"
	"github.com/aptshark/gateway/server"
)

type TestResult struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Error  string `json:"error,omitempty"`
}

type TestReport struct {
	Gateway string        `json:"gateway_version"`
	Time    string        `json:"time"`
	Results []TestResult  `json:"results"`
	Summary ReportSummary `json:"summary"`
}

type ReportSummary struct {
	Total  int `json:"total"`
	Passed int `json:"passed"`
	Failed int `json:"failed"`
}

func runSelfTest(cfg *config.GatewayConfig, addr string) {
	report := &TestReport{
		Gateway: "gateway-runtime v1.0",
		Time:    time.Now().UTC().Format(time.RFC3339),
		Results: make([]TestResult, 0),
	}

	mgr := provider.NewManager()
	registerFactories(mgr)
	mgr.Bootstrap(cfg.Providers)

	watcher := config.NewWatcher("provider.yaml", 5*time.Second)
	srv := server.NewWithWatcher(mgr, addr, watcher, cfg.Auth, nil)

	go func() {
		if err := srv.Start(); err != nil {
			log.Printf("selftest: server error: %v", err)
		}
	}()
	time.Sleep(500 * time.Millisecond)

	client := &http.Client{Timeout: 30 * time.Second}
	base := "http://" + addr
	if strings.HasPrefix(addr, ":") {
		base = "http://localhost" + addr
	}

	add := func(name string, fn func() error) {
		r := TestResult{Name: name}
		if err := fn(); err != nil {
			r.Passed = false
			r.Error = err.Error()
		} else {
			r.Passed = true
		}
		report.Results = append(report.Results, r)
	}

	add("health", func() error {
		resp, err := client.Get(base + "/v1/health")
		if err != nil { return err }
		defer resp.Body.Close()
		if resp.StatusCode != 200 && resp.StatusCode != 503 { return fmt.Errorf("expected 200/503, got %d", resp.StatusCode) }
		return nil
	})

	add("health/detail", func() error {
		resp, err := client.Get(base + "/v1/health/detail")
		if err != nil { return err }
		defer resp.Body.Close()
		var m map[string]any
		json.NewDecoder(resp.Body).Decode(&m)
		if m["go_version"] == nil { return fmt.Errorf("missing go_version") }
		return nil
	})

	add("providers_public", func() error {
		req, _ := http.NewRequest("GET", base+"/v1/providers", nil)
		req.Header.Set("Authorization", "Bearer sk-test-client")
		resp, err := client.Do(req)
		if err != nil { return err }
		defer resp.Body.Close()
		if resp.StatusCode != 200 { return fmt.Errorf("expected 200, got %d", resp.StatusCode) }
		return nil
	})

	add("auth_401", func() error {
		resp, err := client.Get(base + "/v1/providers")
		if err != nil { return err }
		defer resp.Body.Close()
		if resp.StatusCode != 401 { return fmt.Errorf("expected 401, got %d", resp.StatusCode) }
		return nil
	})

	add("auth_403", func() error {
		req, _ := http.NewRequest("POST", base+"/v1/admin/reload", nil)
		req.Header.Set("Authorization", "Bearer wrong-token")
		resp, err := client.Do(req)
		if err != nil { return err }
		defer resp.Body.Close()
		if resp.StatusCode != 403 { return fmt.Errorf("expected 403, got %d", resp.StatusCode) }
		return nil
	})

	add("admin_reload", func() error {
		req, _ := http.NewRequest("POST", base+"/v1/admin/reload", nil)
		req.Header.Set("Authorization", "Bearer admin-test")
		resp, err := client.Do(req)
		if err != nil { return err }
		defer resp.Body.Close()
		if resp.StatusCode != 200 { return fmt.Errorf("expected 200, got %d", resp.StatusCode) }
		return nil
	})

	add("admin_add_provider", func() error {
		body := strings.NewReader(`{"name":"st-p","kind":"openai_compatible","base_url":"https://t.example.com","api_key":"sk","enabled":true}`)
		req, _ := http.NewRequest("POST", base+"/v1/admin/providers", body)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer admin-test")
		resp, err := client.Do(req)
		if err != nil { return err }
		defer resp.Body.Close()
		if resp.StatusCode != 201 { return fmt.Errorf("expected 201, got %d", resp.StatusCode) }
		return nil
	})

	add("admin_remove_provider", func() error {
		req, _ := http.NewRequest("DELETE", base+"/v1/admin/providers/st-p", nil)
		req.Header.Set("Authorization", "Bearer admin-test")
		resp, err := client.Do(req)
		if err != nil { return err }
		defer resp.Body.Close()
		if resp.StatusCode != 200 { return fmt.Errorf("expected 200, got %d", resp.StatusCode) }
		return nil
	})

	add("request_validation", func() error {
		body := strings.NewReader(`{"model":"","messages":[]}`)
		req, _ := http.NewRequest("POST", base+"/v1/chat/completions?provider=deepseek", body)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer sk-test-client")
		resp, err := client.Do(req)
		if err != nil { return err }
		defer resp.Body.Close()
		if resp.StatusCode != 400 { return fmt.Errorf("expected 400, got %d", resp.StatusCode) }
		return nil
	})

	add("chat_nonstream", func() error {
		body := strings.NewReader(`{"model":"deepseek-chat","messages":[{"role":"user","content":"Reply with exactly: hello world"}],"temperature":0}`)
		req, _ := http.NewRequest("POST", base+"/v1/chat/completions?provider=deepseek", body)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer sk-test-client")
		resp, err := client.Do(req)
		if err != nil { return err }
		defer resp.Body.Close()
		if resp.StatusCode != 200 { return fmt.Errorf("expected 200, got %d", resp.StatusCode) }
		var m map[string]any
		json.NewDecoder(resp.Body).Decode(&m)
		choices, _ := m["choices"].([]any)
		if len(choices) == 0 { return fmt.Errorf("no choices") }
		c := choices[0].(map[string]any)
		msg := c["message"].(map[string]any)
		content := strings.ToLower(msg["content"].(string))
		if !strings.Contains(content, "hello") { return fmt.Errorf("unexpected: %s", content) }
		return nil
	})

	add("stats_after_chat", func() error {
		resp, err := client.Get(base + "/v1/stats")
		if err != nil { return err }
		defer resp.Body.Close()
		var m map[string]any
		json.NewDecoder(resp.Body).Decode(&m)
		if reqs, ok := m["requests_by_model"].(map[string]any); !ok || len(reqs) == 0 {
			return fmt.Errorf("requests_by_model empty")
		}
		if tp, ok := m["tokens_prompt"].(map[string]any); !ok || len(tp) == 0 {
			return fmt.Errorf("tokens_prompt empty")
		}
		if tc, ok := m["tokens_completion"].(map[string]any); !ok || len(tc) == 0 {
			return fmt.Errorf("tokens_completion empty")
		}
		return nil
	})

	add("prometheus_metrics", func() error {
		resp, err := client.Get(base + "/v1/metrics")
		if err != nil { return err }
		defer resp.Body.Close()
		if resp.StatusCode != 200 { return fmt.Errorf("expected 200, got %d", resp.StatusCode) }
		return nil
	})

	add("usage", func() error {
		req, _ := http.NewRequest("GET", base+"/v1/usage", nil)
		req.Header.Set("Authorization", "Bearer sk-test-client")
		resp, err := client.Do(req)
		if err != nil { return err }
		defer resp.Body.Close()
		if resp.StatusCode != 200 { return fmt.Errorf("expected 200, got %d", resp.StatusCode) }
		return nil
	})

	time.Sleep(200 * time.Millisecond)

	var passed, failed int
	for _, r := range report.Results {
		if r.Passed { passed++ } else { failed++ }
	}
	report.Summary = ReportSummary{Total: len(report.Results), Passed: passed, Failed: failed}

	fmt.Println("\n========================================")
	fmt.Println("  GATEWAY RUNTIME — SELF TEST REPORT")
	fmt.Println("========================================")
	for _, r := range report.Results {
		status := "PASS"
		if !r.Passed { status = "FAIL" }
		fmt.Printf("  [%s] %s", status, r.Name)
		if r.Error != "" { fmt.Printf(" — %s", r.Error) }
		fmt.Println()
	}
	fmt.Println("----------------------------------------")
	fmt.Printf("  Total: %d  Passed: %d  Failed: %d\n", report.Summary.Total, passed, failed)
	fmt.Println("========================================")

	data, _ := json.MarshalIndent(report, "", "  ")
	fmt.Fprintf(os.Stderr, "\n%s\n", string(data))

	os.Exit(0)
}
