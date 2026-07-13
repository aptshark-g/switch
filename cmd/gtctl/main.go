package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
)

const usage = `gtctl 鈥?Gateway Runtime CLI

Commands:
  health [-v]                         Health check (use -v for detail)
  providers                           List all providers
  models [provider]                   List available models (all providers, or specify one)
  chat [-p provider] [-m model] "msg" Single chat completion
  chat -i [-p provider]                Interactive chat REPL
  stats                                Show metrics snapshot
  metrics                              Show Prometheus metrics
  usage                                Show token usage
  reload                               Trigger config hot reload (needs --admin)

Global flags:
  --base URL       Gateway URL (default: http://localhost:8080)
  --token KEY      API key for protected endpoints
  --admin TOKEN    Admin token for admin endpoints
`

var (
	baseURL    = "http://localhost:8080"
	apiToken   = ""
	adminToken = ""
)

func main() {
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	baseFlag := flag.String("base", "http://localhost:8080", "")
	tokenFlag := flag.String("token", "", "")
	adminFlag := flag.String("admin", "", "")
	flag.Parse()

	baseURL = strings.TrimRight(*baseFlag, "/")
	apiToken = *tokenFlag
	adminToken = *adminFlag
	args := flag.Args()

	if len(args) == 0 { fmt.Fprint(os.Stderr, usage); os.Exit(1) }

	switch args[0] {
	case "health": cmdHealth(args[1:])
	case "providers": pretty(getJSON("/v1/providers"))
	case "models": cmdModels(args[1:])
	case "chat": cmdChat(args[1:])
	case "stats": pretty(getJSON("/v1/stats"))
	case "metrics": cmdMetrics()
	case "usage": pretty(getJSON("/v1/usage"))
	case "reload": cmdReload()
	default: fatalf("unknown command: %s", args[0])
	}
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

func do(method, path string, body io.Reader) *http.Response {
	req, _ := http.NewRequest(method, baseURL+path, body)
	req.Header.Set("Content-Type", "application/json")
	tok := apiToken
	if adminToken != "" && strings.HasPrefix(path, "/v1/admin") { tok = adminToken }
	if tok != "" { req.Header.Set("Authorization", "Bearer "+tok) }
	resp, err := http.DefaultClient.Do(req)
	if err != nil { fatalf("request failed: %v", err) }
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body); resp.Body.Close()
		fatalf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return resp
}

func getJSON(path string) map[string]any {
	resp := do("GET", path, nil)
	defer resp.Body.Close()
	var m map[string]any
	json.NewDecoder(resp.Body).Decode(&m)
	return m
}

func pretty(v any) { b, _ := json.MarshalIndent(v, "", "  "); fmt.Println(string(b)) }
func fatalf(format string, args ...any) { fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...); os.Exit(1) }

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

func cmdHealth(args []string) {
	if len(args) > 0 && args[0] == "-v" { pretty(getJSON("/v1/health/detail")); return }
	pretty(getJSON("/v1/health"))
}

func cmdModels(args []string) {
	data := getJSON("/v1/providers")
	providers, _ := data["providers"].([]any)

	// Also fetch from config file by calling health/detail which lists all known providers
	detail := getJSON("/v1/health/detail")
	allProvs, _ := detail["providers"].([]any)

	allModels := make(map[string]map[string]bool)
	for _, p := range allProvs {
		pm := p.(map[string]any)
		pname := pm["name"].(string)
		modelsRaw := pm["models"]
		if modelsRaw == nil { continue }

		allModels[pname] = make(map[string]bool)
		switch v := modelsRaw.(type) {
		case []any:
			for _, m := range v { allModels[pname][fmt.Sprint(m)] = true }
		case string:
			for _, m := range strings.Fields(v) { allModels[pname][m] = true }
		}
	}

	// Mark which are currently active
	activeSet := make(map[string]bool)
	for _, p := range providers {
		activeSet[p.(map[string]any)["name"].(string)] = true
	}

	filter := ""
	if len(args) > 0 { filter = args[0] }

	fmt.Println("Available models:")
	for pname, models := range allModels {
		if filter != "" && pname != filter { continue }
		status := "active"
		if !activeSet[pname] { status = "disabled" }
		fmt.Printf("  %s [%s]:\n", pname, status)
		sorted := make([]string, 0, len(models))
		for m := range models { sorted = append(sorted, m) }
		sort.Strings(sorted)
		for _, m := range sorted { fmt.Printf("    - %s\n", m) }
	}
}

func cmdChat(args []string) {
	provider := ""
	model := ""
	interactive := false

	for len(args) > 0 && strings.HasPrefix(args[0], "-") {
		switch args[0] {
		case "-p":
			if len(args) < 2 { fatalf("-p requires a provider name") }
			provider = args[1]; args = args[2:]
		case "-m":
			if len(args) < 2 { fatalf("-m requires a model name") }
			model = args[1]; args = args[2:]
		case "-i":
			interactive = true; args = args[1:]
		default:
			fatalf("unknown flag: %s", args[0])
		}
	}

	// Auto-detect provider if not specified.
	if provider == "" {
		provs := getJSON("/v1/providers")
		if pl, ok := provs["providers"].([]any); ok && len(pl) > 0 {
			provider = pl[0].(map[string]any)["name"].(string)
		}
	}
	if provider == "" { fatalf("no provider available") }

	if interactive {
		interactiveChat(provider, model)
		return
	}

	message := strings.Join(args, " ")
	if message == "" { fatalf("usage: gtctl chat [-p provider] [-m model] <message>") }
	sendChat(provider, model, message)
}

func sendChat(provider, model, message string) {
	path := "/v1/chat/completions?provider=" + provider
	req := map[string]any{"messages": []map[string]string{{"role": "user", "content": message}}, "stream": false}
	if model != "" { req["model"] = model }
	body, _ := json.Marshal(req)
	resp := do("POST", path, strings.NewReader(string(body)))
	defer resp.Body.Close()
	var m map[string]any
	json.NewDecoder(resp.Body).Decode(&m)
	choices, _ := m["choices"].([]any)
	if len(choices) > 0 {
		c := choices[0].(map[string]any)
		msg := c["message"].(map[string]any)
		fmt.Println(msg["content"])
	}
}

func interactiveChat(provider, model string) {
	fmt.Printf("gtctl chat 鈥?provider: %s", provider)
	if model != "" { fmt.Printf(", model: %s", model) }
	fmt.Println("\n  /exit, /models, /model <name>, /provider <name>")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() { break }
		line := strings.TrimSpace(scanner.Text())
		if line == "" { continue }

		switch {
		case line == "/exit" || line == "/quit":
			fmt.Println("bye"); return
		case line == "/models":
			cmdModels([]string{provider})
		case strings.HasPrefix(line, "/model "):
			model = strings.TrimSpace(line[7:])
			fmt.Printf("model: %s\n", model)
		case strings.HasPrefix(line, "/provider "):
			provider = strings.TrimSpace(line[10:])
			fmt.Printf("provider: %s\n", provider)
		default:
			sendChat(provider, model, line)
			fmt.Println()
		}
	}
}

func cmdMetrics() {
	resp := do("GET", "/v1/metrics", nil)
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
}

func cmdReload() {
	resp := do("POST", "/v1/admin/reload", nil)
	defer resp.Body.Close()
	var m map[string]any; json.NewDecoder(resp.Body).Decode(&m); pretty(m)
}

