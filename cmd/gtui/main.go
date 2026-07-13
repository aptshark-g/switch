package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aptshark/gateway/i18n"
)

var stdin = bufio.NewScanner(os.Stdin)
var (
	baseURL, apiToken, adminToken, curProvider, curModel string
	clr, reset, cyan, green, yellow, red, dim, bold      string
)

func main() {
	bf := flag.String("base", "http://localhost:8080", ""); tf := flag.String("token", "", ""); af := flag.String("admin", "", "")
	flag.Parse()
	baseURL = strings.TrimRight(*bf, "/"); apiToken = *tf; adminToken = *af
	i18n.SetLang(i18n.DetectLang())
	initTerm()
	pl := getJSON("/v1/providers"); if p, ok := pl["providers"].([]any); ok && len(p) > 0 { curProvider = p[0].(map[string]any)["name"].(string) }
	if curProvider == "" { curProvider = "deepseek" }
	mainMenu()
}

func initTerm() {
	clr, reset, cyan, green, yellow, red, dim, bold = "\033[2J\033[H", "\033[0m", "\033[36m", "\033[32m", "\033[33m", "\033[31m", "\033[2m", "\033[1m"
	if os.Getenv("WT_SESSION") == "" && os.Getenv("TERM_PROGRAM") == "" && os.Getenv("TERM") == "" { clr, reset, cyan, green, yellow, red, dim, bold = "", "", "", "", "", "", "", "" }
}

func do(m, p string, b io.Reader) *http.Response {
	r, _ := http.NewRequest(m, baseURL+p, b); r.Header.Set("Content-Type", "application/json")
	t := apiToken; if adminToken != "" && strings.HasPrefix(p, "/v1/admin") { t = adminToken }
	if t != "" { r.Header.Set("Authorization", "Bearer "+t) }; resp, _ := http.DefaultClient.Do(r); return resp
}

func getJSON(p string) map[string]any {
	r := do("GET", p, nil); if r == nil { return nil }; defer r.Body.Close(); var m map[string]any; json.NewDecoder(r.Body).Decode(&m); return m
}

func sendChat(pv, md, msg string) string {
	b, _ := json.Marshal(map[string]any{"messages": []map[string]string{{"role": "user", "content": msg}}, "stream": false, "model": md})
	r := do("POST", "/v1/chat/completions?provider="+pv, strings.NewReader(string(b)))
	if r == nil { return "(err)" }; defer r.Body.Close(); var m map[string]any; json.NewDecoder(r.Body).Decode(&m)
	if e, ok := m["error"].(string); ok { return e }
	if c, ok := m["choices"].([]any); ok && len(c) > 0 { if mm, ok := c[0].(map[string]any)["message"].(map[string]any); ok { return mm["content"].(string) } }
	return ""
}

func clear() { fmt.Print(clr) }
func input(p string) string { fmt.Print(p); if stdin.Scan() { return strings.TrimSpace(stdin.Text()) }; return "" }
func readInt(max int) int { for { s := input(fmt.Sprintf("  [0-%d] > ", max)); if s == "0" || s == "" { return 0 }; n, e := strconv.Atoi(s); if e == nil && n >= 1 && n <= max { return n }; fmt.Println("  "+i18n.T("invalid")) } }

func statusBar() {
	s := getJSON("/v1/stats"); if s == nil { return }
	rq, tk := int64(0), int64(0)
	if m, ok := s["requests_by_provider"].(map[string]any); ok { for _, v := range m { rq += int64(v.(float64)) } }
	if m, ok := s["tokens_prompt"].(map[string]any); ok { for _, v := range m { tk += int64(v.(float64)) } }
	if m, ok := s["tokens_completion"].(map[string]any); ok { for _, v := range m { tk += int64(v.(float64)) } }
	u := 0.0; if v, ok := s["uptime_seconds"].(float64); ok { u = v }
	ks := ""; pl := getJSON("/v1/providers"); if pv, ok := pl["providers"].([]any); ok { for _, p := range pv { pm := p.(map[string]any); if pm["name"] == curProvider { if k, ok := pm["key_configured"].(bool); ok && k { ks = "[key]" } else { ks = "[no key]" }; break } } }
	pv := fmt.Sprintf("%-18s", curProvider); md := fmt.Sprintf("%-12s", curModel)
	fmt.Printf("%s+----------------------------------------------------+%s\n", dim, reset)
	fmt.Printf("%s|%s %s%s                       %s|%s\n", dim, reset, bold, i18n.T("title"), reset, dim, reset)
	fmt.Printf("%s|%s %s: %s%s%s  %s: %s%s%s %s|%s\n", dim, reset, i18n.T("provider"), green, pv, reset, i18n.T("model"), green, md, reset, dim, reset)
	fmt.Printf("%s|%s %s: %s%-5ds%s  %s: %s%-4d%s  %s: %s%-6d%s %s|%s\n", dim, reset, i18n.T("uptime"), green, int(u), reset, i18n.T("reqs"), yellow, rq, reset, i18n.T("tokens"), cyan, tk, reset, dim, reset)
	fmt.Printf("%s+----------------------------------------------------+%s\n", dim, reset)
}

func mainMenu() {
	for { clear(); statusBar(); fmt.Println()
		fmt.Printf("  %s1.%s %s\n", bold, reset, i18n.T("chat"))
		fmt.Printf("  %s2.%s %s\n", bold, reset, i18n.T("provider_manager"))
		fmt.Printf("  %s3.%s %s\n", bold, reset, i18n.T("switch_model"))
		fmt.Printf("  %s4.%s %s\n", bold, reset, i18n.T("view_stats"))
		fmt.Printf("  %s5.%s %s\n", bold, reset, i18n.T("diagnostics"))
		fmt.Printf("  %s6.%s %s\n", bold, reset, i18n.T("reload_config"))
		fmt.Printf("  %s7.%s %s\n", bold, reset, i18n.T("switch_lang"))
		fmt.Printf("  %s8.%s %s\n", bold, reset, i18n.T("exit"))
		switch readInt(8) {
		case 1: chatMode(); case 2: providerManager(); case 3: switchModel(); case 4: viewStats(); case 5: showDiag(); case 6: adminReload(); case 7: langMenu(); case 8: clear(); fmt.Println(i18n.T("bye")); return
		}
	}
}

func providerManager() {
	for {
		clear()
		fmt.Printf("%s=== %s%s\n", bold, i18n.T("provider_manager"), reset)
		fmt.Printf("  [0] %s\n\n", i18n.T("back_to_menu"))
		pl := getJSON("/v1/providers"); if pl == nil { fmt.Println("  "+i18n.T("no_providers")); input(i18n.T("press_enter")); return }
		pv, _ := pl["providers"].([]any)
		fmt.Printf("  %-3s %-20s %-8s %-8s %s\n", "#", "Name", "Active", "Key", "Models")
		for i, p := range pv { pm := p.(map[string]any); ac, kc := "no", "no"; if v, _ := pm["active"].(bool); v { ac = "ACTIVE" }; if v, _ := pm["key_configured"].(bool); v { kc = "set" }; ms := ""; if v, ok := pm["models"].(string); ok { ms = v }; if v, ok := pm["models"].([]any); ok { for _, m := range v { ms += fmt.Sprint(m) + " " } }; ms = strings.TrimSpace(ms); if len(ms) > 30 { ms = ms[:27] + "..." }
			fmt.Printf("  %-3d %s%-20s%s %s%-8s%s %s%-8s%s %s\n", i+1, green, pm["name"], reset, dim+ac+reset, green, kc, reset, dim+kc+reset, reset, ms)
		}
		fmt.Printf("\n  [A] %s  [K] %s  [E] %s  [D] %s  [N] %s\n", i18n.T("activate"), i18n.T("add_key"), i18n.T("edit"), i18n.T("delete"), i18n.T("add_new"))
		fmt.Printf("  [S] %s  [T] %s\n", i18n.T("select"), i18n.T("test"))
		c := input(fmt.Sprintf("  [0/%s] > ", "A/K/E/D/N/S/T"))
		switch strings.ToUpper(c) {
		case "A": activateProvider(pv)
		case "K": addKeyProvider(pv)
		case "E": editProviderForm(pv)
		case "D": deleteProvider(pv)
		case "N": addNewProvider()
		case "S": n := readInt(len(pv)); if n > 0 { curProvider = pv[n-1].(map[string]any)["name"].(string); curModel = ""; return }
		case "T": testProvider(pv)
		case "0", "": return
		}
	}
}

func testOne(name string) bool {
	r := do("GET", "/v1/chat/completions?provider="+name, strings.NewReader(`{"model":"","messages":[{"role":"user","content":"hi"}],"stream":false}`))
	if r == nil { return false }; defer r.Body.Close(); var m map[string]any; json.NewDecoder(r.Body).Decode(&m); return m["error"] == nil
}

func activateProvider(pv []any) {
	clear(); fmt.Printf("%s=== %s%s\n\n", bold, i18n.T("activate"), reset)
	n := readInt(len(pv)); if n == 0 { return }
	nm := pv[n-1].(map[string]any)["name"].(string)
	hasKey, _ := pv[n-1].(map[string]any)["key_configured"].(bool)
	if !hasKey {
		fmt.Println("  " + i18n.T("need_key_first"))
		k := input("  API Key: "); if k == "" { return }
		do("PUT", "/v1/admin/providers/"+nm, strings.NewReader(`{"api_key":"`+k+`","enabled":true}`))
	} else {
		do("PUT", "/v1/admin/providers/"+nm, strings.NewReader(`{"enabled":true}`))
	}
	curProvider = nm
	fmt.Println("  " + i18n.T("testing"))
	if testOne(nm) {
		fmt.Println("  " + green + "OK" + reset)
	} else {
		fmt.Println("  " + red + i18n.T("test_failed") + reset)
	}
	time.Sleep(1 * time.Second)
}
func addKeyProvider(pv []any) { clear(); fmt.Printf("%s=== %s%s\n\n", bold, i18n.T("add_key"), reset); n := readInt(len(pv)); if n == 0 { return }; nm := pv[n-1].(map[string]any)["name"].(string); k := input("  API Key: "); if k == "" { return }; b, _ := json.Marshal(map[string]any{"api_key": k, "enabled": true}); do("PUT", "/v1/admin/providers/"+nm, strings.NewReader(string(b))); curProvider = nm; fmt.Println("  key saved"); time.Sleep(500 * time.Millisecond) }
func editProviderForm(pv []any) { clear(); fmt.Printf("%s=== %s%s\n\n", bold, i18n.T("edit"), reset); n := readInt(len(pv)); if n == 0 { return }; nm := pv[n-1].(map[string]any)["name"].(string); b := input("  Base URL: "); k := input("  API Key: "); m := input("  Models (csv): "); e := input("  Enabled (y/n): "); en := e == "y" || e == "Y"; md := strings.Split(m, ","); for i := range md { md[i] = strings.TrimSpace(md[i]) }; body, _ := json.Marshal(map[string]any{"base_url": b, "api_key": k, "models": md, "enabled": en}); do("PUT", "/v1/admin/providers/"+nm, strings.NewReader(string(body))); curProvider = nm; fmt.Println("  updated"); time.Sleep(500 * time.Millisecond) }
func deleteProvider(pv []any) { clear(); fmt.Printf("%s=== %s%s\n\n", bold, i18n.T("delete"), reset); n := readInt(len(pv)); if n == 0 { return }; nm := pv[n-1].(map[string]any)["name"].(string); do("DELETE", "/v1/admin/providers/"+nm, nil); fmt.Println("  deleted"); time.Sleep(500 * time.Millisecond) }
func addNewProvider() { clear(); fmt.Printf("%s=== %s%s\n\n", bold, i18n.T("add_new"), reset); nm := input("  Name: "); if nm == "" { return }; b := input("  Base URL: "); k := input("  API Key: "); m := input("  Models (csv): "); md := strings.Split(m, ","); for i := range md { md[i] = strings.TrimSpace(md[i]) }; body, _ := json.Marshal(map[string]any{"name": nm, "kind": "openai_compatible", "base_url": b, "api_key": k, "models": md, "enabled": true}); do("POST", "/v1/admin/providers", strings.NewReader(string(body))); curProvider = nm; fmt.Println("  added"); time.Sleep(500 * time.Millisecond) }
func testProvider(pv []any) { clear(); fmt.Printf("%s=== %s%s\n\n", bold, i18n.T("test"), reset); n := readInt(len(pv)); if n == 0 { return }; nm := pv[n-1].(map[string]any)["name"].(string); fmt.Printf("  Testing %s...\n", nm); r := do("GET", "/v1/chat/completions?provider="+nm, strings.NewReader(`{"model":"","messages":[{"role":"user","content":"hi"}],"stream":false}`)); if r == nil { fmt.Println("  "+red+"no response"+reset) } else { defer r.Body.Close(); var m map[string]any; json.NewDecoder(r.Body).Decode(&m); if m["error"] != nil { fmt.Printf("  %sFAIL: %v%s\n", red, m["error"], reset) } else { fmt.Printf("  %sOK%s\n", green, reset) } }; input(i18n.T("press_enter")) }

func langMenu() { clear(); fmt.Printf("%s=== %s%s\n  [0] %s\n\n", bold, i18n.T("select_lang"), reset, i18n.T("back_to_menu")); ls := i18n.List(); for i, l := range ls { fmt.Printf("  %d. %s\n", i+1, l) }; if n := readInt(len(ls)); n > 0 { i18n.SetLang(ls[n-1]); fmt.Println("  "+i18n.T("lang_switched")); time.Sleep(500 * time.Millisecond) } }
func chatMode() { clear(); fmt.Printf("%s=== %s [%s / %s]%s\n  [0] %s\n", bold, i18n.T("chat"), curProvider, curModel, reset, i18n.T("back_to_menu")); for { fmt.Print("> "); if !stdin.Scan() { break }; l := strings.TrimSpace(stdin.Text()); if l == "" { continue }; if l == "0" || l == "/back" { return }; fmt.Println("  "+sendChat(curProvider, curModel, l)) } }
func switchModel() { clear(); fmt.Printf("%s=== %s [%s]%s\n  [0] %s\n\n", bold, i18n.T("switch_model"), curProvider, reset, i18n.T("back_to_menu")); d := getJSON("/v1/health/detail"); if d == nil { input(i18n.T("press_enter")); return }; var ms []string; for _, p := range d["providers"].([]any) { pm := p.(map[string]any); if pm["name"] != curProvider { continue }; switch v := pm["models"].(type) { case []any: for _, m := range v { ms = append(ms, fmt.Sprint(m)) }; case string: ms = strings.Fields(v) }; break }; if len(ms) == 0 { fmt.Println("  "+i18n.T("no_models")); input(i18n.T("press_enter")); return }; for i, m := range ms { fmt.Printf("  %d. %s\n", i+1, m) }; fmt.Println(); if n := readInt(len(ms)); n > 0 { curModel = ms[n-1] } }
func viewStats() { for { clear(); fmt.Printf("%s=== %s%s\n  [0] %s  [q]\n\n", bold, i18n.T("view_stats"), reset, i18n.T("back_to_menu")); b, _ := json.MarshalIndent(getJSON("/v1/stats"), "", "  "); fmt.Println(string(b)); fmt.Printf("\n  %s[0/q]%s", dim, reset); done := make(chan bool); go func() { stdin.Scan(); if s := strings.TrimSpace(stdin.Text()); s == "q" || s == "0" { done <- true } }(); select { case <-done: return; case <-time.After(3 * time.Second): } } }
func showDiag() { clear(); fmt.Printf("%s=== %s%s\n  [0] %s\n\n", bold, i18n.T("diagnostics"), reset, i18n.T("back_to_menu")); b, _ := json.MarshalIndent(getJSON("/v1/diagnostics"), "", "  "); fmt.Println(string(b)); fmt.Println(); input(i18n.T("press_enter")) }
func adminReload() { clear(); fmt.Printf("%s=== %s%s\n  [0] %s\n\n", bold, i18n.T("reload_config"), reset, i18n.T("back_to_menu")); r, _ := http.NewRequest("POST", baseURL+"/v1/admin/reload", nil); t := apiToken; if adminToken != "" { t = adminToken }; r.Header.Set("Authorization", "Bearer "+t); re, e := http.DefaultClient.Do(r); if e != nil { fmt.Printf("  %serr: %v%s\n", red, e, reset) } else if re.StatusCode != 200 { b, _ := io.ReadAll(re.Body); re.Body.Close(); fmt.Printf("  %s%d: %s%s\n", red, re.StatusCode, string(b), reset) } else { fmt.Printf("  %s%s%s\n", green, i18n.T("reloaded"), reset); re.Body.Close() }; input(i18n.T("press_enter")) }
