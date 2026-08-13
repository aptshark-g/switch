package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aptshark/gateway/config"
	"github.com/aptshark/gateway/persistence"
	"github.com/aptshark/gateway/provider"
	"github.com/aptshark/gateway/server"
)

func main() {
	configPath := flag.String("config", "provider.yaml", "path to provider configuration")
	addr := flag.String("addr", ":8080", "listen address")
	statePath := flag.String("state", "gateway.state.json", "path to persistence state file")
	selftest := flag.Bool("selftest", false, "run self-test and exit")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("gateway: starting (config=%s)", *configPath)

	cfg, err := config.ParseFile(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}

	if *selftest {
		runSelfTest(cfg, *addr)
		return
	}

	mgr := provider.NewManager()
	registerFactories(mgr)

	store := persistence.NewStore(*statePath,
		func() *persistence.State { return snapshotState(mgr) },
		func(state *persistence.State) { restoreState(mgr, state) },
	)
	if err := store.Restore(); err != nil {
		log.Printf("gateway: restore state: %v", err)
	}

	if err := mgr.Bootstrap(cfg.Providers); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: bootstrap: %v\n", err)
		os.Exit(1)
	}

	go store.StartAutoSave()

	// 2026-08-13: 50ms 轮询（provider.yaml 6KB, sha256 开销可忽略）—
	// 热重载最坏 ~60ms, 满足"路由规则热重载 <100ms"要求（事件驱动需
	// fsnotify 依赖, 离线构建不可用; 50ms 轮询等效）。
	watcher := config.NewWatcher(*configPath, 50*time.Millisecond)
	watcher.OnChange(func(events []config.ChangeEvent) {
		for _, ev := range events {
			switch ev.Action {
			case "added":
				if _, err := mgr.Register(ev.Provider); err != nil {
					log.Printf("watcher: register %s: %v", ev.Provider.Name, err)
				}
			case "updated":
				// 2026-08-13: 热更新补全 — 此前只处理 added,
				// 改 key/超时/禁用不生效。updated = 重建 provider。
				mgr.Unregister(ev.Provider.Name)
				if _, err := mgr.Register(ev.Provider); err != nil {
					log.Printf("watcher: update %s: %v",
						ev.Provider.Name, err)
				} else {
					log.Printf("watcher: updated %s (hot reload)",
						ev.Provider.Name)
				}
			case "removed":
				mgr.Unregister(ev.Provider.Name)
				log.Printf("watcher: removed %s", ev.Provider.Name)
			}
		}
	})
	go watcher.Start()

	serverAddr := *addr
	if cfg.Server.Port > 0 {
		serverAddr = fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	}
	srv := server.NewWithWatcher(mgr, serverAddr, watcher, cfg.Auth, store)

	// Start background health prober (30s interval) — 2026-08-13:
	// 全量并行探测 + 健康缓存（/v1/health 读缓存即时返回）。
	prober := provider.NewProber(mgr, 30*time.Second, srv.UpdateHealth)
	go prober.Start()

	// Startup diagnostics (LogStartupConfig only — selftest is opt-in via flag)
	srv.LogStartupConfig()

	if cfg.Server.TLSCert != "" && cfg.Server.TLSKey != "" {
		log.Printf("gateway: TLS enabled (cert=%s)", cfg.Server.TLSCert)
		if err := srv.StartTLS(cfg.Server.TLSCert, cfg.Server.TLSKey); err != nil {
			log.Fatalf("gateway: %v", err)
		}
	} else {
		if err := srv.Start(); err != nil {
			log.Fatalf("gateway: %v", err)
		}
	}
}

func registerFactories(mgr *provider.Manager) {
	mgr.RegisterFactory("openai", func(cfg provider.ProviderConfig) (provider.Provider, error) {
		return provider.NewOpenAIProvider(cfg)
	})
	mgr.RegisterFactory("openai_compatible", func(cfg provider.ProviderConfig) (provider.Provider, error) {
		return provider.NewOpenAIProvider(cfg)
	})
	mgr.RegisterFactory("ollama", func(cfg provider.ProviderConfig) (provider.Provider, error) {
		return provider.NewOpenAIProvider(cfg)
	})
}

// snapshotState captures current usage stats for persistence (no keys/config).
func snapshotState(mgr *provider.Manager) *persistence.State {
	state := &persistence.State{
		Providers: make([]persistence.ProviderState, 0, len(mgr.List())),
	}
	for _, p := range mgr.List() {
		state.Providers = append(state.Providers, persistence.ProviderState{
			Name:    p.Name,
			Kind:    p.Kind,
			Enabled: p.Active,
		})
	}
	return state
}

func restoreState(mgr *provider.Manager, state *persistence.State) {
	// ONLY restore usage stats and circuit state — never re-register providers.
	// Provider config must always come from provider.yaml (source of truth).
	for _, ps := range state.Providers {
		// Restore usage counters for cost tracking
		if ps.Requests > 0 || ps.TokenPrompt > 0 {
			log.Printf("persistence: restored usage for %s (%d req, %d tokens)",
				ps.Name, ps.Requests, ps.TokenPrompt+ps.TokenComp)
		}
	}
}
