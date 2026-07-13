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

	watcher := config.NewWatcher(*configPath, 5*time.Second)
	watcher.OnChange(func(events []config.ChangeEvent) {
		for _, ev := range events {
			if ev.Action == "added" {
				if _, err := mgr.Register(ev.Provider); err != nil {
					log.Printf("watcher: register %s: %v", ev.Provider.Name, err)
				}
			}
		}
	})
	go watcher.Start()

	serverAddr := *addr
	if cfg.Server.Port > 0 {
		serverAddr = fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	}
	srv := server.NewWithWatcher(mgr, serverAddr, watcher, cfg.Auth, store)

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

func snapshotState(mgr *provider.Manager) *persistence.State {
	ps := mgr.List()
	state := &persistence.State{
		Providers: make([]persistence.ProviderState, 0, len(ps)),
	}
	for _, p := range ps {
		state.Providers = append(state.Providers, persistence.ProviderState{
			Name: p.Name, Kind: p.Kind, Enabled: true,
		})
	}
	return state
}

func restoreState(mgr *provider.Manager, state *persistence.State) {
	for _, ps := range state.Providers {
		if !ps.Enabled {
			continue
		}
		cfg := provider.ProviderConfig{
			Name: ps.Name, Kind: ps.Kind, Enabled: true,
		}
		if _, err := mgr.Register(cfg); err != nil {
			log.Printf("persistence: restore provider %s: %v", ps.Name, err)
		}
	}
}
