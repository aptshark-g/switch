package server

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/aptshark/gateway/provider"
)

// handleAdminProviders dispatches to list/add/remove based on method and path.
func (s *Server) handleAdminProviders(w http.ResponseWriter, r *http.Request) {
	// /v1/admin/providers/{name}
	name := strings.TrimPrefix(r.URL.Path, "/v1/admin/providers"); name = strings.TrimPrefix(name, "/")

	switch {
	case r.Method == http.MethodPost && name == "":
		s.handleAdminAddProvider(w, r)
	case r.Method == http.MethodDelete && name != "":
		s.handleAdminRemoveProvider(w, r, name)
	case r.Method == http.MethodPut && name != "":
		s.handleAdminEditProvider(w, r, name)
	case r.Method == http.MethodGet && name == "":
		writeJSON(w, http.StatusOK, map[string]any{"providers": s.manager.List()})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "use POST to add, DELETE /{name} to remove"})
	}
}

func (s *Server) handleAdminAddProvider(w http.ResponseWriter, r *http.Request) {
	var cfg provider.ProviderConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
		return
	}
	kind := strings.ToLower(strings.TrimSpace(cfg.Kind))
	if kind == "" {
		kind = "openai_compatible"
	}
	cfg.Kind = kind
	if cfg.Name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "provider name is required"})
		return
	}
	if cfg.BaseURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "base_url is required"})
		return
	}
	if cfg.TimeoutMs <= 0 {
		cfg.TimeoutMs = 30000
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 2
	}

	if _, err := s.manager.Register(cfg); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	log.Printf("admin: added provider %q (kind=%s)", cfg.Name, cfg.Kind)
	writeJSON(w, http.StatusCreated, map[string]any{
		"status":   "added",
		"provider": cfg.Name,
	})
}

func (s *Server) handleAdminRemoveProvider(w http.ResponseWriter, r *http.Request, name string) {
	s.manager.Unregister(name)
	log.Printf("admin: removed provider %q", name)
	writeJSON(w, http.StatusOK, map[string]string{
		"status":   "removed",
		"provider": name,
	})
}

func (s *Server) handleAdminEditProvider(w http.ResponseWriter, r *http.Request, name string) {
	var cfg provider.ProviderConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	cfg.Name = name
	s.manager.Unregister(name)
	if _, err := s.manager.Register(cfg); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	s.persistProviderToYAML(cfg)
	log.Printf("admin: edited provider %q (kind=%s) — persisted to YAML", name, cfg.Kind)
	writeJSON(w, http.StatusOK, map[string]any{"status": "edited", "provider": name, "persisted": true})
}

// providerYAMLRoot mirrors the structure of provider.yaml for safe parsing.
type providerYAMLRoot struct {
	Providers []providerConfigYAML `yaml:"providers"`
}

type providerConfigYAML struct {
	Name     string            `yaml:"name"`
	Kind     string            `yaml:"kind"`
	BaseURL  string            `yaml:"base_url"`
	APIKey   string            `yaml:"api_key,omitempty"`
	Models   []string          `yaml:"models,omitempty,flow"`
	Enabled  bool              `yaml:"enabled,omitempty"`
	// Preserve unknown fields by keeping the original node
	extra map[string]interface{} `yaml:",inline"`
}

// persistProviderToYAML updates the api_key for a given provider in provider.yaml using proper YAML parsing.
func (s *Server) persistProviderToYAML(cfg provider.ProviderConfig) {
	configPath := "provider.yaml"

	// Read and parse
	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Printf("persist: read YAML: %v", err)
		return
	}

	// Backup original
	os.WriteFile(configPath+".bak", data, 0644)

	var root providerYAMLRoot
	if err := yaml.Unmarshal(data, &root); err != nil {
		log.Printf("persist: parse YAML: %v", err)
		return
	}

	// Find and update the provider
	found := false
	for i := range root.Providers {
		if root.Providers[i].Name == cfg.Name {
			root.Providers[i].APIKey = cfg.APIKey
			found = true
			break
		}
	}

	if !found {
		log.Printf("persist: provider %s not found in %s", cfg.Name, configPath)
		return
	}

	// Marshal back
	out, err := yaml.Marshal(&root)
	if err != nil {
		log.Printf("persist: marshal YAML: %v", err)
		return
	}

	if err := os.WriteFile(configPath, out, 0644); err != nil {
		log.Printf("persist: write YAML: %v", err)
		return
	}
	log.Printf("persist: wrote api_key for %s to %s", cfg.Name, configPath)
}

// handleAdminReload — 配置热重载确认端点（watcher 本就每 5s 自动重载,
// 此端点用于显式触发/确认, 兼容 gtctl/gtui）。
func (s *Server) handleAdminReload(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"note":   "watcher auto-reloads provider.yaml every 5s"})
}
