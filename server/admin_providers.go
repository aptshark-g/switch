package server

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

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
	// Normalise kind.
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

// adminRoutes registers admin management endpoints.
func (s *Server) adminRoutes() {
	s.mux.HandleFunc("/v1/admin/providers", s.handleAdminProviders)
	s.mux.HandleFunc("/v1/admin/providers/", s.handleAdminProviders)

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
	// Persist to provider.yaml so key survives restart
	s.persistProviderToYAML(cfg)
	log.Printf("admin: edited provider %q (kind=%s) — persisted to YAML", name, cfg.Kind)
	writeJSON(w, http.StatusOK, map[string]any{"status": "edited", "provider": name, "persisted": true})
}

// persistProviderToYAML updates the api_key in provider.yaml for a given provider.
func (s *Server) persistProviderToYAML(cfg provider.ProviderConfig) {
	configPath := "provider.yaml" // default; could be made configurable
	data, err := os.ReadFile(configPath)
	if err != nil { log.Printf("persist: read YAML: %v", err); return }

	lines := strings.Split(string(data), "\n")
	inTarget := false
	found := false
	for i, line := range lines {
		if strings.Contains(line, "name: "+cfg.Name) || strings.Contains(line, "name: \""+cfg.Name+"\"") {
			inTarget = true
			continue
		}
		if inTarget && strings.Contains(line, "api_key:") {
			lines[i] = "    api_key: " + cfg.APIKey
			found = true
			inTarget = false
			break
		}
		if inTarget && (strings.HasPrefix(line, "  - name:") || i == len(lines)-1) {
			inTarget = false
		}
	}

	if found {
		// Backup
		backupPath := configPath + ".bak"
		os.WriteFile(backupPath, data, 0644)
		os.WriteFile(configPath, []byte(strings.Join(lines, "\n")), 0644)
		log.Printf("persist: wrote api_key for %s to %s", cfg.Name, configPath)
	} else {
		log.Printf("persist: provider %s not found in %s", cfg.Name, configPath)
	}
}