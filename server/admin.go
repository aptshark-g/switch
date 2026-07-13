package server

import (
	"log"
	"net/http"
)

func (s *Server) handleAdminReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.watcher == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "hot reload not configured"})
		return
	}
	events, err := s.watcher.ReloadNow()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	for _, ev := range events {
		if ev.Action == "added" {
			if _, err := s.manager.Register(ev.Provider); err != nil {
				log.Printf("reload: register %s: %v", ev.Provider.Name, err)
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "reloaded",
		"providers": s.manager.List(),
	})
}
