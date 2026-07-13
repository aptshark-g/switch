package server

import (
	"net/http"
)

// handleDiagnostics returns detailed internal state for all providers.
func (s *Server) handleDiagnostics(w http.ResponseWriter, r *http.Request) {
	diag := s.manager.Diagnostics()
	diag["uptime_seconds"] = s.metrics.Uptime()
	diag["active_connections"] = s.metrics.Snapshot().ActiveConnections
	diag["cache_entries"] = s.cache.Size()
	diag["load_shed_inflight"] = s.shedder.InFlight()
	diag["load_shed_total"] = s.shedder.Shed()
	writeJSON(w, http.StatusOK, diag)
}
