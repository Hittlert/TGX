package daemon

import (
	"encoding/json"
	"net/http"

	"go.uber.org/zap"
)

// NewHandler constructs a test-friendly handler using the production WebServer route table.
func NewHandler(registry *Registry, optAccess ...TelegramAccess) http.Handler {
	var access TelegramAccess
	if len(optAccess) > 0 {
		access = optAccess[0]
	}
	ws := NewWebServer(nil, nil, nil, nil, nil, access, registry, zap.NewNop(), "")
	return ws.Handler()
}

func decodeJSON(w http.ResponseWriter, r *http.Request, value any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024))
	decoder.DisallowUnknownFields()
	return decoder.Decode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
