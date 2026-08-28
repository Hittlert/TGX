package daemon

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gorilla/mux"
)

type controlRequest struct {
	Action string `json:"action"`
}

func NewHandler(registry *Registry, optAccess ...*telegramMediaAccess) http.Handler {
	var access *telegramMediaAccess
	if len(optAccess) > 0 {
		access = optAccess[0]
	}
	router := mux.NewRouter()
	router.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "ok\n")
	})
	router.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "GET required")
			return
		}
		writeJSON(w, http.StatusOK, registry.Status())
	})
	router.HandleFunc("/api/dialogs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "GET required")
			return
		}
		if access == nil {
			writeError(w, http.StatusServiceUnavailable, "telegram access not available")
			return
		}
		dialogs, err := access.GetDialogs(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":      true,
			"dialogs": dialogs,
		})
	})
	router.HandleFunc("/api/chat/history", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "POST required")
			return
		}
		if access == nil {
			writeError(w, http.StatusServiceUnavailable, "telegram access not available")
			return
		}
		var request HistoryRequest
		if err := decodeJSON(w, r, &request); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		messages, err := access.GetHistory(r.Context(), request)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":       true,
			"messages": messages,
		})
	})
	router.HandleFunc("/api/chat/resolve", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "POST required")
			return
		}
		if access == nil {
			writeError(w, http.StatusServiceUnavailable, "telegram access not available")
			return
		}
		var request ResolveRequest
		if err := decodeJSON(w, r, &request); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		dialog, err := access.ResolvePeerInfo(r.Context(), request.Query)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":   true,
			"peer": dialog,
		})
	})
	router.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "POST required")
			return
		}
		var request TaskRequest
		if err := decodeJSON(w, r, &request); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		task, created, err := registry.Submit(request)
		if err != nil {
			switch {
			case errors.Is(err, ErrQueueFull):
				writeError(w, http.StatusTooManyRequests, err.Error())
			case errors.Is(err, ErrIDConflict):
				writeError(w, http.StatusConflict, err.Error())
			default:
				writeError(w, http.StatusBadRequest, err.Error())
			}
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusAccepted
		}
		writeJSON(w, status, task)
	})
	router.HandleFunc("/api/tasks/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "GET required")
			return
		}
		task, ok := registry.Task(mux.Vars(r)["id"])
		if !ok {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		writeJSON(w, http.StatusOK, task)
	})
	router.HandleFunc("/api/control", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "POST required")
			return
		}
		var request controlRequest
		if err := decodeJSON(w, r, &request); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		switch request.Action {
		case "pause":
			registry.SetPaused(true)
		case "resume":
			registry.SetPaused(false)
		default:
			writeError(w, http.StatusBadRequest, "action must be pause or resume")
			return
		}
		writeJSON(w, http.StatusOK, registry.Status())
	})
	return router
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
