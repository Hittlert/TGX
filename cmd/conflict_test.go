package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Hittlert/TGX/app/daemon"
)

func TestCLI_ConflictCommand(t *testing.T) {
	// Setup mock daemon server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer test-token" {
			http.Error(w, `{"ok":false,"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}

		switch r.URL.Path {
		case "/api/conflicts":
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			records := []daemon.DownloadRecord{
				{
					ChatID:     "-100123456",
					MessageID:  42,
					Status:     "conflict",
					FileName:   "test_video.mp4",
					SavePath:   "/downloads/test_video.mp4",
					FileSize:   1048576,
					Error:      "[target_conflict] target file exists",
					ErrorClass: "target_conflict",
				},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(records)

		case "/api/conflicts/resolve":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var req map[string]any
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if req["chat_id"] != "-100123456" || req["message_id"] != float64(42) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "status": "pending"})

		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	// 1. Test `tgx conflict list`
	cmd := NewConflict()
	cmd.SetArgs([]string{"list", "--url", server.URL, "--token", "test-token"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("conflict list failed: %v", err)
	}

	// 2. Test `tgx conflict list --json`
	cmd = NewConflict()
	cmd.SetArgs([]string{"list", "--url", server.URL, "--token", "test-token", "--json"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("conflict list --json failed: %v", err)
	}

	// 3. Test `tgx conflict resolve` with flags
	cmd = NewConflict()
	cmd.SetArgs([]string{"resolve", "--url", server.URL, "--token", "test-token", "--chat", "-100123456", "--msg", "42"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("conflict resolve with flags failed: %v", err)
	}

	// 4. Test `tgx conflict resolve` with POSIX '--' delimiter for negative chat ID
	cmd = NewConflict()
	cmd.SetArgs([]string{"resolve", "--url", server.URL, "--token", "test-token", "--", "-100123456", "42"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("conflict resolve with POSIX delimiter failed: %v", err)
	}

	// 5. Test `tgx conflict resolve` with invalid message_id
	cmd = NewConflict()
	cmd.SetArgs([]string{"resolve", "--url", server.URL, "--token", "test-token", "--", "-100123456", "not-a-number"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error for non-integer message_id, got nil")
	}

	// 6. Test unauthenticated request returns error
	cmd = NewConflict()
	cmd.SetArgs([]string{"list", "--url", server.URL, "--token", "wrong-token"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error for unauthorized request, got nil")
	}
}
