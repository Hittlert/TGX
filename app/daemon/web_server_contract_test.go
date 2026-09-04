package daemon

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/dcpool"
	"github.com/Hittlert/TGX/core/transfer"
)

func TestWebServer_AuthContract(t *testing.T) {
	registry := NewRegistry(5, 100, time.Now)
	ws := NewWebServer(nil, nil, nil, nil, nil, nil, registry, zap.NewNop(), "mypassword")
	handler := ws.Handler()

	// 1. Unauthenticated request to protected endpoint should redirect to /login
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	handler.ServeHTTP(w, req)
	// /api/status is not protected by requireAuth, let's test a protected route like /
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusFound {
		t.Fatalf("expected redirect to login for protected endpoint, got %d", w.Code)
	}
	if loc := w.Header().Get("Location"); loc != "/login" {
		t.Fatalf("expected redirect to /login, got %s", loc)
	}

	// 2. Malformed JSON login request
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/login", strings.NewReader("not-json"))
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed login body, got %d", w.Code)
	}

	// 3. Wrong password
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"password":"wrong"}`))
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong password, got %d", w.Code)
	}

	// 4. Correct password over HTTPS
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"password":"mypassword"}`))
	req.TLS = &tls.ConnectionState{} // Simulate HTTPS
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for correct login, got %d", w.Code)
	}

	cookies := w.Result().Cookies()
	var sessCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "tg_downloader_session" {
			sessCookie = c
			break
		}
	}
	if sessCookie == nil {
		t.Fatal("session cookie not set after successful login")
	}
	if !sessCookie.HttpOnly {
		t.Error("session cookie should be HttpOnly")
	}
	if sessCookie.SameSite != http.SameSiteLaxMode {
		t.Errorf("expected SameSite=Lax, got %v", sessCookie.SameSite)
	}
	if !sessCookie.Secure {
		t.Error("session cookie should be Secure when served over HTTPS")
	}

	// 5. Access protected endpoint with valid session cookie
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(sessCookie)
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid session, got %d", w.Code)
	}

	// 6. Logout
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/logout", nil)
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusFound {
		t.Fatalf("expected 302 redirect after logout, got %d", w.Code)
	}
	clearedCookie := false
	for _, c := range w.Result().Cookies() {
		if c.Name == "tg_downloader_session" && c.Value == "" {
			clearedCookie = true
			break
		}
	}
	if !clearedCookie {
		t.Fatal("session cookie was not cleared on logout")
	}
}

func TestWebServer_SettingsAndControlContract(t *testing.T) {
	registry := NewRegistry(5, 100, time.Now)
	tm := transfer.NewTransferManager(transfer.Options{FileConcurrency: 5})
	orch := NewOrchestrator(nil, tm, nil, nil, nil, registry, zap.NewNop(), "")

	ws := NewWebServer(nil, tm, nil, nil, orch, nil, registry, zap.NewNop(), "")
	handler := ws.Handler()

	// 1. Control Pause & Resume
	for _, tc := range []struct {
		action        string
		expectedState string
		expectedPause bool
	}{
		{"pause", "paused", true},
		{"resume", "running", false},
	} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/control", strings.NewReader(`{"action":"`+tc.action+`"}`))
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("control %s returned %d", tc.action, w.Code)
		}
		var resp map[string]any
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatal(err)
		}
		if resp["state"] != tc.expectedState {
			t.Fatalf("expected state %s, got %v", tc.expectedState, resp["state"])
		}
		if orch.IsRunning() == tc.expectedPause {
			t.Fatalf("orchestrator running=%v, expected=%v", orch.IsRunning(), !tc.expectedPause)
		}
		if registry.Status().Paused != tc.expectedPause {
			t.Fatalf("registry paused=%v, expected=%v", registry.Status().Paused, tc.expectedPause)
		}
	}

	// 2. Control Invalid Action
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/control", strings.NewReader(`{"action":"invalid"}`))
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid action, got %d", w.Code)
	}

	// 3. Settings Read
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/settings/concurrency", nil)
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("settings get failed: %d", w.Code)
	}
	var getResp struct {
		OK       bool `json:"ok"`
		Settings struct {
			MaxActiveFiles int `json:"max_active_files"`
		} `json:"settings"`
	}
	if err := json.NewDecoder(w.Body).Decode(&getResp); err != nil {
		t.Fatal(err)
	}
	if getResp.Settings.MaxActiveFiles != 5 {
		t.Fatalf("expected 5 max_active_files, got %d", getResp.Settings.MaxActiveFiles)
	}

	// 4. Settings Write and Read-After-Write Consistency
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/settings/concurrency", strings.NewReader(`{"max_active_files": 12}`))
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("settings post returned %d: %s", w.Code, w.Body.String())
	}
	if tm.FileConcurrency() != 12 {
		t.Fatalf("transfer manager capacity not updated, got %d", tm.FileConcurrency())
	}

	// Read after write
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/settings/concurrency", nil)
	handler.ServeHTTP(w, req)
	_ = json.NewDecoder(w.Body).Decode(&getResp)
	if getResp.Settings.MaxActiveFiles != 12 {
		t.Fatalf("read after write failed: expected 12, got %d", getResp.Settings.MaxActiveFiles)
	}

	// 5. Settings Invalid Value
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/settings/concurrency", strings.NewReader(`{"max_active_files": 0}`))
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for max_active_files=0, got %d", w.Code)
	}
}

type mockContractAccess struct {
	dialogs []DialogDTO
}

func (m *mockContractAccess) GetDialogs(ctx context.Context) ([]DialogDTO, error) {
	return m.dialogs, nil
}
func (m *mockContractAccess) ResolvePeerInfo(ctx context.Context, queryStr string) (DialogDTO, error) {
	return DialogDTO{ID: 100, ChatID: "-100100", Title: "Test Chat", TopMessageID: 888}, nil
}
func (m *mockContractAccess) GetHistory(ctx context.Context, req HistoryRequest) ([]MessageDTO, error) {
	return nil, nil
}
func (m *mockContractAccess) SyncPeers(ctx context.Context) error { return nil }
func (m *mockContractAccess) Resolve(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
	return ResolvedMedia{}, nil
}
func (m *mockContractAccess) ResolveBatch(ctx context.Context, peer string, messageIDs []int) (map[int]ResolvedMedia, error) {
	return nil, nil
}
func (m *mockContractAccess) Pool() dcpool.Pool {
	return nil
}

func TestWebServer_DialogsAndTargetContract(t *testing.T) {
	registry := NewRegistry(5, 100, time.Now)
	access := &mockContractAccess{
		dialogs: []DialogDTO{
			{ID: 101, ChatID: "-100101", Title: "Channel 1", TopMessageID: 1234},
		},
	}

	ws := NewWebServer(nil, nil, nil, nil, nil, access, registry, zap.NewNop(), "")
	handler := ws.Handler()

	// 1. GET /api/dialogs
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/dialogs?refresh=true", nil)
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("dialogs returned %d", w.Code)
	}

	var dResp struct {
		OK      bool             `json:"ok"`
		Dialogs []map[string]any `json:"dialogs"`
	}
	if err := json.NewDecoder(w.Body).Decode(&dResp); err != nil {
		t.Fatal(err)
	}
	if len(dResp.Dialogs) == 0 {
		t.Fatal("expected at least 1 dialog")
	}
	topMsg, exists := dResp.Dialogs[0]["top_message_id"]
	if !exists || topMsg.(float64) != 1234 {
		t.Fatalf("top_message_id missing or invalid: %v", topMsg)
	}

	// 2. POST /api/resolve_target
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/resolve_target", strings.NewReader(`{"query":"test"}`))
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("resolve target returned %d", w.Code)
	}
	var resolveResp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resolveResp); err != nil {
		t.Fatal(err)
	}
	dlg := resolveResp["dialog"].(map[string]any)
	if dlg["top_message_id"].(float64) != 888 {
		t.Fatalf("expected top_message_id 888, got %v", dlg["top_message_id"])
	}
}

func TestWebServer_EventsStreamContract(t *testing.T) {
	registry := NewRegistry(5, 100, time.Now)
	tm := transfer.NewTransferManager(transfer.Options{FileConcurrency: 5})
	ws := NewWebServer(nil, tm, nil, nil, nil, nil, registry, zap.NewNop(), "")
	handler := ws.Handler()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/api/events", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(w, req)
	}()

	// Read initial snapshot from SSE output
	// Allow short delay for first event flush
	time.Sleep(50 * time.Millisecond)
	cancel() // Cancel request context to exit stream
	<-done

	if w.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("expected Content-Type text/event-stream, got %s", w.Header().Get("Content-Type"))
	}

	scanner := bufio.NewScanner(w.Body)
	foundSnapshot := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event: snapshot") {
			foundSnapshot = true
			break
		}
	}
	if !foundSnapshot {
		t.Fatalf("expected initial snapshot event, received: %s", w.Body.String())
	}
}

func TestWebServer_AddTargetPersistenceFailure(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	// Close DB to inject persistence failure
	_ = db.Close()

	access := &mockContractAccess{}
	registry := NewRegistry(5, 100, time.Now)
	ws := NewWebServer(db, nil, nil, nil, nil, access, registry, zap.NewNop(), "")
	handler := ws.Handler()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/add_target", strings.NewReader(`{"query":"test"}`))
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on database failure, got %d: %s", w.Code, w.Body.String())
	}
}

func TestWebServer_StatusActiveFilesNeverNull(t *testing.T) {
	registry := NewRegistry(5, 100, time.Now)
	ws := NewWebServer(nil, nil, nil, nil, nil, nil, registry, zap.NewNop(), "")
	handler := ws.Handler()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, `"active_files":null`) {
		t.Fatalf("active_files must not serialize as null, got: %s", body)
	}
	if !strings.Contains(body, `"active_files":[]`) {
		t.Fatalf("active_files should serialize as empty slice [], got: %s", body)
	}
}
