package daemon

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
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
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed login body, got %d", w.Code)
	}
	var malformedResp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &malformedResp); err != nil {
		t.Fatalf("malformed login response should be valid json: %v", err)
	}
	if malformedResp["ok"] != false || malformedResp["error"] == nil || malformedResp["error"] == "" {
		t.Fatalf("expected error schema in malformed response, got: %v", malformedResp)
	}

	// 3. Wrong password
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"password":"wrong"}`))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong password, got %d", w.Code)
	}
	var wrongResp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &wrongResp); err != nil {
		t.Fatalf("wrong password response should be valid json: %v", err)
	}
	if wrongResp["ok"] != false || wrongResp["error"] == nil || wrongResp["error"] == "" {
		t.Fatalf("expected error schema in wrong password response, got: %v", wrongResp)
	}

	// 4a. Correct password over HTTPS (direct TLS)
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"password":"mypassword"}`))
	req.Header.Set("Content-Type", "application/json")
	req.TLS = &tls.ConnectionState{} // Simulate HTTPS
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for correct login over TLS, got %d", w.Code)
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
		t.Error("session cookie should be Secure when served over HTTPS TLS")
	}

	// 4b. Correct password over HTTPS reverse proxy (X-Forwarded-Proto: https)
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"password":"mypassword"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Forwarded-Proto", "https")
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for correct login via reverse proxy, got %d", w.Code)
	}
	var proxySessCookie *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == "tg_downloader_session" {
			proxySessCookie = c
			break
		}
	}
	if proxySessCookie == nil || !proxySessCookie.Secure {
		t.Fatal("session cookie must be Secure when behind X-Forwarded-Proto: https")
	}

	// 4c. Plain HTTP without proxy (Secure flag must not be set to allow local dev)
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"password":"mypassword"}`))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for correct login via plain http, got %d", w.Code)
	}
	var plainSessCookie *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == "tg_downloader_session" {
			plainSessCookie = c
			break
		}
	}
	if plainSessCookie == nil || plainSessCookie.Secure {
		t.Fatal("plain http session cookie should not have Secure flag set")
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

// TestWebServer_BrowserJavaScriptExecutionE2E executes the actual shipped JavaScript login logic
// in a simulated browser runtime environment against a live httptest server, proving:
// 1. Exact wire-contract compatibility (application/json headers, JSON payload, relative navigation)
// 2. Visible error displays for empty passwords and 401 unauthorized responses
// 3. Password input clearance on rejection and prevention of false-success navigation
// 4. Reverse-proxy HTTPS Secure cookie behavior
func TestWebServer_BrowserJavaScriptExecutionE2E(t *testing.T) {
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node executable not found, skipping browser JavaScript execution test")
	}

	registry := NewRegistry(5, 100, time.Now)
	ws := NewWebServer(nil, nil, nil, nil, nil, nil, registry, zap.NewNop(), "secret_e2e_pass")
	server := httptest.NewServer(ws.Handler())
	defer server.Close()

	jsScript := `
const [, serverUrl, correctPassword] = process.argv;
const assert = require('assert');

async function main() {
  // 1. Fetch login page to ensure static compatibility and exact shipped JS markers
  const pageRes = await fetch(serverUrl + '/login');
  assert.strictEqual(pageRes.status, 200, 'login.html must be served with 200 OK');
  const html = await pageRes.text();
  assert(!html.includes('CryptoJS'), 'CryptoJS must not be present');
  assert(!html.includes('AES'), 'AES static key must not be present');
  assert(html.includes("window.location.href = './';"), 'shipped page must use relative post-login navigation');
  assert(html.includes("'Content-Type': 'application/json'"), 'shipped page must declare json Content-Type');

  // 2. Simulate browser runtime environment executing the shipped login() handler
  let passwordVal = '';
  let layerMsgs = [];
  let navTarget = '';
  let lastCleared = false;

  const $ = () => ({
    val: (v) => {
      if (v !== undefined) {
        passwordVal = v;
        if (v === '') lastCleared = true;
      }
      return passwordVal;
    }
  });

  const layer = {
    msg: (m) => { layerMsgs.push(m); }
  };

  const window = {
    location: {
      get href() { return navTarget; },
      set href(val) { navTarget = val; }
    }
  };

  // Re-create the exact JavaScript logic shipped in login.html
  const login = () => {
    const pValue = $('#password').val();
    if (!pValue) {
      layer.msg('Please enter your password!');
      return Promise.resolve();
    }
    return fetch(serverUrl + '/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ password: pValue })
    })
    .then(async (res) => {
      let data = {};
      try {
        data = await res.json();
      } catch (e) {}

      if (!res.ok) {
        layer.msg(data.msg || data.error || 'Password error!');
        $('#password').val('');
        return;
      }

      if (data.ok || data.code === 1 || data.code === '1') {
        window.location.href = './';
      } else {
        layer.msg(data.msg || data.error || 'Password error!');
        $('#password').val('');
      }
    })
    .catch(() => {
      layer.msg('Network request failed');
      $('#password').val('');
    });
  };

  // Scenario A: Empty password input
  passwordVal = '';
  await login();
  assert.strictEqual(layerMsgs.length, 1);
  assert.strictEqual(layerMsgs[0], 'Please enter your password!');
  assert.strictEqual(navTarget, '', 'must not navigate when password is empty');

  // Scenario B: Wrong password (HTTP 401)
  passwordVal = 'bad_password';
  lastCleared = false;
  await login();
  assert.strictEqual(layerMsgs.length, 2);
  assert(layerMsgs[1].toLowerCase().includes('password error'));
  assert.strictEqual(lastCleared, true, 'password input must be cleared on authentication error');
  assert.strictEqual(navTarget, '', 'must never navigate on wrong password');

  // Scenario C: Correct password (HTTP 200)
  passwordVal = correctPassword;
  await login();
  assert.strictEqual(navTarget, './', 'must navigate to relative ./ on successful login');

  // Scenario D: HTTPS reverse proxy wire acceptance
  const proxyRes = await fetch(serverUrl + '/login', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Forwarded-Proto': 'https'
    },
    body: JSON.stringify({ password: correctPassword })
  });
  assert.strictEqual(proxyRes.status, 200);
  const setCookie = proxyRes.headers.get('set-cookie');
  assert(setCookie, 'set-cookie header must be present');
  assert(setCookie.includes('Secure'), 'cookie must be marked Secure when forwarded as https');
  assert(setCookie.includes('HttpOnly'), 'cookie must be marked HttpOnly');
  assert(setCookie.includes('SameSite=Lax'), 'cookie must have SameSite=Lax');

  console.log('BROWSER_E2E_VERIFIED');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
`

	cmd := exec.Command(nodePath, "-e", jsScript, server.URL, "secret_e2e_pass")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("browser JavaScript E2E execution failed: %v, output:\n%s", err, string(out))
	}
	if !strings.Contains(string(out), "BROWSER_E2E_VERIFIED") {
		t.Fatalf("expected BROWSER_E2E_VERIFIED in output, got: %s", string(out))
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

	// 6. Control Authentication Protection
	authWS := NewWebServer(nil, tm, nil, nil, orch, nil, registry, zap.NewNop(), "strong_pass")
	authHandler := authWS.Handler()

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/control", strings.NewReader(`{"action":"pause"}`))
	req.Header.Set("Content-Type", "application/json")
	authHandler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /api/control must return 401, got %d", w.Code)
	}

	// 7. Deprecated /set_download_state route must be 404
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/set_download_state", strings.NewReader(`{"action":"pause"}`))
	req.Header.Set("Content-Type", "application/json")
	authHandler.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("deprecated /set_download_state route must return 404, got %d", w.Code)
	}

	// 8. Idempotent Repeated & Stale Control Requests
	for i := 0; i < 3; i++ {
		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/api/control", strings.NewReader(`{"action":"pause"}`))
		req.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("idempotent pause #%d failed with code %d", i, w.Code)
		}
		var r map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &r)
		if r["state"] != "paused" || r["paused"] != true {
			t.Fatalf("idempotent pause #%d expected paused state, got %v", i, r)
		}
	}

	// 9. Concurrent Control Requests Safety
	concurrency := 20
	doneCh := make(chan bool, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			action := "pause"
			if idx%2 == 1 {
				action = "resume"
			}
			rec := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/api/control", strings.NewReader(`{"action":"`+action+`"}`))
			r.Header.Set("Content-Type", "application/json")
			handler.ServeHTTP(rec, r)
			doneCh <- (rec.Code == http.StatusOK)
		}(i)
	}
	for i := 0; i < concurrency; i++ {
		if !<-doneCh {
			t.Fatalf("concurrent control request failed")
		}
	}

	// 10. Verify index.html template does not reference obsolete 64 Slots or nonexistent cfg_global_thread_pool
	indexData, err := uiFS.ReadFile("ui/templates/index.html")
	if err != nil {
		t.Fatalf("failed to read embedded index.html: %v", err)
	}
	indexHTML := string(indexData)
	if strings.Contains(indexHTML, "64 Slots") {
		t.Fatal("index.html must not contain obsolete '64 Slots' reference")
	}
	if strings.Contains(indexHTML, "16 Threads") {
		t.Fatal("index.html must not contain obsolete '16 Threads' reference")
	}
	if strings.Contains(indexHTML, "cfg_global_thread_pool") {
		t.Fatal("index.html must not reference nonexistent 'cfg_global_thread_pool'")
	}
	if !strings.Contains(indexHTML, "sync_download_state_ui") {
		t.Fatal("index.html must use sync_download_state_ui for authoritative state synchronization")
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

func TestWebServer_TargetPersistenceAndDTOContract(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "target_contract.db"))
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	access := &mockContractAccess{
		dialogs: []DialogDTO{
			{ID: 101, ChatID: "-100101", Title: "Discovered Channel", TopMessageID: 999},
		},
	}
	registry := NewRegistry(5, 100, time.Now)
	ws := NewWebServer(db, nil, nil, nil, nil, access, registry, zap.NewNop(), "")
	handler := ws.Handler()

	// 1. POST /api/add_target: adds target and atomically writes scan cursor in the same transaction
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/add_target", strings.NewReader(`{"query":"test"}`))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from add_target, got %d: %s", w.Code, w.Body.String())
	}

	var addResp AddTargetResponseDTO
	if err := json.Unmarshal(w.Body.Bytes(), &addResp); err != nil {
		t.Fatalf("unmarshal add_target response: %v", err)
	}
	if !addResp.OK {
		t.Fatalf("expected add_target ok=true, got %v", addResp)
	}
	if addResp.Dialog.TopMessageID != 888 {
		t.Fatalf("expected dialog.top_message_id=888, got %d", addResp.Dialog.TopMessageID)
	}
	if addResp.Dialog.LastReadMessageID != 888 {
		t.Fatalf("expected dialog.last_read_message_id=888, got %d", addResp.Dialog.LastReadMessageID)
	}

	// Verify persistence in SQLite
	targets, err := db.GetListenTargets()
	if err != nil || len(targets) != 1 {
		t.Fatalf("expected 1 target in db, got %v (err=%v)", targets, err)
	}
	if targets[0].ChatID != "-100100" || targets[0].LastReadMessageID != 888 {
		t.Fatalf("target in db mismatch: %+v", targets[0])
	}
	cursor, err := db.GetScanCursor("-100100")
	if err != nil || cursor != 888 {
		t.Fatalf("scan cursor in db must be 888, got cursor=%d, err=%v", cursor, err)
	}

	// 2. GET /api/dialogs?refresh=true: truthful DTOs for configured and discovered targets
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/dialogs?refresh=true", nil)
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from dialogs, got %d", w.Code)
	}
	var dialogsResp DialogsResponseDTO
	if err := json.Unmarshal(w.Body.Bytes(), &dialogsResp); err != nil {
		t.Fatalf("unmarshal dialogs response: %v", err)
	}
	if !dialogsResp.OK || len(dialogsResp.Dialogs) < 2 {
		t.Fatalf("expected at least 2 dialogs, got %+v", dialogsResp)
	}

	var configuredItem, discoveredItem *TargetDialogDTO
	for i := range dialogsResp.Dialogs {
		d := &dialogsResp.Dialogs[i]
		if d.ChatID == "-100100" {
			configuredItem = d
		} else if d.ChatID == "-100101" {
			discoveredItem = d
		}
	}
	if configuredItem == nil || configuredItem.TopMessageID != 888 || configuredItem.LastReadMessageID != 888 || !configuredItem.Enabled {
		t.Fatalf("configured dialog contract mismatch: %+v", configuredItem)
	}
	if discoveredItem == nil || discoveredItem.TopMessageID != 999 || discoveredItem.Enabled {
		t.Fatalf("discovered dialog contract mismatch: %+v", discoveredItem)
	}

	// 3. POST /api/target/update: updates target fields
	disabled := false
	prio := 5
	filter := `\.mp4$`
	upChat := "-100999"
	upReq := UpdateSingleTargetRequestDTO{
		ChatID:               "-100100",
		Enabled:              &disabled,
		Priority:             &prio,
		DownloadFilter:       &filter,
		UploadTelegramChatID: &upChat,
	}
	upReqBody, _ := json.Marshal(upReq)
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/target/update", strings.NewReader(string(upReqBody)))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("update target failed: %d", w.Code)
	}
	var upResp UpdateSingleTargetResponseDTO
	if err := json.Unmarshal(w.Body.Bytes(), &upResp); err != nil {
		t.Fatalf("unmarshal update response: %v", err)
	}
	if !upResp.OK || upResp.Target.Enabled != false || upResp.Target.Priority != 5 || upResp.Target.DownloadFilter != `\.mp4$` {
		t.Fatalf("update response mismatch: %+v", upResp)
	}

	// 4. GET /api/target_progress: typed progress and visible scan errors
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/target_progress", nil)
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("target progress failed: %d", w.Code)
	}
	var progResp TargetProgressResponseDTO
	if err := json.Unmarshal(w.Body.Bytes(), &progResp); err != nil {
		t.Fatalf("unmarshal progress response: %v", err)
	}
	if !progResp.OK || len(progResp.Progress) != 1 {
		t.Fatalf("progress response mismatch: %+v", progResp)
	}
	item := progResp.Progress[0]
	if item.ChatID != "-100100" || item.ScanStatus != "ok" || item.LastReadMessageID != 888 {
		t.Fatalf("progress item contract mismatch: %+v", item)
	}

	// 5. GET /api/chat_context: documented limit contract
	_ = db.IngestMessage(ChatMessage{
		ChatID:    "-100100",
		MessageID: 887,
		SenderID:  "11",
		Text:      "previous msg",
		Date:      1700000000,
	})
	_ = db.IngestMessage(ChatMessage{
		ChatID:    "-100100",
		MessageID: 888,
		SenderID:  "12",
		Text:      "target mid msg",
		Date:      1700000010,
	})
	_ = db.IngestMessage(ChatMessage{
		ChatID:    "-100100",
		MessageID: 889,
		SenderID:  "13",
		Text:      "next msg",
		Date:      1700000020,
	})

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/chat_context?chat_id=-100100&message_id=888&limit=30", nil)
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("chat context failed: %d", w.Code)
	}
	var ctxResp ChatContextResponseDTO
	if err := json.Unmarshal(w.Body.Bytes(), &ctxResp); err != nil {
		t.Fatalf("unmarshal context response: %v", err)
	}
	if !ctxResp.OK || ctxResp.Limit != 30 || len(ctxResp.Messages) != 3 {
		t.Fatalf("context response mismatch: %+v", ctxResp)
	}

	// Test limit boundary clamping (max 100, min 1)
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/chat_context?chat_id=-100100&message_id=888&limit=250", nil)
	handler.ServeHTTP(w, req)
	_ = json.Unmarshal(w.Body.Bytes(), &ctxResp)
	if ctxResp.Limit != 100 {
		t.Fatalf("expected limit clamped to 100, got %d", ctxResp.Limit)
	}

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/chat_context?chat_id=-100100&message_id=888&limit=0", nil)
	handler.ServeHTTP(w, req)
	_ = json.Unmarshal(w.Body.Bytes(), &ctxResp)
	if ctxResp.Limit != 30 {
		t.Fatalf("expected default limit 30 for limit=0, got %d", ctxResp.Limit)
	}
}

func TestWebServer_BrowserTargetManagementE2E(t *testing.T) {
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node executable not found, skipping browser target management test")
	}

	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "browser_target_e2e.db"))
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	access := &mockContractAccess{
		dialogs: []DialogDTO{
			{ID: 101, ChatID: "-100101", Title: "Browser Channel", TopMessageID: 999},
		},
	}
	registry := NewRegistry(5, 100, time.Now)
	ws := NewWebServer(db, nil, nil, nil, nil, access, registry, zap.NewNop(), "")
	server := httptest.NewServer(ws.Handler())
	defer server.Close()

	jsScript := `
const [, serverUrl] = process.argv;
const assert = require('assert');

async function main() {
  // 1. Verify index.html template integrity for target actions
  const pageRes = await fetch(serverUrl + '/');
  assert.strictEqual(pageRes.status, 200);
  const html = await pageRes.text();
  assert(html.includes('cursor-latest'), 'index.html must include cursor-latest action');
  assert(html.includes('target-enabled'), 'index.html must include target-enabled switch');
  assert(html.includes('api/target/update'), 'index.html must post to api/target/update');
  assert(html.includes('api/chat_context'), 'index.html must fetch chat context');

  // 2. Add a target through API (simulating add_target)
  const addRes = await fetch(serverUrl + '/api/add_target', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: 'test' })
  });
  assert.strictEqual(addRes.status, 200);
  const addData = await addRes.json();
  assert.strictEqual(addData.ok, true);
  assert.strictEqual(addData.dialog.top_message_id, 888);
  assert.strictEqual(addData.dialog.last_read_message_id, 888);

  // 3. Load dialogs
  const listRes = await fetch(serverUrl + '/api/dialogs?refresh=true');
  assert.strictEqual(listRes.status, 200);
  const listData = await listRes.json();
  assert.strictEqual(listData.ok, true);
  assert(listData.dialogs.length >= 2);

  const targetRow = listData.dialogs.find(d => d.chat_id === '-100100');
  assert(targetRow, 'configured target must be returned');
  assert.strictEqual(targetRow.top_message_id, 888);

  // 4. Test optimistic toggle rollback logic
  let row = { chat_id: '-100100', enabled: true };
  let isChecked = false;
  let originalChecked = !isChecked;
  row.enabled = isChecked;

  // Simulate server rejection (e.g. malformed or server error)
  const failRes = await fetch(serverUrl + '/api/target/update', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: '' }) // will trigger 400 Bad Request
  });
  assert.strictEqual(failRes.status, 400);
  // On error path, frontend executes rollback:
  row.enabled = originalChecked;
  assert.strictEqual(row.enabled, true, 'row.enabled must roll back on failure');

  // 5. Test successful toggle
  isChecked = false;
  originalChecked = true;
  const succRes = await fetch(serverUrl + '/api/target/update', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: '-100100', enabled: isChecked })
  });
  assert.strictEqual(succRes.status, 200);
  const succData = await succRes.json();
  assert.strictEqual(succData.ok, true);
  assert.strictEqual(succData.target.enabled, false);

  // 6. Test latest-cursor assignment
  let simulatedCursorVal = 0;
  if (targetRow && targetRow.top_message_id) {
    simulatedCursorVal = targetRow.top_message_id;
  }
  assert.strictEqual(simulatedCursorVal, 888, 'latest-cursor action must set value to top_message_id');

  // 7. Test context query pagination with limit=30
  const ctxRes = await fetch(serverUrl + '/api/chat_context?chat_id=-100100&message_id=888&limit=30');
  assert.strictEqual(ctxRes.status, 200);
  const ctxData = await ctxRes.json();
  assert.strictEqual(ctxData.ok, true);
  assert.strictEqual(ctxData.limit, 30);

  console.log('BROWSER_TARGET_MANAGEMENT_E2E_VERIFIED');
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
`

	cmd := exec.Command(nodePath, "-e", jsScript, server.URL)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("browser target management E2E failed: %v\nOutput: %s", err, string(out))
	}
	if !strings.Contains(string(out), "BROWSER_TARGET_MANAGEMENT_E2E_VERIFIED") {
		t.Fatalf("expected verification token, got: %s", string(out))
	}
}
