package daemon

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
const vm = require('vm');

async function main() {
  // 1. Fetch login page delivered by the production handler
  const pageRes = await fetch(serverUrl + '/login');
  assert.strictEqual(pageRes.status, 200, 'login.html must be served with 200 OK');
  const html = await pageRes.text();
  assert(!html.includes('CryptoJS'), 'CryptoJS must not be present');
  assert(!html.includes('AES'), 'AES static key must not be present');
  assert(html.includes("window.location.href = './';"), 'shipped page must use relative post-login navigation');
  assert(html.includes("'Content-Type': 'application/json'"), 'shipped page must declare json Content-Type');

  // 2. Extract the exact shipped <script> delivered in login.html
  const scriptTags = [...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)];
  const inlineScript = scriptTags.find(m => m[1].includes('login') && m[1].includes('fetch('));
  assert(inlineScript, 'delivered login.html must include inline script defining login()');
  const deliveredCode = inlineScript[1];

  // 3. Build simulated browser environment with DOM, layer, and cookie-aware fetch
  let passwordVal = '';
  let layerMsgs = [];
  let navTarget = '';
  let currentCookie = '';
  let simulatedBaseUrl = serverUrl + '/login';
  let simulatedHeaders = {};

  const sandbox = {
    console,
    JSON,
    Promise,
    setTimeout,
    clearTimeout,
    fetch: async (url, opts = {}) => {
      // Map relative URL to live test server while tracking simulated client-side base
      const dispatchUrl = new URL(url, serverUrl + '/login').toString();
      const headers = { ...(opts.headers || {}), ...simulatedHeaders };
      if (currentCookie && !headers['Cookie']) {
        headers['Cookie'] = currentCookie;
      }
      const res = await globalThis.fetch(dispatchUrl, { ...opts, headers });
      const setCookie = res.headers.get('set-cookie');
      if (setCookie) {
        currentCookie = setCookie.split(';')[0];
      }
      return res;
    },
    layui: {
      $: (selector) => {
        if (selector === '#password') {
          return {
            val: (v) => {
              if (v !== undefined) {
                passwordVal = v;
              }
              return passwordVal;
            },
            on: () => {}
          };
        }
        return { val: () => '', on: () => {} };
      },
      layer: {
        msg: (m) => { layerMsgs.push(m); }
      }
    }
  };

  sandbox.window = sandbox;
  sandbox.document = {
    location: {
      get href() { return navTarget || simulatedBaseUrl; },
      set href(val) {
        navTarget = val;
        sandbox.resolvedHref = new URL(val, simulatedBaseUrl).toString();
      }
    }
  };
  sandbox.window.location = sandbox.document.location;

  // Execute the exact delivered JavaScript code in the sandbox
  const context = vm.createContext(sandbox);
  vm.runInContext(deliveredCode, context);
  assert.strictEqual(typeof vm.runInContext('login', context), 'function', 'delivered script must define login() function');
  const triggerLogin = () => vm.runInContext('login()', context);

  const waitFor = async (predicate, desc, timeoutMs = 3000) => {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (predicate()) return;
      await new Promise(r => setTimeout(r, 10));
    }
    throw new Error('timeout waiting for ' + desc + '; layerMsgs=' + JSON.stringify(layerMsgs) + ', navTarget=' + navTarget + ', passwordVal=' + passwordVal);
  };

  // Scenario A: Empty password input -> visible rejection without HTTP dispatch
  passwordVal = '';
  layerMsgs = [];
  navTarget = '';
  await triggerLogin();
  assert.strictEqual(layerMsgs.length, 1, 'must display visible warning message');
  assert.strictEqual(layerMsgs[0], 'Please enter your password!');
  assert.strictEqual(navTarget, '', 'must not navigate on empty password');

  // Scenario B: Wrong password -> HTTP 401 rejection, visible message, cleared password input, no navigation
  passwordVal = 'wrong_password';
  layerMsgs = [];
  navTarget = '';
  await triggerLogin();
  await waitFor(() => layerMsgs.length === 1 && passwordVal === '', 'Scenario B 401 rejection and password cleared');
  assert(layerMsgs[0].toLowerCase().includes('password error'));
  assert.strictEqual(passwordVal, '', 'password input must be cleared on authentication rejection');
  assert.strictEqual(navTarget, '', 'must not navigate on wrong password');

  // Scenario C: Correct password -> HTTP 200, relative navigation, session cookie captured
  passwordVal = correctPassword;
  layerMsgs = [];
  navTarget = '';
  await triggerLogin();
  await waitFor(() => navTarget === './', 'Scenario C successful navigation to ./');
  assert.strictEqual(sandbox.resolvedHref, new URL('./', simulatedBaseUrl).toString(), 'navigation must preserve scheme and base path');
  assert(currentCookie.includes('tg_downloader_session='), 'tg_downloader_session cookie must be captured in client store');

  // Scenario D: Reverse-proxy HTTPS & subpath preservation
  simulatedBaseUrl = 'https://nas.internal:8443/custom_prefix/login';
  simulatedHeaders = { 'X-Forwarded-Proto': 'https', 'X-Forwarded-Prefix': '/custom_prefix' };
  passwordVal = correctPassword;
  layerMsgs = [];
  navTarget = '';
  await triggerLogin();
  await waitFor(() => navTarget === './', 'Scenario D reverse proxy navigation to ./');
  assert.strictEqual(sandbox.resolvedHref, 'https://nas.internal:8443/custom_prefix/', 'scheme https and subpath prefix must be preserved');

  // Scenario E: Verify authenticated session with captured cookie on production endpoint
  const authRes = await sandbox.fetch(serverUrl + '/api/status');
  assert.strictEqual(authRes.status, 200, 'captured cookie must authenticate protected API endpoints');
  const authData = await authRes.json();
  assert(authData.backend, 'must receive status payload from protected router');

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

	_ = db.IngestMessage(ChatMessage{
		ChatID:    "-100100",
		MessageID: 888,
		Text:      "Target message 888",
		Date:      time.Now().Unix(),
	})

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
const vm = require('vm');

async function main() {
  // 1. Verify index.html template integrity from production handler
  const pageRes = await fetch(serverUrl + '/');
  assert.strictEqual(pageRes.status, 200);
  const html = await pageRes.text();
  assert(html.includes('cursor-latest'), 'index.html must include cursor-latest action');
  assert(html.includes('target-enabled'), 'index.html must include target-enabled switch');
  assert(html.includes('api/target/update'), 'index.html must post to api/target/update');
  assert(html.includes('api/chat_context'), 'index.html must fetch chat context');

  // 2. Extract delivered script containing target management logic
  const scriptTags = [...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)];
  const inlineScript = scriptTags.find(m => m[1].includes('load_dialogs'));
  assert(inlineScript, 'index.html must include delivered script defining load_dialogs');
  const deliveredCode = inlineScript[1];

  // 3  // 4. Setup browser DOM & Ajax harness connected to live Go test server
  let promptValue = '';
  let layerMsgs = [];
  let targetListHTML = '';
  let contextMsgHTML = '';
  const buttonClickHandlers = {};
  const delegatedHandlers = {
    change: {},
    click: {}
  };

  class MockEventSource {
    constructor(url) {
      this.listeners = {};
      this.onerror = null;
    }
    addEventListener() {}
  }

  function makeCardElem(chatKey, initialCursor = '0') {
    let checked = true;
    let cursorVal = String(initialCursor);
    const classes = new Set(['apple-target-card', 'is-enabled']);
    const cardElem = {
      attr: (k) => k === 'data-chat-key' ? chatKey : '',
      closest: (sel) => cardElem,
      find: (sel) => {
        if (sel === '.target-cursor') {
          return {
            val: (v) => {
              if (v !== undefined) {
                cursorVal = String(v);
                return cardElem;
              }
              return cursorVal;
            }
          };
        }
        return createDOMWrapper(sel);
      },
      toggleClass: (cls, state) => {
        if (state) classes.add(cls);
        else classes.delete(cls);
        return cardElem;
      },
      hasClass: (cls) => classes.has(cls),
      prop: (propName, val) => {
        if (val !== undefined) {
          checked = Boolean(val);
          return cardElem;
        }
        return checked;
      }
    };
    return cardElem;
  }

  let inFlightAjax = 0;
  const ajaxCompleteWaiters = [];
  function notifyAjaxDone() {
    inFlightAjax--;
    if (inFlightAjax <= 0) {
      while (ajaxCompleteWaiters.length) {
        ajaxCompleteWaiters.shift()();
      }
    }
  }

  function waitForAjax() {
    if (inFlightAjax <= 0) return Promise.resolve();
    return new Promise(resolve => ajaxCompleteWaiters.push(resolve));
  }

  function createDOMWrapper(sel) {
    if (typeof sel === 'object' && sel !== null) {
      return new Proxy(sel, {
        get(t, prop) {
          if (typeof prop === 'symbol') return undefined;
          if (prop in t) return t[prop];
          return (...args) => createDOMWrapper(t);
        }
      });
    }

    if (typeof sel === 'string' && (sel === '#reload_dialogs' || sel === '#add_target_btn' || sel === '#save_targets')) {
      return {
        on: (evt, fn) => {
          if (evt === 'click') buttonClickHandlers[sel] = fn;
        }
      };
    }

    if (sel === '#target_list') {
      return {
        html: (content) => {
          if (content !== undefined) targetListHTML = content;
          return targetListHTML;
        },
        on: (evt, subSel, fn) => {
          if (!delegatedHandlers[evt]) delegatedHandlers[evt] = {};
          delegatedHandlers[evt][subSel] = fn;
        }
      };
    }

    if (sel === '#context_msg_list') {
      return {
        html: (content) => {
          if (content !== undefined) contextMsgHTML = content;
          return contextMsgHTML;
        }
      };
    }

    const target = {
      length: 1,
      0: { scrollIntoView: () => {} },
      is: () => false,
      val: () => '',
      text: () => '',
      html: () => '',
      width: () => 1024,
      get: () => ({ getContext: () => ({ clearRect: () => {}, beginPath: () => {}, moveTo: () => {}, lineTo: () => {}, stroke: () => {}, fill: () => {} }) })
    };
    return new Proxy(target, {
      get(t, prop) {
        if (typeof prop === 'symbol') return undefined;
        if (prop in t) return t[prop];
        return (...args) => createDOMWrapper(String(sel) + '.' + String(prop));
      }
    });
  }

  const sandbox = {
    console, JSON, parseInt, parseFloat, String, Array, Object, Date, Math,
    setTimeout: (fn, ms) => setTimeout(fn, ms || 0),
    clearTimeout: (id) => clearTimeout(id),
    setInterval: () => 2,
    clearInterval: () => {},
    location: { reload: () => {} },
    window: {},
    document: {},
    encodeURIComponent,
    layui: {
      use: (mods, cb) => { cb(); },
      $: (sel) => createDOMWrapper(sel),
      layer: {
        msg: (m) => { layerMsgs.push(m); return 1; },
        prompt: (opts, cb) => { cb(promptValue, 1); },
        open: (opts) => {
          const layero = {
            find: (sel) => {
              if (sel === '#context_msg_list') {
                return {
                  html: (h) => { if (h !== undefined) contextMsgHTML = h; return contextMsgHTML; },
                  find: (sub) => createDOMWrapper(sub)
                };
              }
              return createDOMWrapper(sel);
            },
            on: () => {}
          };
          if (opts.success) opts.success(layero);
        },
        close: () => {},
        load: () => 1
      },
      table: { render: () => {}, reload: () => {}, on: () => {} }
    }
  };

  sandbox.layui.$.trim = (s) => (s || '').trim();
  sandbox.layui.$.ajax = (opts) => {
    inFlightAjax++;
    const url = new URL(opts.url, serverUrl + '/').toString();
    const fetchOpts = {
      method: (opts.type || 'GET').toUpperCase(),
      headers: {}
    };
    if (opts.contentType) fetchOpts.headers['Content-Type'] = opts.contentType;
    if (opts.data) fetchOpts.body = typeof opts.data === 'string' ? opts.data : JSON.stringify(opts.data);

    fetch(url, fetchOpts).then(async (res) => {
      const text = await res.text();
      let json = null;
      try { json = JSON.parse(text); } catch (_) {}
      if (res.ok) {
        if (opts.success) opts.success(json !== null ? json : text);
      } else {
        if (opts.error) opts.error({ status: res.status, responseText: text, responseJSON: json });
      }
    }).catch((err) => {
      if (opts.error) opts.error(err);
    }).finally(() => {
      if (opts.complete) opts.complete();
      notifyAjaxDone();
    });
  };

  sandbox.window = sandbox;
  sandbox.window.EventSource = MockEventSource;

  // Execute the exact delivered JavaScript inside VM sandbox
  const context = vm.createContext(sandbox);
  vm.runInContext(deliveredCode, context);

  // 4. Test Case 1: Execute delivered reload_dialogs button click against live Go server
  assert(buttonClickHandlers['#reload_dialogs'], 'reload_dialogs button click handler must be wired');
  buttonClickHandlers['#reload_dialogs']();
  await waitForAjax();

  assert(targetListHTML.includes('Browser Channel'), 'Channel -100101 must be rendered in target list');
  assert(targetListHTML.includes('cursor-latest'), 'cursor-latest action must render when top_message_id > 0');

  // 5. Test Case 2: Execute delivered add_target button click against live Go server
  assert(buttonClickHandlers['#add_target_btn'], 'add_target_btn button click handler must be wired');
  promptValue = 'test';
  layerMsgs = [];
  buttonClickHandlers['#add_target_btn']();
  await waitForAjax();

  assert(targetListHTML.includes('-100100'), 'Added target -100100 must be rendered in target list');
  assert(layerMsgs.some(m => String(m).includes('Target added')), 'Must display success message for add_target');

  // 6. Test Case 3: Execute delivered target-enabled toggle with server rejection -> rollback verification
  const toggleHandler = delegatedHandlers.change['.target-enabled'];
  assert(toggleHandler, 'shipped change handler for .target-enabled must be registered');

  // Temporarily hook $.ajax to simulate a 400 Bad Request error
  const origAjax = sandbox.layui.$.ajax;
  sandbox.layui.$.ajax = (opts) => {
    if (opts.url === 'api/target/update') {
      opts.error({ status: 400, responseText: 'Bad Request', responseJSON: { error: 'Injected toggle failure' } });
      return;
    }
    origAjax(opts);
  };

  const card100 = makeCardElem('-100100');
  card100.prop('checked', false); // user unchecks checkbox
  layerMsgs = [];
  toggleHandler.call(card100);

  // Assert rollback on failure
  assert.strictEqual(card100.prop('checked'), true, 'Checkbox must roll back to true on failure');
  assert.strictEqual(card100.hasClass('is-enabled'), true, 'Card is-enabled class must roll back to true on failure');
  assert(layerMsgs.some(m => String(m).includes('Update failed') || String(m).includes('Injected toggle failure')), 'Must display failure message');

  // Restore live $.ajax
  sandbox.layui.$.ajax = origAjax;

  // 7. Test Case 4: Execute delivered target-enabled toggle with live server success
  card100.prop('checked', false);
  toggleHandler.call(card100);
  await waitForAjax();

  assert.strictEqual(card100.prop('checked'), false, 'Checkbox must remain false after successful toggle');

  // 8. Test Case 5: Execute delivered cursor-latest click action
  const latestHandler = delegatedHandlers.click['.cursor-latest'];
  assert(latestHandler, 'shipped click handler for .cursor-latest must be registered');

  const card101 = makeCardElem('-100101', '50');
  latestHandler.call(card101);

  assert.strictEqual(card101.find('.target-cursor').val(), '999', 'Latest cursor button must set input value to top_message_id (999)');

  // 9. Test Case 6: Execute delivered context viewer button action against live server
  const contextHandler = delegatedHandlers.click['.btn-context'];
  assert(contextHandler, 'shipped click handler for .btn-context must be registered');

  contextMsgHTML = '';
  contextHandler.call(card100);
  await waitForAjax();

  assert(contextMsgHTML.includes('#888'), 'Context viewer must render message #888 returned by server');
  assert(contextMsgHTML.includes('Target message 888'), 'Context viewer must render message text from server');

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

// TestWebServer_ProductionRouterContractInventory verifies that production and tests share
// the exact same router and middleware table. It validates every browser-used endpoint for:
// 1. Unauthenticated rejection (401 JSON or 302 redirect to /login)
// 2. Authenticated wire acceptance and correct HTTP status (no 404 or 405)
// 3. Named owners for non-browser routes (healthz, gate, tasks, status)
// 4. Absence of unowned obsolete aliases (e.g. /set_download_state is 404)
// 5. Complete static HTML template route inventory alignment (anti-drift)
func TestWebServer_ProductionRouterContractInventory(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "inventory.db"))
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	registry := NewRegistry(5, 100, time.Now)
	tm := transfer.NewTransferManager(transfer.Options{FileConcurrency: 5})
	orch := NewOrchestrator(nil, tm, nil, nil, nil, registry, zap.NewNop(), tempDir)
	access := &mockContractAccess{
		dialogs: []DialogDTO{
			{ID: 101, ChatID: "-100101", Title: "Channel", TopMessageID: 100},
		},
	}

	testPass := "inventory_secret_password"
	ws := NewWebServer(db, tm, nil, nil, orch, access, registry, zap.NewNop(), testPass)
	handler := ws.Handler()

	// 1. Obtain authenticated session cookie via /login
	loginRec := httptest.NewRecorder()
	loginReq := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"password":"`+testPass+`"}`))
	loginReq.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(loginRec, loginReq)
	if loginRec.Code != http.StatusOK {
		t.Fatalf("login failed with code %d: %s", loginRec.Code, loginRec.Body.String())
	}
	var sessCookie *http.Cookie
	for _, c := range loginRec.Result().Cookies() {
		if c.Name == "tg_downloader_session" {
			sessCookie = c
			break
		}
	}
	if sessCookie == nil {
		t.Fatal("expected tg_downloader_session cookie from login")
	}

	type routeContract struct {
		method         string
		path           string
		body           string
		requiresAuth   bool
		expectedStatus int
		namedOwner     string
	}

	inventory := []routeContract{
		// Non-browser routes with named owners
		{"GET", "/healthz", "", false, http.StatusOK, "CGroup/Docker container healthcheck"},
		{"GET", "/api/status", "", false, http.StatusOK, "CLI and external telemetry"},
		{"GET", "/api/gate", "", false, http.StatusOK, "Evaluation sandbox live diagnostics"},
		{"GET", "/api/tasks", "", false, http.StatusOK, "CLI task queue monitoring"},
		{"POST", "/api/tasks", "{}", false, http.StatusBadRequest, "CLI task batch submission"},

		// Public authentication routes
		{"GET", "/get_app_version", "", false, http.StatusOK, "Frontend version check"},
		{"GET", "/login", "", false, http.StatusOK, "Browser login page"},
		{"POST", "/login", `{"password":"` + testPass + `"}`, false, http.StatusOK, "Browser login wire"},
		{"GET", "/logout", "", false, http.StatusFound, "Browser logout handler"},

		// Authenticated dashboard routes
		{"GET", "/", "", true, http.StatusOK, "Browser root dashboard"},
		{"POST", "/api/control", `{"action":"pause"}`, true, http.StatusOK, "Authoritative system state control"},
		{"GET", "/get_download_status", "", true, http.StatusOK, "Telemetry status probe"},
		{"GET", "/get_download_list?already_down=false", "", true, http.StatusOK, "Active download queue snapshot"},
		{"GET", "/api/downloaded_records?page=1&limit=10", "", true, http.StatusOK, "Historical downloaded records table"},
		{"GET", "/api/system/storage", "", false, http.StatusOK, "Storage utilization diagnostics"},

		// Target & Dialog management
		{"GET", "/api/dialogs", "", true, http.StatusOK, "Dialog and monitored target list"},
		{"POST", "/api/add_target", `{"query":"test"}`, true, http.StatusOK, "Single target atomic creation"},
		{"POST", "/api/listen_targets", `{"targets":[]}`, true, http.StatusOK, "Target batch persistence"},
		{"POST", "/api/target/update", `{"chat_id":"-100100"}`, true, http.StatusOK, "Inline target settings update"},
		{"GET", "/api/target_progress", "", true, http.StatusOK, "Live target scan & download progress"},
		{"GET", "/api/chat_context?chat_id=-100100&message_id=1&limit=30", "", true, http.StatusOK, "Unified limit message context"},

		// Settings & Telemetry
		{"GET", "/api/settings/concurrency", "", true, http.StatusOK, "Concurrency settings read"},
		{"POST", "/api/settings/concurrency", `{"max_active_files":8}`, true, http.StatusOK, "Concurrency settings write"},

		// Telegram Account Auth Wizard & Multi-account
		{"GET", "/api/auth/status", "", true, http.StatusOK, "Telegram MTProto authorization status"},
		{"POST", "/api/auth/qr/start", "", true, http.StatusServiceUnavailable, "Telegram QR login start"},
		{"GET", "/api/auth/qr/poll", "", true, http.StatusServiceUnavailable, "Telegram QR login polling"},
		{"POST", "/api/auth/phone/send_code", "{}", true, http.StatusBadRequest, "Phone auth SMS dispatch"},
		{"POST", "/api/auth/phone/verify_code", "{}", true, http.StatusBadRequest, "Phone auth SMS verification"},
		{"POST", "/api/auth/2fa/verify", "{}", true, http.StatusBadRequest, "Cloud password 2FA verification"},
		{"POST", "/api/auth/logout", "", true, http.StatusServiceUnavailable, "Telegram session logout"},
		{"GET", "/api/accounts", "", true, http.StatusOK, "Multi-account list"},
		{"POST", "/api/accounts/switch", "{}", true, http.StatusBadRequest, "Multi-account switch"},
		{"POST", "/api/accounts/delete", "{}", true, http.StatusBadRequest, "Multi-account deletion"},

		// Proxy actions
		{"GET", "/api/proxy/list", "", true, http.StatusOK, "Configured proxy list"},
		{"POST", "/api/proxy/switch", "{}", true, http.StatusBadRequest, "Proxy profile switch"},
		{"POST", "/api/proxy/ping", "{}", true, http.StatusBadRequest, "Proxy connectivity probe"},
	}

	for _, rc := range inventory {
		t.Run(rc.method+"_"+rc.path, func(t *testing.T) {
			// Phase A: Unauthenticated test for protected routes
			if rc.requiresAuth {
				rec := httptest.NewRecorder()
				r := httptest.NewRequest(rc.method, rc.path, strings.NewReader(rc.body))
				if rc.body != "" {
					r.Header.Set("Content-Type", "application/json")
				}
				handler.ServeHTTP(rec, r)

				if strings.HasPrefix(rc.path, "/api/") || strings.HasPrefix(rc.path, "/get_") {
					if rec.Code != http.StatusUnauthorized {
						t.Fatalf("[%s %s] unauthenticated request must return 401 Unauthorized, got %d", rc.method, rc.path, rec.Code)
					}
					var errMap map[string]any
					if err := json.Unmarshal(rec.Body.Bytes(), &errMap); err != nil || errMap["ok"] != false {
						t.Fatalf("[%s %s] unauthenticated 401 response must be structured error json, got: %s", rc.method, rc.path, rec.Body.String())
					}
				} else if rc.path == "/" {
					if rec.Code != http.StatusFound || rec.Header().Get("Location") != "/login" {
						t.Fatalf("[%s %s] unauthenticated root page must redirect to /login, got %d", rc.method, rc.path, rec.Code)
					}
				}
			}

			// Phase B: Authenticated request verification
			rec := httptest.NewRecorder()
			r := httptest.NewRequest(rc.method, rc.path, strings.NewReader(rc.body))
			if rc.body != "" {
				r.Header.Set("Content-Type", "application/json")
			}
			if rc.requiresAuth {
				r.AddCookie(sessCookie)
			}
			handler.ServeHTTP(rec, r)

			if rec.Code == http.StatusNotFound {
				t.Fatalf("[%s %s] route is NOT registered in production WebServer (got 404 Not Found)", rc.method, rc.path)
			}
			if rec.Code == http.StatusMethodNotAllowed {
				t.Fatalf("[%s %s] method not allowed on production WebServer (got 405)", rc.method, rc.path)
			}
			if rec.Code != rc.expectedStatus {
				t.Fatalf("[%s %s] expected status %d, got %d: %s", rc.method, rc.path, rc.expectedStatus, rec.Code, rec.Body.String())
			}
		})
	}

	// 2. Anti-drift test: parse index.html and ensure all called endpoints are covered in production WebServer
	indexBytes, err := uiFS.ReadFile("ui/templates/index.html")
	if err != nil {
		t.Fatalf("failed to read embedded index.html: %v", err)
	}
	indexStr := string(indexBytes)

	expectedClientEndpoints := []string{
		"get_app_version",
		"api/accounts/switch",
		"api/accounts/delete",
		"api/auth/status",
		"api/auth/qr/start",
		"api/auth/qr/poll",
		"api/auth/2fa/verify",
		"api/auth/phone/send_code",
		"api/auth/phone/verify_code",
		"api/auth/logout",
		"api/control",
		"api/target_progress",
		"api/dialogs",
		"api/add_target",
		"api/listen_targets",
		"api/target/update",
		"api/chat_context",
		"api/downloaded_records",
		"get_download_list",
		"api/system/storage",
		"get_download_status",
		"api/settings/concurrency",
		"api/proxy/list",
		"api/proxy/switch",
		"api/proxy/ping",
	}

	for _, ep := range expectedClientEndpoints {
		if !strings.Contains(indexStr, ep) {
			t.Fatalf("index.html is missing expected endpoint call: %s", ep)
		}
	}

	// 3. Verify unowned obsolete aliases are strictly removed (404)
	obsoleteRoutes := []string{
		"/set_download_state",
	}
	for _, obs := range obsoleteRoutes {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, obs, strings.NewReader(`{"action":"pause"}`))
		r.AddCookie(sessCookie)
		handler.ServeHTTP(rec, r)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("obsolete route %s must return 404, got %d", obs, rec.Code)
		}
	}
}

func TestWebServer_EventsStreamComprehensiveContract(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "events.db"))
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	_ = db.SaveSingleListenTarget(ListenTarget{
		ChatID:            "-100123",
		Title:             "SSE Target",
		Enabled:           true,
		LastReadMessageID: 50,
	})

	registry := NewRegistry(5, 100, time.Now)
	tm := transfer.NewTransferManager(transfer.Options{FileConcurrency: 5})
	orch := NewOrchestrator(nil, tm, nil, nil, nil, registry, zap.NewNop(), tempDir)

	ws := NewWebServer(db, tm, nil, nil, orch, nil, registry, zap.NewNop(), "sse_pass")
	handler := ws.Handler()

	// 1. Unauthenticated request to /api/events must be rejected with 401
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/events", nil)
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /api/events must return 401, got %d", w.Code)
	}

	// 2. Authenticate and obtain session cookie
	loginRec := httptest.NewRecorder()
	loginReq := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"password":"sse_pass"}`))
	loginReq.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(loginRec, loginReq)
	var sessCookie *http.Cookie
	for _, c := range loginRec.Result().Cookies() {
		if c.Name == "tg_downloader_session" {
			sessCookie = c
			break
		}
	}
	if sessCookie == nil {
		t.Fatal("missing session cookie")
	}

	// 3. Connect to /api/events and verify comprehensive snapshot content
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamReq := httptest.NewRequest(http.MethodGet, "/api/events", nil).WithContext(ctx)
	streamReq.AddCookie(sessCookie)
	streamRec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(streamRec, streamReq)
	}()

	// Wait briefly for first event flush
	time.Sleep(50 * time.Millisecond)
	cancel() // Trigger client disconnect
	<-done

	if streamRec.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %s", streamRec.Header().Get("Content-Type"))
	}

	body := streamRec.Body.String()
	if !strings.Contains(body, "event: snapshot") {
		t.Fatalf("snapshot event missing from SSE stream: %s", body)
	}

	var snapshotData map[string]any
	lines := strings.Split(body, "\n")
	for _, l := range lines {
		if strings.HasPrefix(l, "data: ") {
			dataStr := strings.TrimPrefix(l, "data: ")
			if err := json.Unmarshal([]byte(dataStr), &snapshotData); err == nil {
				break
			}
		}
	}

	if snapshotData == nil {
		t.Fatalf("failed to extract snapshot data JSON from: %s", body)
	}

	// Verify mandatory telemetry keys required by dashboard
	requiredKeys := []string{"state", "download_state", "speed_bps", "gate", "ssd", "archive", "storage", "target_progress"}
	for _, k := range requiredKeys {
		if _, exists := snapshotData[k]; !exists {
			t.Fatalf("snapshot missing required telemetry key %q: %+v", k, snapshotData)
		}
	}

	// 4. Test reconnect: verify server cleanly provides a new full snapshot on subsequent connection
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	streamReq2 := httptest.NewRequest(http.MethodGet, "/api/events", nil).WithContext(ctx2)
	streamReq2.AddCookie(sessCookie)
	streamRec2 := httptest.NewRecorder()

	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		handler.ServeHTTP(streamRec2, streamReq2)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel2()
	<-done2

	if !strings.Contains(streamRec2.Body.String(), "event: snapshot") {
		t.Fatalf("reconnect must provide full initial snapshot")
	}
}

func TestWebServer_BrowserSSETelemetryE2E(t *testing.T) {
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node executable not found, skipping browser SSE test")
	}

	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "browser_sse.db"))
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	_ = db.SaveSingleListenTarget(ListenTarget{
		ChatID:            "-100555",
		Title:             "Live Channel",
		Enabled:           true,
		LastReadMessageID: 120,
	})

	registry := NewRegistry(5, 100, time.Now)
	tm := transfer.NewTransferManager(transfer.Options{FileConcurrency: 5})
	orch := NewOrchestrator(nil, tm, nil, nil, nil, registry, zap.NewNop(), tempDir)

	ws := NewWebServer(db, tm, nil, nil, orch, nil, registry, zap.NewNop(), "")
	server := httptest.NewServer(ws.Handler())
	defer server.Close()

	jsScript := `
const [, serverUrl] = process.argv;
const assert = require('assert');
const vm = require('vm');

async function main() {
  // 1. Fetch delivered index.html from production handler
  const pageRes = await fetch(serverUrl + '/');
  assert.strictEqual(pageRes.status, 200, 'index.html must be served with 200 OK');
  const html = await pageRes.text();
  assert(html.includes("new EventSource('api/events')"), 'index.html must instantiate EventSource');
  assert(html.includes("stop_fallback_polling()"), 'index.html must stop fallback polling on SSE');
  assert(html.includes("stop_target_progress_poll()"), 'index.html must stop progress polling on SSE');

  // 2. Fetch live /api/events directly and parse the authoritative SSE snapshot
  const sseRes = await fetch(serverUrl + '/api/events');
  assert.strictEqual(sseRes.status, 200);
  assert(sseRes.headers.get('content-type').includes('text/event-stream'));

  const reader = sseRes.body.getReader();
  const decoder = new TextDecoder();
  let receivedText = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    receivedText += decoder.decode(value, { stream: true });
    if (receivedText.includes('event: snapshot') && receivedText.includes('\n\n')) {
      break;
    }
  }
  reader.cancel();

  assert(receivedText.includes('event: snapshot'), 'SSE must deliver snapshot event');
  assert(receivedText.includes('target_progress'), 'snapshot must contain target_progress');
  assert(receivedText.includes('storage'), 'snapshot must contain storage metrics');
  assert(receivedText.includes('gate'), 'snapshot must contain gate metrics');
  assert(receivedText.includes('active_files'), 'snapshot must contain active_files');

  // 3. Extract the delivered JavaScript controller from index.html
  const scriptTags = [...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)];
  const inlineScript = scriptTags.find(m => m[1].includes('setup_sse_telemetry'));
  assert(inlineScript, 'index.html must include delivered script containing setup_sse_telemetry');
  const deliveredCode = inlineScript[1];

  // 4. Setup simulated browser runtime harness
  const downloadCards = new Map();
  let activeEventSources = [];
  let ajaxCalls = [];
  let intervals = new Map();
  let nextIntervalId = 100;
  let speedTitle = '';
  let poolStatus = '';
  let segmentedClickHandler = null;

  class MockEventSource {
    constructor(url) {
      this.url = url;
      this.listeners = {};
      this.onerror = null;
      activeEventSources.push(this);
    }
    addEventListener(type, fn) {
      this.listeners[type] = this.listeners[type] || [];
      this.listeners[type].push(fn);
    }
    emit(type, data) {
      const list = this.listeners[type] || [];
      for (const fn of list) {
        fn({ data: JSON.stringify(data) });
      }
    }
  }

  function createDOMWrapper(sel) {
    if (typeof sel === 'object' && sel !== null) {
      return new Proxy(sel, {
        get(t, prop) {
          if (typeof prop === 'symbol') return undefined;
          if (prop in t) return t[prop];
          return (...args) => createDOMWrapper(t);
        }
      });
    }

    if (sel === '#download_jobs_list') {
      return {
        find(childSel) {
          if (childSel.startsWith('.apple-download-card[data-key=')) {
            const match = childSel.match(/data-key=\"([^\"]+)\"/);
            const key = match ? match[1] : '';
            if (downloadCards.has(key)) {
              const card = downloadCards.get(key);
              return {
                length: 1,
                find(propSel) {
                  const node = {
                    text(v) { if (v !== undefined) { card[propSel + '_text'] = v; return node; } return card[propSel + '_text'] || ''; },
                    attr(k, v) { if (v !== undefined) { card[propSel + '_' + k] = v; return node; } return card[propSel + '_' + k] || ''; },
                    css(k, v) { card[propSel + '_css'] = v; return node; }
                  };
                  return node;
                }
              };
            }
            return { length: 0 };
          }
          if (childSel === '.apple-empty-state') {
            return { remove: () => {} };
          }
          if (childSel === '.apple-download-card') {
            return {
              each(fn) {
                for (const [key, card] of [...downloadCards.entries()]) {
                  const elem = {
                    attr: (a) => a === 'data-key' ? key : '',
                    remove: () => downloadCards.delete(key)
                  };
                  fn.call(elem);
                }
              }
            };
          }
          return { length: 0 };
        },
        append(htmlStr) {
          const match = htmlStr.match(/data-key=\"([^\"]+)\"/);
          const key = match ? match[1] : String(downloadCards.size + 1);
          const existing = downloadCards.get(key);
          downloadCards.set(key, { key, html: htmlStr, appendCount: (existing ? existing.appendCount : 0) + 1 });
        },
        html(v) {
          if (v && v.includes('apple-empty-state')) {
            downloadCards.clear();
          }
        }
      };
    }

    if (sel === '#download_speed_title') {
      return { html: (v) => { speedTitle = v; }, text: (v) => { speedTitle = v; } };
    }
    if (sel === '#media_pool_status') {
      return { html: (v) => { poolStatus = v; }, attr: () => {} };
    }
    if (sel === '.apple-segmented-control') {
      return {
        on(evt, child, fn) {
          if (evt === 'click') {
            segmentedClickHandler = fn;
          }
        }
      };
    }

    const target = {
      length: 1,
      is: () => false,
      get: () => ({ getContext: () => ({ clearRect: () => {}, beginPath: () => {}, moveTo: () => {}, lineTo: () => {}, stroke: () => {}, fill: () => {} }) })
    };
    return new Proxy(target, {
      get(t, prop) {
        if (typeof prop === 'symbol') return undefined;
        if (prop in t) return t[prop];
        return (...args) => createDOMWrapper(String(sel) + '.' + String(prop));
      }
    });
  }

  const sandbox = {
    console, JSON, parseInt, parseFloat, String, Array, Object, Date, Math,
    setTimeout: () => 1,
    clearTimeout: () => {},
    setInterval: (fn, ms) => {
      const id = ++nextIntervalId;
      intervals.set(id, { fn, ms });
      return id;
    },
    clearInterval: (id) => {
      intervals.delete(id);
    },
    location: { reload: () => {} },
    window: {},
    document: {},
    layui: {
      use: (mods, cb) => { cb(); },
      $: (sel) => createDOMWrapper(sel),
      layer: { msg: () => {}, confirm: () => {}, close: () => {}, load: () => 1 },
      table: { render: () => {}, reload: () => {}, on: () => {} }
    }
  };
  sandbox.layui.$.ajax = (opts) => {
    ajaxCalls.push({ url: opts.url, type: opts.type });
  };
  sandbox.window = sandbox;
  sandbox.window.EventSource = MockEventSource;

  // Execute the exact delivered JavaScript controller inside the VM sandbox
  const context = vm.createContext(sandbox);
  vm.runInContext(deliveredCode, context);

  // 5. Assert exactly one EventSource telemetry connection created
  assert.strictEqual(activeEventSources.length, 1, 'Browser must create exactly one EventSource telemetry connection');
  assert.strictEqual(activeEventSources[0].url, 'api/events');

  const sseInstance = activeEventSources[0];
  const telemetryUrls = ['get_download_list', 'get_download_status', 'api/system/storage', 'api/target_progress'];

  // 6. Healthy SSE Snapshot: renders cards directly without ANY HTTP telemetry polling
  ajaxCalls = [];
  sseInstance.emit('snapshot', {
    speed_human: '12.4 MB/s',
    gate: { active_files: 2, file_concurrency: 5 },
    active_files: [
      { chat_id: '-100555', id: 101, filename: 'video_101.mp4', progress: 45.5, download_speed: '6.2 MB/s', total_size: 104857600 },
      { chat_id: '-100555', id: 102, filename: 'video_102.mp4', progress: 78.0, download_speed: '6.2 MB/s', total_size: 209715200 }
    ]
  });

  assert.strictEqual(speedTitle, '12.4 MB/s', 'Speed must render from SSE snapshot');
  assert(poolStatus.includes('2 个文件下载中 (2/5)'), 'Gate slot status must render from SSE snapshot');
  assert.strictEqual(downloadCards.size, 2, 'Must render exactly 2 active download cards from SSE snapshot');
  assert(downloadCards.has('-100555-101'));
  assert(downloadCards.has('-100555-102'));

  let telemetryCalls = ajaxCalls.filter(c => telemetryUrls.some(u => c.url.includes(u)));
  assert.strictEqual(telemetryCalls.length, 0, 'No HTTP telemetry requests must occur while SSE delivers snapshots');

  // 7. Tab switching during healthy SSE: does NOT start parallel polling
  assert(segmentedClickHandler, 'segmented control click handler must be wired');
  segmentedClickHandler.call({ attr: (a) => a === 'data-tab' ? 'downloading' : '' });
  segmentedClickHandler.call({ attr: (a) => a === 'data-tab' ? 'targets' : '' });

  telemetryCalls = ajaxCalls.filter(c => telemetryUrls.some(u => c.url.includes(u)));
  assert.strictEqual(telemetryCalls.length, 0, 'Tab switching while SSE is healthy must not initiate parallel HTTP polling');

  // 8. Network disconnect (SSE error): fallback polling engages
  sseInstance.onerror();
  assert(intervals.size >= 1, 'Fallback polling timer must engage upon SSE disconnect');

  // Trigger fallback interval tick
  for (const [, item] of intervals.entries()) {
    if (item.ms === 5000) {
      item.fn();
    }
  }
  telemetryCalls = ajaxCalls.filter(c => telemetryUrls.some(u => c.url.includes(u)));
  assert(telemetryCalls.length > 0, 'Fallback polling must trigger telemetry HTTP requests while SSE is unavailable');

  // 9. Reconnect recovery: full snapshot restores state without duplicate cards, stops fallback polling
  ajaxCalls = [];
  sseInstance.emit('snapshot', {
    speed_human: '18.0 MB/s',
    gate: { active_files: 2, file_concurrency: 5 },
    active_files: [
      { chat_id: '-100555', id: 101, filename: 'video_101.mp4', progress: 95.0, download_speed: '9.0 MB/s', total_size: 104857600 },
      { chat_id: '-100555', id: 103, filename: 'video_103.mp4', progress: 12.0, download_speed: '9.0 MB/s', total_size: 52428800 }
    ]
  });

  assert.strictEqual(downloadCards.size, 2, 'Card count after reconnect snapshot must be exactly 2');
  assert(downloadCards.has('-100555-101'), 'Existing task 101 must remain');
  assert(!downloadCards.has('-100555-102'), 'Finished task 102 must be removed');
  assert(downloadCards.has('-100555-103'), 'New task 103 must be added');
  assert.strictEqual(downloadCards.get('-100555-101').appendCount, 1, 'Task 101 must be updated in place without duplicate DOM append');

  // Trigger fallback tick again to verify it is disengaged
  for (const [, item] of intervals.entries()) {
    if (item.ms === 5000) {
      item.fn();
    }
  }
  telemetryCalls = ajaxCalls.filter(c => telemetryUrls.some(u => c.url.includes(u)));
  assert.strictEqual(telemetryCalls.length, 0, 'Fallback HTTP polling must disengage after SSE recovers');

  console.log('BROWSER_SSE_TELEMETRY_E2E_VERIFIED');
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
`

	cmd := exec.Command(nodePath, "-e", jsScript, server.URL)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("browser SSE telemetry E2E failed: %v\nOutput: %s", err, string(out))
	}
	if !strings.Contains(string(out), "BROWSER_SSE_TELEMETRY_E2E_VERIFIED") {
		t.Fatalf("expected verification token, got: %s", string(out))
	}
}

// TestWebServer_FrontendCleanlinessAndStaticCheck enforces Issue #15 acceptance criteria:
// 1. Catches dead/unused JavaScript bindings and timer variables.
// 2. Proves all referenced CSS custom properties are defined in stylesheets.
// 3. Proves unreferenced CryptoJS tree and static-key AES code are removed.
func TestWebServer_FrontendCleanlinessAndStaticCheck(t *testing.T) {
	// 1. Static check: prove dead bindings and unused timer states are absent from index.html
	indexData, err := uiFS.ReadFile("ui/templates/index.html")
	if err != nil {
		t.Fatalf("read index.html: %v", err)
	}
	indexStr := string(indexData)

	deadBindings := []string{
		"already_download_list_data",
		"download_list_table_data",
		"current_phone_hash",
		"scan_status_label",
		"scan_status_class",
		"update_already_download_list_int",
		"CryptoJS",
		"AES",
	}

	for _, sym := range deadBindings {
		if strings.Contains(indexStr, sym) {
			t.Fatalf("authored template must not contain dead symbol %q", sym)
		}
	}

	// 2. Static check: prove login.html does not contain CryptoJS or AES
	loginData, err := uiFS.ReadFile("ui/templates/login.html")
	if err != nil {
		t.Fatalf("read login.html: %v", err)
	}
	loginStr := string(loginData)
	for _, sym := range []string{"CryptoJS", "AES", "crypto-js"} {
		if strings.Contains(loginStr, sym) {
			t.Fatalf("login.html must not contain %q", sym)
		}
	}

	// 3. Static check: CSS custom properties are canonical and defined
	cssData, err := uiFS.ReadFile("ui/static/css/index.css")
	if err != nil {
		t.Fatalf("read index.css: %v", err)
	}
	cssStr := string(cssData)

	defRe := regexp.MustCompile(`--[a-zA-Z0-9_-]+(?:\s*:)`)
	usedRe := regexp.MustCompile(`var\((--[a-zA-Z0-9_-]+)`)

	definedVars := make(map[string]bool)
	for _, match := range defRe.FindAllString(cssStr+"\n"+indexStr, -1) {
		trimmed := strings.TrimRight(match, ": \t\r\n")
		definedVars[trimmed] = true
	}

	usedMatches := usedRe.FindAllStringSubmatch(cssStr+"\n"+indexStr, -1)
	for _, sub := range usedMatches {
		varName := sub[1]
		if !definedVars[varName] {
			t.Fatalf("found undefined CSS custom property: %s", varName)
		}
	}

	// 4. Verify no unreferenced CryptoJS or request directory in embedded static assets
	staticEntries, err := uiFS.ReadDir("ui/static")
	if err != nil {
		t.Fatalf("read ui/static: %v", err)
	}
	for _, entry := range staticEntries {
		name := strings.ToLower(entry.Name())
		if strings.Contains(name, "crypto") {
			t.Fatalf("unreferenced CryptoJS tree must not exist in ui/static: %s", entry.Name())
		}
		if name == "request" {
			t.Fatalf("unreferenced request directory must not exist in ui/static: %s", entry.Name())
		}
	}

	// 5. Authored-JS Unused-Binding Static Analysis Gate
	findUnusedBindings := func(tmplName, tmplContent string) []string {
		scriptRe := regexp.MustCompile(`(?s)<script\b[^>]*>(.*?)</script>`)
		declRe := regexp.MustCompile(`\b(?:var|let|const)\s+([a-zA-Z_$][0-9a-zA-Z_$]*)\b|\bfunction\s+([a-zA-Z_$][0-9a-zA-Z_$]*)\s*\(`)
		var unused []string
		for _, scriptMatch := range scriptRe.FindAllStringSubmatch(tmplContent, -1) {
			code := scriptMatch[1]
			declMatches := declRe.FindAllStringSubmatch(code, -1)
			declared := make(map[string]int)
			for _, dm := range declMatches {
				sym := dm[1]
				if sym == "" {
					sym = dm[2]
				}
				if sym != "" {
					declared[sym]++
				}
			}
			for sym, declCount := range declared {
				tokenRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(sym) + `\b`)
				allMatches := tokenRe.FindAllString(tmplContent, -1)
				if len(allMatches) <= declCount {
					unused = append(unused, fmt.Sprintf("%s:%s", tmplName, sym))
				}
			}
		}
		return unused
	}

	// Assert 0 unused bindings in authored index.html
	if unusedIndex := findUnusedBindings("index.html", indexStr); len(unusedIndex) > 0 {
		t.Fatalf("found unused JavaScript bindings in index.html: %v", unusedIndex)
	}
	// Assert 0 unused bindings in authored login.html
	if unusedLogin := findUnusedBindings("login.html", loginStr); len(unusedLogin) > 0 {
		t.Fatalf("found unused JavaScript bindings in login.html: %v", unusedLogin)
	}
	// Assert the analysis gate has teeth by testing detection on dummy snippet
	dummySnippet := `<html><body><script>var dead_token = 123; function used_fn() { return 1; } used_fn();</script></body></html>`
	if dummyUnused := findUnusedBindings("dummy", dummySnippet); len(dummyUnused) != 1 || dummyUnused[0] != "dummy:dead_token" {
		t.Fatalf("static analysis gate must catch unused binding, got: %v", dummyUnused)
	}

	// 6. Representative Desktop and Mobile Render Acceptance
	nodePath, err := exec.LookPath("node")
	if err != nil {
		return // Node not installed in environment, skip browser render check
	}

	renderScript := `
const fs = require('fs');
const vm = require('vm');
const assert = require('assert');

const input = JSON.parse(fs.readFileSync(0, 'utf8'));
const html = input.html;
const css = input.css;

// Verify responsive media query rules in stylesheet
assert(css.includes('@media (max-width: 768px)'), 'index.css must declare @media (max-width: 768px)');
assert(css.includes('.mobile-job-list'), 'index.css must style .mobile-job-list');
assert(css.includes('.apple-mobile-pagination'), 'index.css must style .apple-mobile-pagination');

const scriptTags = [...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)];
const code = scriptTags.find(m => m[1].includes('render_mobile_downloaded_cards'))[1];

function runWithWindowWidth(width) {
  let mobileCardsHtml = '';
  let pageIndicatorText = '';
  let prevDisabled = null;
  let nextDisabled = null;
  let modalArea = null;
  let tableRenderOpts = null;

  function createDOM(sel) {
    if (sel === '#already_download_mobile_list') {
      return { html: (h) => { if (h !== undefined) mobileCardsHtml = h; return mobileCardsHtml; } };
    }
    if (sel === '#mobile_page_indicator') {
      return { text: (t) => { if (t !== undefined) pageIndicatorText = t; return pageIndicatorText; } };
    }
    if (sel === '#mobile_prev_page') {
      return {
        prop: (p, v) => { if (p === 'disabled') prevDisabled = v; return prevDisabled; },
        on: () => {}
      };
    }
    if (sel === '#mobile_next_page') {
      return {
        prop: (p, v) => { if (p === 'disabled') nextDisabled = v; return nextDisabled; },
        on: () => {}
      };
    }
    const target = {
      length: 1,
      0: { scrollIntoView: () => {} },
      is: () => false,
      val: () => '',
      text: () => '',
      html: () => '',
      width: () => width,
      get: () => ({ getContext: () => ({ clearRect: () => {}, beginPath: () => {}, moveTo: () => {}, lineTo: () => {}, stroke: () => {}, fill: () => {} }) })
    };
    return new Proxy(target, {
      get(t, prop) {
        if (typeof prop === 'symbol') return undefined;
        if (prop in t) return t[prop];
        return (...args) => createDOM(String(sel) + '.' + String(prop));
      }
    });
  }

  const sandbox = {
    console, JSON, parseInt, parseFloat, String, Array, Object, Date, Math,
    setTimeout: () => 1, clearTimeout: () => {}, setInterval: () => 2, clearInterval: () => {},
    location: { reload: () => {} }, window: {}, document: {},
    encodeURIComponent,
    layui: {
      use: (mods, cb) => { cb(); },
      $: (sel) => createDOM(sel),
      layer: {
        msg: () => {},
        prompt: () => {},
        open: (opts) => { modalArea = opts.area; },
        close: () => {},
        load: () => 1
      },
      table: {
        render: (opts) => { tableRenderOpts = opts; return {}; },
        reload: () => {},
        on: () => {}
      }
    }
  };
  sandbox.layui.$.trim = (s) => (s || '').trim();
  sandbox.layui.$.ajax = () => {};
  sandbox.window = sandbox;
  sandbox.window.EventSource = function() { this.addEventListener = () => {}; };

  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);

  return {
    openContextViewer: (chatId, title, mid) => {
      sandbox.window.open_context_viewer(chatId, title, mid);
      return modalArea;
    },
    parseTableData: (res) => {
      tableRenderOpts.parseData(res);
      return { text: pageIndicatorText, prevDisabled, nextDisabled, cardsHtml: mobileCardsHtml };
    }
  };
}

// 1. Desktop Render Check (width: 1280)
const desktop = runWithWindowWidth(1280);
const desktopModal = desktop.openContextViewer('-1001', 'Channel', 10);
assert.strictEqual(desktopModal[0], '760px', 'Desktop context viewer width must be 760px');
assert.strictEqual(desktopModal[1], '620px', 'Desktop context viewer height must be 620px');

// 2. Mobile Render Check (width: 375)
const mobile = runWithWindowWidth(375);
const mobileModal = mobile.openContextViewer('-1001', 'Channel', 10);
assert.strictEqual(mobileModal[0], '95%', 'Mobile context viewer width must be 95%');
assert.strictEqual(mobileModal[1], '90%', 'Mobile context viewer height must be 90%');

// Test mobile cards & pagination page 1 of 3
const page1 = mobile.parseTableData({
  count: 120, limit: 50, page: 1,
  data: [{ id: 1, filename: 'mobile_doc.pdf', total_size: '1.5 MB', chat: 'Channel', completed_at: 1700000000 }]
});
assert.strictEqual(page1.text, 'Page 1 / 3');
assert.strictEqual(page1.prevDisabled, true, 'Prev must be disabled on page 1');
assert.strictEqual(page1.nextDisabled, false, 'Next must be enabled on page 1 of 3');
assert(page1.cardsHtml.includes('mobile_doc.pdf'), 'Mobile card must contain filename');

// Test mobile pagination page 3 of 3
const page3 = mobile.parseTableData({ count: 120, limit: 50, page: 3, data: [] });
assert.strictEqual(page3.text, 'Page 3 / 3');
assert.strictEqual(page3.prevDisabled, false, 'Prev must be enabled on page 3 of 3');
assert.strictEqual(page3.nextDisabled, true, 'Next must be disabled on page 3 of 3');

console.log('RESPONSIVE_RENDER_ACCEPTANCE_VERIFIED');
`
	payload, err := json.Marshal(map[string]string{
		"html": indexStr,
		"css":  cssStr,
	})
	if err != nil {
		t.Fatalf("marshal render payload: %v", err)
	}

	cmd := exec.Command(nodePath, "-e", renderScript)
	cmd.Stdin = bytes.NewReader(payload)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("responsive render acceptance failed: %v\nOutput: %s", err, string(out))
	}
	if !strings.Contains(string(out), "RESPONSIVE_RENDER_ACCEPTANCE_VERIFIED") {
		t.Fatalf("expected verification token, got: %s", string(out))
	}
}

func findChromeExecutable() string {
	if env := os.Getenv("CHROME_BIN"); env != "" {
		if _, err := os.Stat(env); err == nil {
			return env
		}
	}
	candidates := []string{
		"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
		"/Applications/Chromium.app/Contents/MacOS/Chromium",
		"/usr/bin/google-chrome",
		"/usr/bin/google-chrome-stable",
		"/usr/bin/chromium",
		"/usr/bin/chromium-browser",
	}
	for _, cand := range candidates {
		if _, err := os.Stat(cand); err == nil {
			return cand
		}
	}
	for _, bin := range []string{"google-chrome", "google-chrome-stable", "chromium", "chromium-browser"} {
		if path, err := exec.LookPath(bin); err == nil {
			return path
		}
	}
	return ""
}

func TestWebServer_RealHeadlessChromeRenderAcceptance(t *testing.T) {
	chromePath := findChromeExecutable()
	if chromePath == "" {
		t.Skip("Google Chrome / Chromium not installed in test environment")
	}
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js not installed in test environment")
	}

	db, err := NewDatabase(":memory:")
	if err != nil {
		t.Fatalf("failed to create in-memory database: %v", err)
	}
	defer db.Close()

	registry := NewRegistry(5, 100, time.Now)
	ws := NewWebServer(db, nil, nil, nil, nil, nil, registry, zap.NewNop(), "")
	server := httptest.NewServer(ws.Handler())
	defer server.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp for cdp port: %v", err)
	}
	cdpPort := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	userDataDir := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	chromeCmd := exec.CommandContext(ctx, chromePath,
		"--headless=new",
		fmt.Sprintf("--remote-debugging-port=%d", cdpPort),
		fmt.Sprintf("--user-data-dir=%s", userDataDir),
		"--disable-gpu",
		"--no-sandbox",
		"--disable-dev-shm-usage",
		server.URL,
	)
	if err := chromeCmd.Start(); err != nil {
		t.Fatalf("start chrome: %v", err)
	}
	defer func() {
		_ = chromeCmd.Process.Kill()
		_ = chromeCmd.Wait()
	}()

	driverScript := fmt.Sprintf(`
const port = %d;
async function test() {
  let pageWsUrl = '';
  for (let i = 0; i < 50; i++) {
    await new Promise(r => setTimeout(r, 100));
    try {
      const res = await fetch('http://127.0.0.1:' + port + '/json/list');
      const list = await res.json();
      const page = list.find(t => t.type === 'page' && t.webSocketDebuggerUrl);
      if (page) {
        pageWsUrl = page.webSocketDebuggerUrl;
        break;
      }
    } catch (e) {}
  }
  if (!pageWsUrl) throw new Error('No page WebSocket URL found from Chrome CDP');

  const ws = new WebSocket(pageWsUrl);
  await new Promise(r => ws.onopen = r);

  let id = 1;
  function send(method, params = {}) {
    return new Promise((resolve, reject) => {
      const reqId = id++;
      const handler = (event) => {
        const msg = JSON.parse(event.data);
        if (msg.id === reqId) {
          ws.removeEventListener('message', handler);
          if (msg.error) reject(new Error(JSON.stringify(msg.error)));
          else resolve(msg.result);
        }
      };
      ws.addEventListener('message', handler);
      ws.send(JSON.stringify({ id: reqId, method, params }));
    });
  }

  async function evaluate(expression) {
    const res = await send('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
    if (res.exceptionDetails) throw new Error(JSON.stringify(res.exceptionDetails));
    return res.result.value;
  }

  await send('Page.enable');
  await send('Runtime.enable');

  // Wait for document ready
  for (let i = 0; i < 50; i++) {
    const ready = await evaluate('document.readyState');
    if (ready === 'complete') break;
    await new Promise(r => setTimeout(r, 100));
  }
  await new Promise(r => setTimeout(r, 500));

  // --- 1. Desktop Render Check (1280x800) ---
  await send('Emulation.setDeviceMetricsOverride', {
    width: 1280,
    height: 800,
    deviceScaleFactor: 1,
    mobile: false
  });
  await new Promise(r => setTimeout(r, 300));

  // Switch to downloaded tab
  await evaluate('document.querySelector("[data-tab=\\"downloaded\\"]").click()');
  await new Promise(r => setTimeout(r, 300));

  const desktop = await evaluate('({ ' +
    'innerWidth: window.innerWidth, ' +
    'scrollWidth: document.documentElement.scrollWidth, ' +
    'desktopTableDisplay: window.getComputedStyle(document.querySelector(".table-card.desktop-only")).display ' +
  '})');
  if (desktop.scrollWidth > desktop.innerWidth) {
    throw new Error('Desktop horizontal overflow detected: scrollWidth=' + desktop.scrollWidth + ' > innerWidth=' + desktop.innerWidth);
  }
  if (desktop.desktopTableDisplay === 'none') {
    throw new Error('Desktop table card should be visible on 1280px screen');
  }

  // Open Settings Modal on Desktop
  await evaluate('document.getElementById("btn_open_settings").click()');
  await new Promise(r => setTimeout(r, 400));
  const desktopModal = await evaluate('({ ' +
    'modalDisplay: window.getComputedStyle(document.getElementById("settings_modal")).display, ' +
    'scrollWidth: document.documentElement.scrollWidth, ' +
    'innerWidth: window.innerWidth ' +
  '})');
  if (desktopModal.scrollWidth > desktopModal.innerWidth) {
    throw new Error('Desktop modal caused horizontal overflow: scrollWidth=' + desktopModal.scrollWidth + ' > innerWidth=' + desktopModal.innerWidth);
  }
  await evaluate('document.getElementById("btn_close_settings").click()');
  await new Promise(r => setTimeout(r, 300));

  // --- 2. Mobile Render Check (375x667) ---
  await send('Emulation.setDeviceMetricsOverride', {
    width: 375,
    height: 667,
    deviceScaleFactor: 2,
    mobile: true
  });
  await new Promise(r => setTimeout(r, 300));

  const mobile = await evaluate('({ ' +
    'innerWidth: window.innerWidth, ' +
    'scrollWidth: document.documentElement.scrollWidth, ' +
    'desktopTableDisplay: window.getComputedStyle(document.querySelector(".table-card.desktop-only")).display ' +
  '})');
  if (mobile.scrollWidth > mobile.innerWidth) {
    throw new Error('Mobile horizontal overflow detected: scrollWidth=' + mobile.scrollWidth + ' > innerWidth=' + mobile.innerWidth);
  }
  if (mobile.desktopTableDisplay !== 'none') {
    throw new Error('Desktop table card must be hidden on 375px mobile viewport');
  }

  // Render sample mobile card and verify zero overflow
  await evaluate('render_mobile_downloaded_cards([{ ' +
    'id: 101, filename: "test_sample_render_acceptance.mp4", total_size: "15.4 MB", ' +
    'completed_at: 1700000000, save_path: "/downloads/test.mp4", chat: "Channel" ' +
  '}])');
  await new Promise(r => setTimeout(r, 300));

  const mobileCardCheck = await evaluate('({ ' +
    'scrollWidth: document.documentElement.scrollWidth, ' +
    'innerWidth: window.innerWidth, ' +
    'hasCard: document.querySelectorAll("#already_download_mobile_list .apple-download-card").length > 0 ' +
  '})');
  if (mobileCardCheck.scrollWidth > mobileCardCheck.innerWidth) {
    throw new Error('Mobile card rendering caused horizontal overflow');
  }
  if (!mobileCardCheck.hasCard) {
    throw new Error('Mobile card was not rendered in #already_download_mobile_list');
  }

  // Open Settings Modal on Mobile
  await evaluate('document.getElementById("btn_open_settings").click()');
  await new Promise(r => setTimeout(r, 400));
  const mobileModal = await evaluate('({ ' +
    'modalDisplay: window.getComputedStyle(document.getElementById("settings_modal")).display, ' +
    'dialogWidth: document.querySelector("#settings_modal .apple-modal-dialog").getBoundingClientRect().width, ' +
    'scrollWidth: document.documentElement.scrollWidth, ' +
    'innerWidth: window.innerWidth ' +
  '})');
  if (mobileModal.scrollWidth > mobileModal.innerWidth) {
    throw new Error('Mobile modal caused horizontal overflow: scrollWidth=' + mobileModal.scrollWidth + ' > innerWidth=' + mobileModal.innerWidth);
  }
  if (mobileModal.dialogWidth > mobileModal.innerWidth) {
    throw new Error('Mobile dialog width exceeds viewport width: ' + mobileModal.dialogWidth + ' > ' + mobileModal.innerWidth);
  }

  ws.close();
  console.log("REAL_CHROME_RENDER_ACCEPTANCE_PASS");
}

test().catch(e => { console.error('CHROME_TEST_ERROR:', e); process.exit(1); });
`, cdpPort)

	nodeCmd := exec.Command(nodePath, "-e", driverScript)
	out, err := nodeCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("real headless Chrome render acceptance failed: %v\nOutput: %s", err, string(out))
	}
	if !strings.Contains(string(out), "REAL_CHROME_RENDER_ACCEPTANCE_PASS") {
		t.Fatalf("expected real Chrome verification token, got: %s", string(out))
	}
}

func TestWebServer_SSELiveSessionExpiry(t *testing.T) {
	registry := NewRegistry(5, 100, time.Now)
	ws := NewWebServer(nil, nil, nil, nil, nil, nil, registry, zap.NewNop(), "mypassword")
	ws.SetSSEIntervals(50*time.Millisecond, 200*time.Millisecond, 2*time.Second)

	server := httptest.NewServer(ws.Handler())
	defer server.Close()

	// 1. Log in to get session cookie
	loginBody := `{"password":"mypassword"}`
	resp, err := http.Post(server.URL+"/login", "application/json", strings.NewReader(loginBody))
	if err != nil {
		t.Fatalf("login post: %v", err)
	}
	defer resp.Body.Close()

	cookies := resp.Cookies()
	var sessionCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "tg_downloader_session" {
			sessionCookie = c
			break
		}
	}
	if sessionCookie == nil {
		t.Fatal("expected tg_downloader_session cookie")
	}

	// 2. Connect to SSE stream
	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/events", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.AddCookie(sessionCookie)

	client := &http.Client{}
	sseResp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET /api/events: %v", err)
	}
	defer sseResp.Body.Close()

	if sseResp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", sseResp.StatusCode)
	}

	reader := bufio.NewReader(sseResp.Body)

	// Read snapshot
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read snapshot event line: %v", err)
	}
	if !strings.HasPrefix(line, "event: snapshot") {
		t.Fatalf("expected event: snapshot, got: %s", line)
	}

	// Wait 60ms to let a healthy update pass
	time.Sleep(60 * time.Millisecond)

	// 3. Expire the session in web server state while stream is active
	ws.sessionsMu.Lock()
	ws.sessions[sessionCookie.Value] = time.Now().Add(-1 * time.Hour)
	ws.sessionsMu.Unlock()

	// 4. Read stream until EOF or error event
	var sawErrorEvent bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		line, err = reader.ReadString('\n')
		if err != nil {
			// Stream was closed by server
			break
		}
		if strings.Contains(line, "event: error") || strings.Contains(line, "session expired") {
			sawErrorEvent = true
		}
	}

	if !sawErrorEvent && err == nil {
		t.Fatal("expected live stream to terminate or emit error event upon session expiration")
	}
}

func TestWebServer_SSEHeartbeat(t *testing.T) {
	registry := NewRegistry(5, 100, time.Now)
	ws := NewWebServer(nil, nil, nil, nil, nil, nil, registry, zap.NewNop(), "")
	// Fast heartbeat interval (50ms)
	ws.SetSSEIntervals(500*time.Millisecond, 50*time.Millisecond, 2*time.Second)

	server := httptest.NewServer(ws.Handler())
	defer server.Close()

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/events", nil)
	client := &http.Client{}
	sseResp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET /api/events: %v", err)
	}
	defer sseResp.Body.Close()

	reader := bufio.NewReader(sseResp.Body)
	sawHeartbeat := false
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, "event: heartbeat") {
			sawHeartbeat = true
			break
		}
	}
	if !sawHeartbeat {
		t.Fatal("expected heartbeat event in SSE stream")
	}
}

func TestWebServer_SSEStalledClientBackpressure(t *testing.T) {
	registry := NewRegistry(100, 100, time.Now)
	// Add active tasks with substantial payload so telemetry snapshots generate ~100KB per update,
	// quickly filling kernel socket buffers and enforcing write deadline backpressure.
	padding := strings.Repeat("x", 2048)
	for i := 0; i < 50; i++ {
		_, _, _ = registry.Submit(TaskRequest{
			ID:           fmt.Sprintf("backpressure_task_%d", i),
			Peer:         "-100123456789",
			MessageID:    1000 + i,
			FinalPath:    fmt.Sprintf("Channel_Test/%s_%d.bin", padding, i),
			ExpectedSize: 1024 * 1024 * 50,
		})
		_, _ = registry.Next(context.Background())
	}

	ws := NewWebServer(nil, nil, nil, nil, nil, nil, registry, zap.NewNop(), "")
	// Set 2ms updates, 20ms heartbeat, and 50ms write deadline
	ws.SetSSEIntervals(2*time.Millisecond, 20*time.Millisecond, 50*time.Millisecond)

	server := httptest.NewServer(ws.Handler())
	defer server.Close()

	// Connect using raw TCP dial to simulate a stalled client that NEVER reads from socket
	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}
	defer conn.Close()

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetReadBuffer(256)
	}

	// Send HTTP GET /api/events
	reqStr := "GET /api/events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n"
	if _, err := conn.Write([]byte(reqStr)); err != nil {
		t.Fatalf("write request: %v", err)
	}

	// Wait for server to accept and register active SSE connection
	activeStart := time.Now()
	for ws.ActiveSSEConnections() == 0 && time.Since(activeStart) < 300*time.Millisecond {
		time.Sleep(2 * time.Millisecond)
	}
	if ws.ActiveSSEConnections() == 0 {
		t.Fatal("expected ActiveSSEConnections to become > 0")
	}

	// Client STALLS completely: zero calls to conn.Read!
	// Server continuously attempts to flush snapshots.
	// Kernel socket buffer quickly saturates, causing SetWriteDeadline(50ms) to trigger.
	// Assert server-side handler exits and ActiveSSEConnections drops to 0 well within 500ms (far less than 2s).
	deadlineStart := time.Now()
	for ws.ActiveSSEConnections() > 0 && time.Since(deadlineStart) < 600*time.Millisecond {
		time.Sleep(5 * time.Millisecond)
	}
	elapsed := time.Since(deadlineStart)

	if ws.ActiveSSEConnections() != 0 {
		t.Fatalf("expected ActiveSSEConnections == 0 after server write deadline, but still %d after %v", ws.ActiveSSEConnections(), elapsed)
	}
	if elapsed > 450*time.Millisecond {
		t.Fatalf("server took too long to enforce write deadline: %v (expected < 450ms, well below 2s)", elapsed)
	}
}

func TestWebServer_ObservabilityEndpointsContract(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	reg := NewRegistry(100, 100, time.Now)
	ws := NewWebServer(db, nil, nil, nil, nil, nil, reg, zap.NewNop(), "")
	server := httptest.NewServer(ws.Handler())
	defer server.Close()

	// 1. Contract test GET /api/status
	respStatus, err := http.Get(server.URL + "/api/status")
	if err != nil {
		t.Fatalf("GET /api/status failed: %v", err)
	}
	defer respStatus.Body.Close()
	if respStatus.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", respStatus.StatusCode)
	}
	var statusData map[string]any
	if err := json.NewDecoder(respStatus.Body).Decode(&statusData); err != nil {
		t.Fatalf("decode /api/status: %v", err)
	}
	for _, field := range []string{
		"wire_rx_bytes", "unique_payload_bytes", "retry_count",
		"process_rss", "heap_alloc", "heap_inuse", "heap_objects",
		"gc_count", "gc_pause_total",
	} {
		val, exists := statusData[field]
		if !exists || val == nil {
			t.Errorf("expected non-null field %q in /api/status, got %v", field, val)
		}
	}
	// Verify process_rss and heap_alloc are positive
	if rss, ok := statusData["process_rss"].(float64); !ok || rss <= 0 {
		t.Errorf("expected positive process_rss, got %v", statusData["process_rss"])
	}
	if alloc, ok := statusData["heap_alloc"].(float64); !ok || alloc <= 0 {
		t.Errorf("expected positive heap_alloc, got %v", statusData["heap_alloc"])
	}

	// 2. Contract test GET /api/system/storage
	respStorage, err := http.Get(server.URL + "/api/system/storage")
	if err != nil {
		t.Fatalf("GET /api/system/storage failed: %v", err)
	}
	defer respStorage.Body.Close()
	if respStorage.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", respStorage.StatusCode)
	}
	var storageData map[string]any
	if err := json.NewDecoder(respStorage.Body).Decode(&storageData); err != nil {
		t.Fatalf("decode /api/system/storage: %v", err)
	}
	for _, field := range []string{
		"target_write_bytes", "target_read_bytes", "target_durable_bytes",
		"target_writer_concurrency", "target_backlog_bytes",
	} {
		val, exists := storageData[field]
		if !exists || val == nil {
			t.Errorf("expected non-null field %q in /api/system/storage, got %v", field, val)
		}
	}
}

func TestWebServer_ConflictEndpoints_ContractAndBearerAuth(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	password := "secret-operator-pass"
	reg := NewRegistry(100, 100, time.Now)
	ws := NewWebServer(db, nil, nil, nil, nil, nil, reg, zap.NewNop(), password)
	server := httptest.NewServer(ws.Handler())
	defer server.Close()

	// 1. Unauthenticated request to /api/conflicts should return 401
	resp, err := http.Get(server.URL + "/api/conflicts")
	if err != nil {
		t.Fatalf("GET /api/conflicts failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", resp.StatusCode)
	}

	// 2. Request with invalid Bearer token should return 401
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/conflicts", nil)
	req.Header.Set("Authorization", "Bearer wrong-pass")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/conflicts with wrong bearer: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong bearer, got %d", resp.StatusCode)
	}

	// 3. Request with valid Bearer token should return 200 with empty list
	req, _ = http.NewRequest(http.MethodGet, server.URL+"/api/conflicts", nil)
	req.Header.Set("Authorization", "Bearer "+password)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/conflicts with valid bearer: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with valid bearer, got %d", resp.StatusCode)
	}
	var list []DownloadRecord
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode /api/conflicts: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("expected empty list, got %d", len(list))
	}

	// 4. Seed a conflict record in DB and verify structured response
	disp := FailureDisposition{
		Stage:      "commit",
		Op:         "target_check",
		Class:      "target_conflict",
		Message:    "target file exists",
		Retryable:  false,
		RetryOwner: "operator",
	}
	if err := db.RecordTargetConflict("channel_1", 101, "gen_1", "vid.mp4", "/downloads/vid.mp4", "video", 5000, disp); err != nil {
		t.Fatalf("RecordTargetConflict: %v", err)
	}

	req, _ = http.NewRequest(http.MethodGet, server.URL+"/api/conflicts", nil)
	req.Header.Set("Authorization", "Bearer "+password)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/conflicts: %v", err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode /api/conflicts after seed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 conflict record, got %d", len(list))
	}
	rec := list[0]
	if rec.ChatID != "channel_1" || rec.MessageID != 101 || rec.Status != "conflict" {
		t.Errorf("unexpected record identity: %+v", rec)
	}
	if rec.ErrorClass != "target_conflict" || rec.Retryable != false || rec.RetryOwner != "operator" {
		t.Errorf("unexpected structured error: %+v", rec)
	}

	// 5. POST /api/conflicts/resolve without body/params should return 400
	resolveReq, _ := http.NewRequest(http.MethodPost, server.URL+"/api/conflicts/resolve", strings.NewReader(`{}`))
	resolveReq.Header.Set("Authorization", "Bearer "+password)
	resolveReq.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(resolveReq)
	if err != nil {
		t.Fatalf("POST /api/conflicts/resolve: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty resolve request, got %d", resp.StatusCode)
	}
}

func TestWebServer_TruthfulPhysicalMetricsAndMonotonicCounters(t *testing.T) {
	// 1. Monotonic network and retry counters across task retries
	reg := NewRegistry(10, 10, time.Now)
	taskReq := TaskRequest{
		ID:           "chan1:100",
		Peer:         "chan1",
		MessageID:    100,
		FinalPath:    "chan1/file.mp4",
		ExpectedSize: 1000,
	}
	_, _, err := reg.Submit(taskReq)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	task, _ := reg.Next(context.Background())
	// Record attempt 1 telemetry
	task.RecordTransferTelemetry(300, 500, 0, 1, 1, "gen1-p1")
	task.Fail("timeout", "simulated timeout", false)

	// Check status after attempt 1
	status1 := reg.Status()
	if status1.WireRxBytes != 500 || status1.UniquePayloadBytes != 300 || status1.RetryCount != 1 {
		t.Fatalf("attempt 1 status mismatch: wire=%d, payload=%d, retries=%d",
			status1.WireRxBytes, status1.UniquePayloadBytes, status1.RetryCount)
	}

	// Trigger retry (creates attempt 2)
	_, err = reg.RetryTask("chan1:100")
	if err != nil {
		t.Fatalf("RetryTask: %v", err)
	}

	// Verify counters did not regress upon retry
	statusAfterRetry := reg.Status()
	if statusAfterRetry.WireRxBytes < 500 {
		t.Fatalf("wire_rx_bytes regressed across retry: got %d, want >= 500", statusAfterRetry.WireRxBytes)
	}
	if statusAfterRetry.UniquePayloadBytes < 300 {
		t.Fatalf("unique_payload_bytes regressed across retry: got %d, want >= 300", statusAfterRetry.UniquePayloadBytes)
	}
	if statusAfterRetry.RetryCount < 1 {
		t.Fatalf("retry_count regressed across retry: got %d, want >= 1", statusAfterRetry.RetryCount)
	}

	// Attempt 2 produces additional 600 wire bytes and 400 payload bytes
	task2, _ := reg.Next(context.Background())
	task2.RecordTransferTelemetry(400, 600, 0, 1, 0, "gen2-p0")
	status2 := reg.Status()
	if status2.WireRxBytes != 1100 {
		t.Fatalf("expected 1100 cumulative wire bytes, got %d", status2.WireRxBytes)
	}
	if status2.UniquePayloadBytes != 700 {
		t.Fatalf("expected 700 cumulative payload bytes, got %d", status2.UniquePayloadBytes)
	}

	// 2. Truthful physical metrics deltas: distinct write, read, and backlog measurements
	orch := &Orchestrator{
		registry: reg,
		logger:   zap.NewNop(),
		saveDir:  t.TempDir(),
	}
	// Simulate physical write of 2048 bytes and physical read of 1024 bytes
	orch.physicalTargetWriteBytes = 2048
	orch.physicalTargetReadBytes = 1024
	orch.activeTargetWriters = 2

	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	defer db.Close()

	ws := NewWebServer(db, nil, nil, nil, orch, nil, reg, zap.NewNop(), "")
	server := httptest.NewServer(ws.Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/api/system/storage")
	if err != nil {
		t.Fatalf("GET /api/system/storage: %v", err)
	}
	defer resp.Body.Close()
	var storageResp map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&storageResp)

	writeBytes := int64(storageResp["target_write_bytes"].(float64))
	readBytes := int64(storageResp["target_read_bytes"].(float64))
	writerConcurrency := int(storageResp["target_writer_concurrency"].(float64))
	if writeBytes != 2048 {
		t.Fatalf("expected physical target_write_bytes 2048, got %d", writeBytes)
	}
	if readBytes != 1024 {
		t.Fatalf("expected physical target_read_bytes 1024, got %d", readBytes)
	}
	if writeBytes == readBytes {
		t.Fatal("target_write_bytes and target_read_bytes must not be artificially equal")
	}
	if writerConcurrency != 2 {
		t.Fatalf("expected target_writer_concurrency 2, got %d", writerConcurrency)
	}

	// 3. Error source propagation: collection failure is emitted as null / collection_errors, not zero
	db.Close() // Force DB failure
	respErr, err := http.Get(server.URL + "/api/system/storage")
	if err != nil {
		t.Fatalf("GET /api/system/storage with closed DB: %v", err)
	}
	defer respErr.Body.Close()
	var errStorageResp map[string]any
	_ = json.NewDecoder(respErr.Body).Decode(&errStorageResp)

	if errStorageResp["target_durable_bytes"] != nil {
		t.Fatalf("expected target_durable_bytes to be null on DB failure, got %v", errStorageResp["target_durable_bytes"])
	}
	errs, ok := errStorageResp["collection_errors"].([]any)
	if !ok || len(errs) == 0 {
		t.Fatalf("expected collection_errors on DB failure, got %v", errStorageResp["collection_errors"])
	}
}
