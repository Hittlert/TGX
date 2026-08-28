package daemon

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPSubmitLookupStatusAndIdempotency(t *testing.T) {
	registry := NewRegistry(2, 100, time.Now)
	handler := NewHandler(registry)
	body := `{"id":"chat:1:1","peer":"-100123","message_id":1,"final_path":"Group/1.mp4","expected_size":100}`

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/tasks", strings.NewReader(body)))
	if response.Code != http.StatusAccepted {
		t.Fatalf("submit status=%d body=%s", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/tasks", strings.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("idempotent status=%d body=%s", response.Code, response.Body.String())
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/tasks/chat:1:1", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("lookup status=%d body=%s", response.Code, response.Body.String())
	}
	var task TaskSnapshot
	if err := json.NewDecoder(response.Body).Decode(&task); err != nil {
		t.Fatal(err)
	}
	if task.ID != "chat:1:1" || task.State != StateQueued {
		t.Fatalf("unexpected lookup: %#v", task)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"backend":"tdl"`) {
		t.Fatalf("status response=%d %s", response.Code, response.Body.String())
	}
}

func TestHTTPPauseResumeAndHealth(t *testing.T) {
	registry := NewRegistry(2, 100, time.Now)
	handler := NewHandler(registry)

	for _, tc := range []struct {
		body   string
		paused bool
	}{
		{`{"action":"pause"}`, true},
		{`{"action":"resume"}`, false},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/control", strings.NewReader(tc.body)))
		if response.Code != http.StatusOK || registry.Status().Paused != tc.paused {
			t.Fatalf("control %s: code=%d paused=%v", tc.body, response.Code, registry.Status().Paused)
		}
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != "ok" {
		t.Fatalf("health=%d %q", response.Code, response.Body.String())
	}
}

func TestHTTPRejectsInvalidRequestsAndFullQueue(t *testing.T) {
	registry := NewRegistry(1, 100, time.Now)
	handler := NewHandler(registry)
	valid := `{"id":"one","peer":"peer","message_id":1,"final_path":"one.bin","expected_size":1}`

	tests := []struct {
		method string
		path   string
		body   string
		code   int
	}{
		{http.MethodGet, "/api/tasks", "", http.StatusMethodNotAllowed},
		{http.MethodPost, "/api/tasks", "not-json", http.StatusBadRequest},
		{http.MethodPost, "/api/tasks", `{"id":"bad","peer":"peer","message_id":1,"final_path":"../bad","expected_size":1}`, http.StatusBadRequest},
		{http.MethodPost, "/api/control", `{"action":"stop"}`, http.StatusBadRequest},
		{http.MethodGet, "/api/tasks/missing", "", http.StatusNotFound},
		{http.MethodGet, "/missing", "", http.StatusNotFound},
	}
	for _, tc := range tests {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if response.Code != tc.code {
			t.Fatalf("%s %s returned %d, want %d: %s", tc.method, tc.path, response.Code, tc.code, response.Body.String())
		}
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/tasks", strings.NewReader(valid)))
	if response.Code != http.StatusAccepted {
		t.Fatalf("initial queue fill failed: %d", response.Code)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/tasks", strings.NewReader(strings.Replace(valid, "one", "two", 1))))
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("full queue returned %d: %s", response.Code, response.Body.String())
	}
}
