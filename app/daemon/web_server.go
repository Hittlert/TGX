package daemon

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/transfer"
	"github.com/Hittlert/TGX/internal/fscommit"
	"github.com/Hittlert/TGX/pkg/consts"
)

//go:embed ui/*
var uiFS embed.FS

type WebServer struct {
	db           *Database
	transferMgr  *transfer.TransferManager
	ssdAdmission *fscommit.SSDAdmission
	proxyManager *ProxyManager
	orchestrator *Orchestrator
	access       TelegramAccess
	registry     *Registry
	logger               *zap.Logger
	password             string
	sessionsMu           sync.RWMutex
	sessions             map[string]time.Time
	authWizard           *AuthWizard
	sseUpdateInterval    time.Duration
	sseHeartbeatInterval time.Duration
	sseWriteTimeout      time.Duration
}

func (s *WebServer) SetAuthWizard(w *AuthWizard) {
	s.authWizard = w
}

func (s *WebServer) SetSSEIntervals(update, heartbeat, writeTimeout time.Duration) {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	if update > 0 {
		s.sseUpdateInterval = update
	}
	if heartbeat > 0 {
		s.sseHeartbeatInterval = heartbeat
	}
	if writeTimeout > 0 {
		s.sseWriteTimeout = writeTimeout
	}
}

func (s *WebServer) getSSEIntervals() (time.Duration, time.Duration, time.Duration) {
	s.sessionsMu.RLock()
	defer s.sessionsMu.RUnlock()
	up := s.sseUpdateInterval
	if up <= 0 {
		up = 1 * time.Second
	}
	hb := s.sseHeartbeatInterval
	if hb <= 0 {
		hb = 15 * time.Second
	}
	wt := s.sseWriteTimeout
	if wt <= 0 {
		wt = 5 * time.Second
	}
	return up, hb, wt
}

func (s *WebServer) isSessionValid(r *http.Request) bool {
	if s.password == "" {
		return true
	}
	cookie, err := r.Cookie("tg_downloader_session")
	if err != nil || cookie.Value == "" {
		return false
	}
	s.sessionsMu.RLock()
	expireAt, ok := s.sessions[cookie.Value]
	s.sessionsMu.RUnlock()
	return ok && time.Now().Before(expireAt)
}

func NewWebServer(
	db *Database,
	transferMgr *transfer.TransferManager,
	ssdAdmission *fscommit.SSDAdmission,
	proxyManager *ProxyManager,
	orchestrator *Orchestrator,
	access TelegramAccess,
	registry *Registry,
	logger *zap.Logger,
	password string,
) *WebServer {
	return &WebServer{
		db:                   db,
		transferMgr:          transferMgr,
		ssdAdmission:         ssdAdmission,
		proxyManager:         proxyManager,
		orchestrator:         orchestrator,
		access:               access,
		registry:             registry,
		logger:               logger,
		password:             password,
		sessions:             make(map[string]time.Time),
		sseUpdateInterval:    1 * time.Second,
		sseHeartbeatInterval: 15 * time.Second,
		sseWriteTimeout:      5 * time.Second,
	}
}

func (s *WebServer) Handler() http.Handler {
	r := mux.NewRouter()

	// Static assets
	staticSubFS, err := fs.Sub(uiFS, "ui/static")
	if err == nil {
		r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.FS(staticSubFS))))
	}

	// Healthz (CGroup/Docker container healthcheck)
	r.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	}).Methods(http.MethodGet, http.MethodHead)

	// Daemon Status & Tasks
	r.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.registry.Status())
	}).Methods(http.MethodGet)

	// Gate Diagnostics (live adaptive controller state)
	r.HandleFunc("/api/gate", func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{}
		if s.transferMgr != nil {
			g := s.transferMgr.Gate()
			if g != nil {
				resp["max_data_in_flight"] = g.Max()
				resp["data_in_flight"] = g.InFlight()
			}
			resp["active_files"] = s.transferMgr.ActiveFiles()
			resp["file_concurrency"] = s.transferMgr.FileConcurrency()
		}
		if s.ssdAdmission != nil {
			resp["ssd_reserved_bytes"] = s.ssdAdmission.ReservedBytes()
			resp["ssd_available_bytes"] = s.ssdAdmission.AvailableBytes()
		}
		writeJSON(w, http.StatusOK, resp)
	}).Methods(http.MethodGet)

	r.HandleFunc("/api/system/storage", func(w http.ResponseWriter, r *http.Request) {
		path := "."
		if s.orchestrator != nil {
			path = s.orchestrator.OutputDir()
		}
		freeBytes, totalBytes, err := fscommit.GetDiskSpace(path)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		usedBytes := uint64(0)
		if totalBytes > freeBytes {
			usedBytes = totalBytes - freeBytes
		}
		percent := 0.0
		if totalBytes > 0 {
			percent = float64(usedBytes) / float64(totalBytes) * 100
		}
		resp := map[string]any{
			"ok":           true,
			"path":         path,
			"free_bytes":   freeBytes,
			"total_bytes":  totalBytes,
			"used_bytes":   usedBytes,
			"free_human":   formatBytes(int64(freeBytes)),
			"total_human":  formatBytes(int64(totalBytes)),
			"used_human":   formatBytes(int64(usedBytes)),
			"percent_used": fmt.Sprintf("%.1f%%", percent),
		}
		if s.db != nil {
			if arcStats, err := s.db.GetArchiveStats(); err == nil {
				resp["archive"] = arcStats
			}
		}
		writeJSON(w, http.StatusOK, resp)
	}).Methods(http.MethodGet)

	r.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			tasks := s.registry.Tasks()
			writeJSON(w, http.StatusOK, tasks)
			return
		}
		var req TaskRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}
		snap, created, err := s.registry.Submit(req)
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
		if s.db != nil {
			_ = s.db.EnsureDownloadRecord(req.Peer, req.MessageID, req.FinalPath, req.ExpectedSize)
		}
		status := http.StatusOK
		if created {
			status = http.StatusAccepted
		}
		writeJSON(w, status, snap)
	}).Methods(http.MethodGet, http.MethodPost)

	r.HandleFunc("/api/tasks/{id}", func(w http.ResponseWriter, r *http.Request) {
		task, ok := s.registry.Task(mux.Vars(r)["id"])
		if !ok {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		writeJSON(w, http.StatusOK, task)
	}).Methods(http.MethodGet)

	r.HandleFunc("/api/chat/history", func(w http.ResponseWriter, r *http.Request) {
		if s.access == nil {
			writeError(w, http.StatusServiceUnavailable, "telegram access not available")
			return
		}
		var req HistoryRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}
		messages, err := s.access.GetHistory(r.Context(), req)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":       true,
			"messages": messages,
		})
	}).Methods(http.MethodPost)

	r.HandleFunc("/api/chat/resolve", func(w http.ResponseWriter, r *http.Request) {
		if s.access == nil {
			writeError(w, http.StatusServiceUnavailable, "telegram access not available")
			return
		}
		var req ResolveRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}
		dialog, err := s.access.ResolvePeerInfo(r.Context(), req.Query)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":   true,
			"peer": dialog,
		})
	}).Methods(http.MethodPost)

	r.HandleFunc("/api/control", s.requireAuth(s.handleControl)).Methods(http.MethodPost)

	// Login & Auth (Web UI Password)
	r.HandleFunc("/login", s.handleLogin)
	r.HandleFunc("/logout", s.handleLogout)

	// Telegram Account Auth Wizard & Multi-Account Management
	r.HandleFunc("/api/auth/status", s.requireAuth(s.handleAuthStatus)).Methods(http.MethodGet)
	r.HandleFunc("/api/auth/qr/start", s.requireAuth(s.handleAuthQRStart)).Methods(http.MethodPost)
	r.HandleFunc("/api/auth/qr/poll", s.requireAuth(s.handleAuthQRPoll)).Methods(http.MethodGet)
	r.HandleFunc("/api/auth/phone/send_code", s.requireAuth(s.handleAuthSendPhoneCode)).Methods(http.MethodPost)
	r.HandleFunc("/api/auth/phone/verify_code", s.requireAuth(s.handleAuthVerifyPhoneCode)).Methods(http.MethodPost)
	r.HandleFunc("/api/auth/2fa/verify", s.requireAuth(s.handleAuthVerify2FA)).Methods(http.MethodPost)
	r.HandleFunc("/api/auth/logout", s.requireAuth(s.handleAuthLogoutTelegram)).Methods(http.MethodPost)
	r.HandleFunc("/api/accounts", s.requireAuth(s.handleGetAccounts)).Methods(http.MethodGet)
	r.HandleFunc("/api/accounts/switch", s.requireAuth(s.handleSwitchAccount)).Methods(http.MethodPost)
	r.HandleFunc("/api/accounts/delete", s.requireAuth(s.handleDeleteAccount)).Methods(http.MethodPost)

	// Web Dashboard
	r.HandleFunc("/", s.requireAuth(s.handleIndex)).Methods(http.MethodGet, http.MethodHead)
	r.HandleFunc("/get_app_version", s.handleGetAppVersion).Methods(http.MethodGet)

	// Download Control & Status
	r.HandleFunc("/get_download_status", s.requireAuth(s.handleGetDownloadStatus)).Methods(http.MethodGet)
	r.HandleFunc("/get_download_list", s.requireAuth(s.handleGetDownloadList)).Methods(http.MethodGet)
	r.HandleFunc("/api/downloaded_records", s.requireAuth(s.handleGetDownloadedRecords)).Methods(http.MethodGet)
	r.HandleFunc("/api/events", s.requireAuth(s.handleEventsStream)).Methods(http.MethodGet)

	// Targets & Dialogs
	r.HandleFunc("/api/listen_targets", s.requireAuth(s.handleListenTargets))
	r.HandleFunc("/api/target/update", s.requireAuth(s.handleUpdateSingleTarget)).Methods(http.MethodPost)
	r.HandleFunc("/api/dialogs", s.requireAuth(s.handleDialogs)).Methods(http.MethodGet)
	r.HandleFunc("/api/resolve_target", s.requireAuth(s.handleResolveTarget)).Methods(http.MethodPost)
	r.HandleFunc("/api/add_target", s.requireAuth(s.handleAddTarget)).Methods(http.MethodPost)
	r.HandleFunc("/api/target_progress", s.requireAuth(s.handleTargetProgress)).Methods(http.MethodGet)
	r.HandleFunc("/api/chat_context", s.requireAuth(s.handleChatContext)).Methods(http.MethodGet)

	// Settings & Concurrency
	r.HandleFunc("/api/settings/concurrency", s.requireAuth(s.handleConcurrencySettings))

	// Proxy
	r.HandleFunc("/api/proxy/list", s.requireAuth(s.handleProxyList)).Methods(http.MethodGet)
	r.HandleFunc("/api/proxy/switch", s.requireAuth(s.handleProxySwitch)).Methods(http.MethodPost)
	r.HandleFunc("/api/proxy/ping", s.requireAuth(s.handleProxyPing)).Methods(http.MethodPost)

	return r
}

func (s *WebServer) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.password == "" {
			next(w, r)
			return
		}

		cookie, err := r.Cookie("tg_downloader_session")
		if err != nil || cookie.Value == "" {
			if strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/get_") || strings.HasPrefix(r.URL.Path, "/download_") {
				http.Error(w, `{"ok":false,"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}

		s.sessionsMu.RLock()
		expireAt, ok := s.sessions[cookie.Value]
		s.sessionsMu.RUnlock()

		if !ok || time.Now().After(expireAt) {
			if strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/get_") || strings.HasPrefix(r.URL.Path, "/set_") || strings.HasPrefix(r.URL.Path, "/download_") {
				http.Error(w, `{"ok":false,"error":"session expired"}`, http.StatusUnauthorized)
				return
			}
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}

		next(w, r)
	}
}

func (s *WebServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	data, err := uiFS.ReadFile("ui/templates/index.html")
	if err != nil {
		http.Error(w, "index.html not found", http.StatusNotFound)
		return
	}

	state := "running"
	if s.orchestrator != nil && !s.orchestrator.IsRunning() {
		state = "paused"
	}

	ver := consts.EffectiveVersion()

	content := strings.ReplaceAll(string(data), "{{ download_state }}", state)
	content = strings.ReplaceAll(content, "{{ app_version }}", ver)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(content))
}

func (s *WebServer) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		data, err := uiFS.ReadFile("ui/templates/login.html")
		if err != nil {
			http.Error(w, "login.html not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
		return
	}

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":    false,
			"code":  0,
			"error": "invalid json",
			"msg":   "invalid json",
		})
		return
	}

	if s.password != "" && req.Password != s.password {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":    false,
			"code":  0,
			"error": "password error",
			"msg":   "password error",
		})
		return
	}

	token := fmt.Sprintf("sess_%d_%s", time.Now().UnixNano(), "token")
	s.sessionsMu.Lock()
	s.sessions[token] = time.Now().Add(7 * 24 * time.Hour)
	s.sessionsMu.Unlock()

	isSecure := r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"
	http.SetCookie(w, &http.Cookie{
		Name:     "tg_downloader_session",
		Value:    token,
		Path:     "/",
		Expires:  time.Now().Add(7 * 24 * time.Hour),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   isSecure,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":   true,
		"code": 1,
		"msg":  "success",
	})
}

func (s *WebServer) handleLogout(w http.ResponseWriter, r *http.Request) {
	isSecure := r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"
	http.SetCookie(w, &http.Cookie{
		Name:     "tg_downloader_session",
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(0, 0),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   isSecure,
	})
	http.Redirect(w, r, "login", http.StatusFound)
}

func (s *WebServer) handleGetAppVersion(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	ver := consts.EffectiveVersion()
	_, _ = w.Write([]byte(ver))
}

func (s *WebServer) handleControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}

	var req struct {
		Action        string `json:"action"`
		State         string `json:"state"`
		DownloadState string `json:"download_state"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	desired := strings.ToLower(strings.TrimSpace(req.Action))
	if desired == "" {
		st := strings.ToLower(strings.TrimSpace(req.State))
		if st == "" {
			st = strings.ToLower(strings.TrimSpace(req.DownloadState))
		}
		if st == "paused" {
			desired = "pause"
		} else if st == "running" {
			desired = "resume"
		}
	}

	switch desired {
	case "pause":
		if s.orchestrator != nil {
			s.orchestrator.SetRunning(false)
		}
		if s.registry != nil {
			s.registry.SetPaused(true)
		}
	case "resume":
		if s.orchestrator != nil {
			s.orchestrator.SetRunning(true)
		}
		if s.registry != nil {
			s.registry.SetPaused(false)
		}
	default:
		writeError(w, http.StatusBadRequest, "action must be pause or resume")
		return
	}

	state := "running"
	isPaused := false
	if (s.orchestrator != nil && !s.orchestrator.IsRunning()) || (s.registry != nil && s.registry.Status().Paused) {
		state = "paused"
		isPaused = true
	}

	resp := map[string]any{
		"ok":             true,
		"state":          state,
		"download_state": state,
		"paused":         isPaused,
	}
	if s.registry != nil {
		resp["status"] = s.registry.Status()
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *WebServer) collectTelemetrySnapshot() map[string]any {
	state := "running"
	isPaused := false
	if (s.orchestrator != nil && !s.orchestrator.IsRunning()) || (s.registry != nil && s.registry.Status().Paused) {
		state = "paused"
		isPaused = true
	}

	snap := map[string]any{
		"timestamp":      time.Now().UnixMilli(),
		"state":          state,
		"download_state": state,
		"paused":         isPaused,
	}

	if s.registry != nil {
		st := s.registry.Status()
		snap["speed_bps"] = st.Rolling5sBPS
		snap["speed_human"] = formatBytes(st.Rolling5sBPS) + "/s"
		snap["queue_depth"] = st.QueueDepth
		snap["queue_size"] = st.QueueDepth
		snap["active_files"] = st.ActiveFiles
		snap["last_error"] = st.LastError
	}

	gateInfo := map[string]any{
		"max_data_in_flight": int64(256 * 1024 * 1024),
		"data_in_flight":     int64(0),
		"active_files":       int64(0),
		"file_concurrency":   5,
	}
	if s.transferMgr != nil {
		gateInfo["active_files"] = s.transferMgr.ActiveFiles()
		gateInfo["file_concurrency"] = s.transferMgr.FileConcurrency()
		if g := s.transferMgr.Gate(); g != nil {
			gateInfo["max_data_in_flight"] = g.Max()
			gateInfo["data_in_flight"] = g.InFlight()
		}
	}
	snap["gate"] = gateInfo

	ssdInfo := map[string]any{
		"ssd_reserved_bytes":  int64(0),
		"ssd_available_bytes": int64(0),
		"reserved_bytes":      int64(0),
		"available_bytes":     int64(0),
	}
	if s.ssdAdmission != nil {
		ssdInfo["ssd_reserved_bytes"] = s.ssdAdmission.ReservedBytes()
		ssdInfo["ssd_available_bytes"] = s.ssdAdmission.AvailableBytes()
		ssdInfo["reserved_bytes"] = s.ssdAdmission.ReservedBytes()
		ssdInfo["available_bytes"] = s.ssdAdmission.AvailableBytes()
	}
	snap["ssd"] = ssdInfo

	if s.db != nil {
		if arcStats, err := s.db.GetArchiveStats(); err == nil {
			snap["archive"] = arcStats
		}

		if targets, err := s.db.GetListenTargets(); err == nil && len(targets) > 0 {
			stats, _ := s.db.GetTargetProgressStats()
			progList := make([]TargetProgressItemDTO, 0, len(targets))
			for _, t := range targets {
				cursor, _ := s.db.GetScanCursor(t.ChatID)
				st := stats[t.ChatID]
				progList = append(progList, TargetProgressItemDTO{
					ChatID:            t.ChatID,
					Title:             t.Title,
					Enabled:           t.Enabled,
					LastReadMessageID: cursor,
					ScanStatus:        "ok",
					TotalFiles:        st.TotalFiles,
					DownloadedFiles:   st.DownloadedFiles,
					PendingFiles:      st.PendingFiles,
					ProcessingFiles:   st.ProcessingFiles,
					FailedFiles:       st.FailedFiles,
					SkippedFiles:      st.SkippedFiles,
					DownloadedBytes:   st.DownloadedBytes,
				})
			}
			snap["target_progress"] = progList
		}
	}

	path := "."
	if s.orchestrator != nil && s.orchestrator.OutputDir() != "" {
		path = s.orchestrator.OutputDir()
	}
	if freeBytes, totalBytes, err := fscommit.GetDiskSpace(path); err == nil {
		usedBytes := uint64(0)
		if totalBytes > freeBytes {
			usedBytes = totalBytes - freeBytes
		}
		percent := 0.0
		if totalBytes > 0 {
			percent = float64(usedBytes) / float64(totalBytes) * 100
		}
		snap["storage"] = map[string]any{
			"free_bytes":   freeBytes,
			"total_bytes":  totalBytes,
			"used_bytes":   usedBytes,
			"free_human":   formatBytes(int64(freeBytes)),
			"total_human":  formatBytes(int64(totalBytes)),
			"used_human":   formatBytes(int64(usedBytes)),
			"percent_used": fmt.Sprintf("%.1f%%", percent),
			"path":         path,
		}
	}

	return snap
}

func (s *WebServer) handleEventsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	rc := http.NewResponseController(w)
	updateInt, heartbeatInt, writeTimeout := s.getSSEIntervals()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	if !s.isSessionValid(r) {
		_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
		fmt.Fprintf(w, "event: error\ndata: {\"error\":\"session expired\"}\n\n")
		flusher.Flush()
		return
	}

	initial := s.collectTelemetrySnapshot()
	if data, err := json.Marshal(initial); err == nil {
		_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
		if _, err := fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", string(data)); err != nil {
			return
		}
		flusher.Flush()
	}

	updateTicker := time.NewTicker(updateInt)
	defer updateTicker.Stop()

	heartbeatTicker := time.NewTicker(heartbeatInt)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeatTicker.C:
			if !s.isSessionValid(r) {
				_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
				fmt.Fprintf(w, "event: error\ndata: {\"error\":\"session expired\"}\n\n")
				flusher.Flush()
				return
			}
			_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
			if _, err := fmt.Fprintf(w, "event: heartbeat\ndata: {}\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-updateTicker.C:
			if !s.isSessionValid(r) {
				_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
				fmt.Fprintf(w, "event: error\ndata: {\"error\":\"session expired\"}\n\n")
				flusher.Flush()
				return
			}
			snap := s.collectTelemetrySnapshot()
			if data, err := json.Marshal(snap); err == nil {
				_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
				if _, err := fmt.Fprintf(w, "event: update\ndata: %s\n\n", string(data)); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}

func (s *WebServer) handleGetDownloadStatus(w http.ResponseWriter, r *http.Request) {
	daemonStatus := s.registry.Status()

	speedBytes := daemonStatus.Rolling5sBPS
	speedStr := formatBytes(speedBytes) + "/s"

	filesMap := make(map[string]any)
	for _, f := range daemonStatus.ActiveFiles {
		filesMap[f.ID] = map[string]any{
			"id":             f.ID,
			"final_path":     f.FinalPath,
			"total_size":     f.TotalSize,
			"downloaded":     f.Downloaded,
			"progress":       f.Progress,
			"rolling_5s_bps": f.Rolling5sBPS,
			"state":          f.State,
		}
	}

	state := "running"
	if s.orchestrator != nil && !s.orchestrator.IsRunning() {
		state = "paused"
	}

	var activeFiles int64
	var fileConcurrency int
	var dataInFlight int64
	var maxInFlight int64
	if s.transferMgr != nil {
		activeFiles = s.transferMgr.ActiveFiles()
		fileConcurrency = s.transferMgr.FileConcurrency()
		if s.transferMgr.Gate() != nil {
			dataInFlight = s.transferMgr.Gate().InFlight()
			maxInFlight = s.transferMgr.Gate().Max()
		}
	}

	slotUtil := 0.0
	if fileConcurrency > 0 {
		slotUtil = float64(activeFiles) / float64(fileConcurrency) * 100
	}

	var reservedBytes, availableBytes int64
	if s.ssdAdmission != nil {
		reservedBytes = s.ssdAdmission.ReservedBytes()
		availableBytes = s.ssdAdmission.AvailableBytes()
	}

	resp := map[string]any{
		"download_speed": speedStr,
		"speed_bps":      speedBytes,
		"download_state": state,
		"slot_pool": map[string]any{
			"total_slots":          fileConcurrency,
			"used_slots":           activeFiles,
			"available_slots":      fileConcurrency - int(activeFiles),
			"max_active_files":     fileConcurrency,
			"active_files_count":   activeFiles,
			"slot_unit_mb":         1,
			"max_slots_per_file":   1,
			"slot_utilization_pct": slotUtil,
			"utilization_pct":      slotUtil,
			"file_utilization_pct": slotUtil,
		},
		"transfer_manager": map[string]any{
			"active_files":     activeFiles,
			"file_concurrency": fileConcurrency,
			"data_in_flight":   dataInFlight,
			"max_in_flight":    maxInFlight,
		},
		"ssd_admission": map[string]any{
			"reserved_bytes":  reservedBytes,
			"available_bytes": availableBytes,
		},
		"media_pool": map[string]any{
			"active_files": len(filesMap),
			"files":        filesMap,
		},
		"total_download_task": 0,
		"total_download_byte": formatBytes(0),
		"finish_task_byte":    formatBytes(0),
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *WebServer) handleGetDownloadList(w http.ResponseWriter, r *http.Request) {
	daemonStatus := s.registry.Status()

	targets, _ := s.db.GetListenTargets()
	targetMap := make(map[string]string)
	for _, t := range targets {
		if t.Title != "" {
			targetMap[t.ChatID] = t.Title
			if t.Username != "" {
				targetMap["@"+t.Username] = t.Title
			}
		}
	}

	list := make([]map[string]any, 0)
	for _, f := range daemonStatus.ActiveFiles {
		parts := strings.SplitN(f.ID, ":", 2)
		chatID := f.Peer
		msgID := f.MessageID
		if len(parts) == 2 {
			chatID = parts[0]
			if m, err := strconv.Atoi(parts[1]); err == nil {
				msgID = m
			}
		}

		chatDisplay := chatID
		if title, ok := targetMap[chatID]; ok && title != "" {
			chatDisplay = title
		}

		fileName := f.FileName
		if fileName == "" || strings.HasSuffix(fileName, ".unknown") {
			base := filepath.Base(f.FinalPath)
			if idx := strings.Index(base, " - "); idx != -1 && len(base) > idx+3 {
				fileName = base[idx+3:]
			} else {
				fileName = base
			}
		}
		if fileName == "" || fileName == "." {
			fileName = fmt.Sprintf("%d.bin", msgID)
		}

		speedStr := formatBytes(f.Rolling5sBPS) + "/s"
		list = append(list, map[string]any{
			"chat":              chatDisplay,
			"chat_id":           chatID,
			"id":                msgID,
			"filename":          fileName,
			"file_name":         fileName,
			"final_path":        f.FinalPath,
			"total_size":        formatBytes(f.TotalSize),
			"download_progress": fmt.Sprintf("%.1f", f.Progress),
			"download_speed":    speedStr,
			"status":            "Downloading",
		})
	}

	// Frontend expects a bare array of active tasks
	writeJSON(w, http.StatusOK, list)
}

func (s *WebServer) handleGetDownloadedRecords(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 500 {
		limit = 50
	}
	offset := (page - 1) * limit
	query := strings.TrimSpace(r.URL.Query().Get("q"))

	targets, _ := s.db.GetListenTargets()
	targetMap := make(map[string]string)
	for _, t := range targets {
		if t.Title != "" {
			targetMap[t.ChatID] = t.Title
			if t.Username != "" {
				targetMap["@"+t.Username] = t.Title
			}
		}
	}

	records, total, err := s.db.GetDownloadedRecords(query, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	list := make([]map[string]any, 0, len(records))
	for _, rec := range records {
		chatDisplay := rec.ChatID
		if title, ok := targetMap[rec.ChatID]; ok && title != "" {
			chatDisplay = title
		}

		fileName := rec.FileName
		if fileName == "" && rec.SavePath != "" {
			fileName = filepath.Base(rec.SavePath)
		}
		if fileName == "" {
			fileName = fmt.Sprintf("media_%d", rec.MessageID)
		}

		dlAt := rec.DownloadedAt
		if dlAt == 0 {
			dlAt = rec.UpdatedAt
		}

		list = append(list, map[string]any{
			"chat":          chatDisplay,
			"chat_id":       rec.ChatID,
			"id":            rec.MessageID,
			"message_id":    rec.MessageID,
			"filename":      fileName,
			"file_name":     fileName,
			"save_path":     rec.SavePath,
			"media_type":    rec.MediaType,
			"total_size":    formatBytes(rec.FileSize),
			"file_size":     formatBytes(rec.FileSize),
			"completed_at":  dlAt,
			"downloaded_at": time.Unix(dlAt, 0).Format("2006-01-02 15:04:05"),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"code":  0,
		"msg":   "",
		"count": total,
		"page":  page,
		"limit": limit,
		"data":  list,
	})
}

func (s *WebServer) handleListenTargets(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		targets, err := s.db.GetListenTargets()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":      true,
			"code":    0,
			"msg":     "",
			"count":   len(targets),
			"targets": targets,
		})
		return
	}

	// POST /api/listen_targets (handles both { "targets": [...] } and [...])
	var bodyRaw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&bodyRaw); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	var targets []ListenTarget
	var wrapper struct {
		Targets []ListenTarget `json:"targets"`
	}

	if err := json.Unmarshal(bodyRaw, &wrapper); err == nil && wrapper.Targets != nil {
		targets = wrapper.Targets
	} else if err := json.Unmarshal(bodyRaw, &targets); err != nil {
		writeError(w, http.StatusBadRequest, "invalid targets payload")
		return
	}

	if err := s.db.SaveListenTargets(targets); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	for _, t := range targets {
		if !t.Enabled && s.orchestrator != nil {
			s.orchestrator.CancelTasksByChatID(t.ChatID)
		}
	}

	globalUpdatesStreamMu.RLock()
	stream := globalUpdatesStream
	globalUpdatesStreamMu.RUnlock()
	if stream != nil {
		stream.refreshTargetCache()
	}

	saved, _ := s.db.GetListenTargets()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"code":    0,
		"msg":     "success",
		"targets": saved,
	})
}

func (s *WebServer) handleUpdateSingleTarget(w http.ResponseWriter, r *http.Request) {
	var item UpdateSingleTargetRequestDTO
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if item.ChatID == "" {
		writeError(w, http.StatusBadRequest, "chat_id is required")
		return
	}

	canonicalChatID := item.ChatID
	if strings.HasPrefix(item.ChatID, "@") && s.access != nil {
		info, err := s.access.ResolvePeerInfo(r.Context(), item.ChatID)
		if err == nil && info.ChatID != "" {
			canonicalChatID = info.ChatID
			if item.Title == "" {
				item.Title = info.Title
			}
			if item.Username == "" {
				item.Username = info.Username
			}
			if item.Type == "" {
				item.Type = info.Type
			}
		}
	}

	var targets []ListenTarget
	if s.db != nil {
		targets, _ = s.db.GetListenTargets()
	}
	var target ListenTarget
	found := false
	for _, t := range targets {
		if t.ChatID == canonicalChatID {
			target = t
			found = true
			break
		}
	}
	if !found {
		target = ListenTarget{
			ChatID:   canonicalChatID,
			Title:    item.Title,
			Username: item.Username,
			ChatType: item.Type,
		}
	}
	if item.Enabled != nil {
		target.Enabled = *item.Enabled
	}
	if item.Priority != nil {
		target.Priority = *item.Priority
	}
	if item.LastReadMessageID != nil {
		target.LastReadMessageID = *item.LastReadMessageID
	}
	if item.DownloadFilter != nil {
		target.DownloadFilter = *item.DownloadFilter
	}
	if item.UploadTelegramChatID != nil {
		target.UploadTelegramChatID = *item.UploadTelegramChatID
	}
	if item.Title != "" {
		target.Title = item.Title
	}
	if item.Username != "" {
		target.Username = item.Username
	}

	if s.db != nil {
		if err := s.db.SaveSingleListenTarget(target); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	if !target.Enabled && s.orchestrator != nil {
		s.orchestrator.CancelTasksByChatID(target.ChatID)
	}

	globalUpdatesStreamMu.RLock()
	stream := globalUpdatesStream
	globalUpdatesStreamMu.RUnlock()
	if stream != nil {
		stream.refreshTargetCache()
	}

	writeJSON(w, http.StatusOK, UpdateSingleTargetResponseDTO{
		OK:     true,
		Target: target,
	})
}

func (s *WebServer) handleDialogs(w http.ResponseWriter, r *http.Request) {
	refresh := r.URL.Query().Get("refresh") == "true"

	var targets []ListenTarget
	var err error
	if s.db != nil {
		targets, err = s.db.GetListenTargets()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	targetMap := make(map[string]ListenTarget)
	for _, t := range targets {
		targetMap[t.ChatID] = t
	}

	var rawDialogs []DialogDTO
	rawMap := make(map[string]DialogDTO)
	if s.access != nil && (refresh || len(targets) == 0) {
		rawDialogs, err = s.access.GetDialogs(r.Context())
		if err != nil && refresh {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		for _, d := range rawDialogs {
			rawMap[d.ChatID] = d
		}
	}

	decorated := make([]TargetDialogDTO, 0, len(targets)+len(rawDialogs))
	seenIDs := make(map[string]bool)

	// 1. First append all configured business targets from database
	for _, t := range targets {
		seenIDs[t.ChatID] = true
		var cursor int
		if s.db != nil {
			cursor, _, _ = s.db.GetScanCursorWithTime(t.ChatID)
		}
		var lastMsgDate int64
		if s.db != nil {
			_ = s.db.DB().QueryRow(`SELECT COALESCE(MAX(date), 0) FROM chat_messages WHERE chat_id = ?`, t.ChatID).Scan(&lastMsgDate)
		}
		if lastMsgDate == 0 {
			lastMsgDate = t.UpdatedAt
		}

		topMsgID := 0
		if d, ok := rawMap[t.ChatID]; ok && d.TopMessageID > 0 {
			topMsgID = d.TopMessageID
		} else if s.db != nil {
			_ = s.db.DB().QueryRow(`SELECT COALESCE(MAX(message_id), 0) FROM chat_messages WHERE chat_id = ?`, t.ChatID).Scan(&topMsgID)
		}
		if topMsgID == 0 {
			if cursor > 0 {
				topMsgID = cursor
			} else if t.LastReadMessageID > 0 {
				topMsgID = t.LastReadMessageID
			}
		}

		decorated = append(decorated, TargetDialogDTO{
			ID:                   t.ChatID,
			ChatID:               t.ChatID,
			Title:                t.Title,
			Username:             t.Username,
			Type:                 t.ChatType,
			Pinned:               false,
			UnreadCount:          0,
			TopMessageID:         topMsgID,
			LastReadMessageID:    cursor,
			IsTarget:             t.Enabled,
			Enabled:              t.Enabled,
			TargetEnabled:        t.Enabled,
			Priority:             t.Priority,
			DownloadFilter:       t.DownloadFilter,
			UploadTelegramChatID: t.UploadTelegramChatID,
			LastMessageAt:        lastMsgDate,
			LastScanFinishedAt:   lastMsgDate,
			LastScanStartedAt:    lastMsgDate,
			UpdatedAt:            t.UpdatedAt,
		})
	}

	// 2. Append newly discovered unconfigured dialogs as read-only view items (without touching DB)
	for _, d := range rawDialogs {
		if seenIDs[d.ChatID] {
			continue
		}
		seenIDs[d.ChatID] = true
		decorated = append(decorated, TargetDialogDTO{
			ID:                   d.ChatID,
			ChatID:               d.ChatID,
			Title:                d.Title,
			Username:             d.Username,
			Type:                 d.Type,
			Pinned:               d.Pinned,
			UnreadCount:          d.UnreadCount,
			TopMessageID:         d.TopMessageID,
			LastReadMessageID:    0,
			IsTarget:             false,
			Enabled:              false,
			TargetEnabled:        false,
			Priority:             0,
			DownloadFilter:       "",
			UploadTelegramChatID: "",
			LastMessageAt:        0,
			LastScanFinishedAt:   0,
			LastScanStartedAt:    0,
			UpdatedAt:            0,
		})
	}

	writeJSON(w, http.StatusOK, DialogsResponseDTO{
		OK:      true,
		Dialogs: decorated,
	})
}

func (s *WebServer) handleResolveTarget(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query  string `json:"query"`
		Target string `json:"target"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	query := req.Query
	if query == "" {
		query = req.Target
	}

	if s.access == nil {
		writeError(w, http.StatusServiceUnavailable, "telegram access not available")
		return
	}

	info, err := s.access.ResolvePeerInfo(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":   true,
		"peer": info,
		"dialog": map[string]any{
			"id":             info.ID,
			"chat_id":        info.ChatID,
			"title":          info.Title,
			"username":       info.Username,
			"type":           info.Type,
			"top_message_id": info.TopMessageID,
		},
	})
}

func (s *WebServer) handleAddTarget(w http.ResponseWriter, r *http.Request) {
	var req AddTargetRequestDTO
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	query := req.Query
	if query == "" {
		query = req.Target
	}
	if query == "" {
		query = req.ChatID
	}

	if s.access == nil {
		writeError(w, http.StatusServiceUnavailable, "telegram access not available")
		return
	}

	info, err := s.access.ResolvePeerInfo(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	targetItem := ListenTarget{
		ChatID:            info.ChatID,
		Title:             info.Title,
		Username:          info.Username,
		ChatType:          info.Type,
		Enabled:           true,
		Priority:          0,
		LastReadMessageID: info.TopMessageID,
	}
	if s.db != nil {
		if err := s.db.SaveSingleListenTarget(targetItem); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to save listen target: "+err.Error())
			return
		}
	}

	globalUpdatesStreamMu.RLock()
	stream := globalUpdatesStream
	globalUpdatesStreamMu.RUnlock()
	if stream != nil {
		stream.refreshTargetCache()
	}

	dialogObj := TargetDialogDTO{
		ID:                   info.ChatID,
		ChatID:               info.ChatID,
		Title:                info.Title,
		Username:             info.Username,
		Type:                 info.Type,
		TopMessageID:         info.TopMessageID,
		LastReadMessageID:    info.TopMessageID,
		Enabled:              true,
		TargetEnabled:        true,
		IsTarget:             true,
		Priority:             0,
		DownloadFilter:       "",
		UploadTelegramChatID: "",
		UpdatedAt:            time.Now().Unix(),
	}

	writeJSON(w, http.StatusOK, AddTargetResponseDTO{
		OK:     true,
		Peer:   info,
		Dialog: dialogObj,
		Target: dialogObj,
	})
}

func (s *WebServer) handleTargetProgress(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database not available")
		return
	}

	targets, err := s.db.GetListenTargets()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	stats, statsErr := s.db.GetTargetProgressStats()
	if statsErr != nil {
		writeError(w, http.StatusInternalServerError, "failed to get target progress stats: "+statsErr.Error())
		return
	}

	progressList := make([]TargetProgressItemDTO, 0, len(targets))
	for _, t := range targets {
		cursor, cursorErr := s.db.GetScanCursor(t.ChatID)
		scanStatus := "ok"
		scanErrStr := ""
		if cursorErr != nil {
			scanStatus = "error"
			scanErrStr = cursorErr.Error()
		}
		st := stats[t.ChatID]

		progressList = append(progressList, TargetProgressItemDTO{
			ChatID:            t.ChatID,
			Title:             t.Title,
			Enabled:           t.Enabled,
			LastReadMessageID: cursor,
			ScanStatus:        scanStatus,
			ScanError:         scanErrStr,
			TotalFiles:        st.TotalFiles,
			DownloadedFiles:   st.DownloadedFiles,
			PendingFiles:      st.PendingFiles,
			ProcessingFiles:   st.ProcessingFiles,
			FailedFiles:       st.FailedFiles,
			SkippedFiles:      st.SkippedFiles,
			DownloadedBytes:   st.DownloadedBytes,
		})
	}

	writeJSON(w, http.StatusOK, TargetProgressResponseDTO{
		OK:       true,
		Progress: progressList,
	})
}

func (s *WebServer) handleChatContext(w http.ResponseWriter, r *http.Request) {
	chatID := strings.TrimSpace(r.URL.Query().Get("chat_id"))
	messageIDStr := strings.TrimSpace(r.URL.Query().Get("message_id"))
	if chatID == "" || messageIDStr == "" {
		writeError(w, http.StatusBadRequest, "chat_id and message_id are required")
		return
	}

	targetMid, err := strconv.Atoi(messageIDStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "message_id must be an integer")
		return
	}

	limit := 30
	if limitStr := strings.TrimSpace(r.URL.Query().Get("limit")); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}
	if limit > 100 {
		limit = 100
	}
	if limit < 1 {
		limit = 1
	}

	limitBefore := limit / 2
	limitAfter := limit - limitBefore

	if lb, err := strconv.Atoi(r.URL.Query().Get("limit_before")); err == nil && lb > 0 {
		limitBefore = lb
	}
	if la, err := strconv.Atoi(r.URL.Query().Get("limit_after")); err == nil && la > 0 {
		limitAfter = la
	}

	// 1. First fetch from local SQLite
	var msgs []ChatMessage
	if s.db != nil {
		msgs, err = s.db.GetChatMessagesAround(chatID, targetMid, limitBefore, limitAfter)
	}
	if (err != nil || len(msgs) == 0) && s.access != nil {
		// 2. Fallback: fetch from Telegram MTProto
		req := HistoryRequest{
			Peer:     chatID,
			OffsetID: targetMid + limitAfter,
			Limit:    limitBefore + limitAfter,
			Reverse:  true,
		}
		if tgMsgs, tgErr := s.access.GetHistory(r.Context(), req); tgErr == nil && len(tgMsgs) > 0 {
			if s.db != nil {
				for _, tm := range tgMsgs {
					_ = s.db.IngestMessage(ChatMessage{
						ChatID:           tm.ChatID,
						MessageID:        tm.ID,
						SenderID:         tm.SenderID,
						SenderName:       tm.SenderName,
						Text:             tm.Text,
						MediaType:        tm.MediaType,
						HasMedia:         tm.HasMedia,
						ReplyToMessageID: tm.ReplyToMessageID,
						Date:             tm.Date,
						FileName:         tm.FileName,
						FileSize:         tm.FileSize,
					})
				}
				msgs, _ = s.db.GetChatMessagesAround(chatID, targetMid, limitBefore, limitAfter)
			}
		}
	}

	msgList := make([]ChatContextMessageDTO, 0, len(msgs))
	for _, m := range msgs {
		msgList = append(msgList, ChatContextMessageDTO{
			ChatID:           m.ChatID,
			MessageID:        m.MessageID,
			SenderID:         m.SenderID,
			SenderName:       m.SenderName,
			Text:             m.Text,
			MediaType:        m.MediaType,
			HasMedia:         m.HasMedia,
			ReplyToMessageID: m.ReplyToMessageID,
			Date:             m.Date,
		})
	}

	writeJSON(w, http.StatusOK, ChatContextResponseDTO{
		OK:       true,
		Messages: msgList,
		Limit:    limit,
	})
}

func (s *WebServer) handleConcurrencySettings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		fileConcurrency := 5
		if s.transferMgr != nil {
			fileConcurrency = s.transferMgr.FileConcurrency()
		}
		var maxDataInFlight int64 = 256 * 1024 * 1024
		if s.transferMgr != nil && s.transferMgr.Gate() != nil {
			maxDataInFlight = s.transferMgr.Gate().Max()
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true,
			"settings": map[string]any{
				"max_active_files":   fileConcurrency,
				"max_data_in_flight": maxDataInFlight,
			},
		})
		return
	}

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		MaxActiveFiles int `json:"max_active_files"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if req.MaxActiveFiles < 1 || req.MaxActiveFiles > 64 {
		writeError(w, http.StatusBadRequest, "max_active_files must be between 1 and 64")
		return
	}

	if s.transferMgr != nil {
		if err := s.transferMgr.SetFileConcurrency(req.MaxActiveFiles); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	var maxDataInFlight int64 = 256 * 1024 * 1024
	if s.transferMgr != nil && s.transferMgr.Gate() != nil {
		maxDataInFlight = s.transferMgr.Gate().Max()
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"message": "Settings saved and applied successfully!",
		"settings": map[string]any{
			"max_active_files":   req.MaxActiveFiles,
			"max_data_in_flight": maxDataInFlight,
		},
	})
}

func (s *WebServer) handleProxyList(w http.ResponseWriter, r *http.Request) {
	if s.proxyManager == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":      true,
			"active":  "direct",
			"nodes":   []any{},
			"proxies": []any{},
			"watchdog": map[string]any{
				"interval_seconds": 60,
				"failover_count":   0,
			},
		})
		return
	}

	nodes, current, err := s.proxyManager.GetProxyList(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if s.db != nil {
		db24hBytes := s.db.Get24hSuccessBytes()
		for i := range nodes {
			if nodes[i].Name == current || nodes[i].Tag == current {
				if nodes[i].Metrics24h.TotalBytes24h < float64(db24hBytes) {
					nodes[i].Metrics24h.TotalBytes24h = float64(db24hBytes)
					nodes[i].Metrics24h.TotalBytesStr = formatBytes(db24hBytes)
				}
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"active":  current,
		"nodes":   nodes,
		"proxies": nodes,
		"watchdog": map[string]any{
			"interval_seconds": 60,
			"failover_count":   0,
		},
	})
}

func (s *WebServer) handleProxySwitch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Group   string `json:"group"`
		Name    string `json:"name"`
		Tag     string `json:"tag"`
		Node    string `json:"node"`
		NodeTag string `json:"node_tag"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	targetNode := req.Name
	if targetNode == "" {
		targetNode = req.Tag
	}
	if targetNode == "" {
		targetNode = req.Node
	}
	if targetNode == "" {
		targetNode = req.NodeTag
	}

	if targetNode == "" {
		writeError(w, http.StatusBadRequest, "node name or tag is required")
		return
	}

	if s.proxyManager == nil {
		writeError(w, http.StatusServiceUnavailable, "proxy manager not initialized")
		return
	}

	if err := s.proxyManager.SwitchProxy(r.Context(), req.Group, targetNode); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"active": targetNode,
		"name":   targetNode,
		"tag":    targetNode,
	})
}

func (s *WebServer) handleProxyPing(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string `json:"name"`
		Tag     string `json:"tag"`
		Node    string `json:"node"`
		NodeTag string `json:"node_tag"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	targetNode := req.Name
	if targetNode == "" {
		targetNode = req.Tag
	}
	if targetNode == "" {
		targetNode = req.Node
	}
	if targetNode == "" {
		targetNode = req.NodeTag
	}

	if targetNode == "" {
		writeError(w, http.StatusBadRequest, "node name or tag is required")
		return
	}

	if s.proxyManager == nil {
		writeError(w, http.StatusServiceUnavailable, "proxy manager not initialized")
		return
	}

	delay, err := s.proxyManager.PingProxy(r.Context(), targetNode)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"delay":    delay,
		"delay_ms": delay,
		"ping_ms":  delay,
	})
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func (s *WebServer) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	if s.authWizard == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"logged_in": false,
			"error":     "auth wizard not initialized",
		})
		return
	}
	writeJSON(w, http.StatusOK, s.authWizard.Status(r.Context()))
}

func (s *WebServer) handleAuthQRStart(w http.ResponseWriter, r *http.Request) {
	if s.authWizard == nil {
		writeError(w, http.StatusServiceUnavailable, "auth wizard not initialized")
		return
	}
	var req struct {
		Namespace string `json:"namespace"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Namespace == "" {
		req.Namespace = r.URL.Query().Get("namespace")
	}
	resp, err := s.authWizard.StartQR(r.Context(), req.Namespace)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *WebServer) handleAuthQRPoll(w http.ResponseWriter, r *http.Request) {
	if s.authWizard == nil {
		writeError(w, http.StatusServiceUnavailable, "auth wizard not initialized")
		return
	}
	resp, err := s.authWizard.PollQR(r.Context())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *WebServer) handleAuthSendPhoneCode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Phone     string `json:"phone"`
		Namespace string `json:"namespace"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Phone == "" {
		writeError(w, http.StatusBadRequest, "invalid phone number")
		return
	}
	if s.authWizard == nil {
		writeError(w, http.StatusServiceUnavailable, "auth wizard not initialized")
		return
	}
	resp, err := s.authWizard.SendPhoneCode(r.Context(), req.Phone, req.Namespace)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *WebServer) handleAuthVerifyPhoneCode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Code == "" {
		writeError(w, http.StatusBadRequest, "invalid verification code")
		return
	}
	if s.authWizard == nil {
		writeError(w, http.StatusServiceUnavailable, "auth wizard not initialized")
		return
	}
	resp, err := s.authWizard.VerifyPhoneCode(r.Context(), req.Code)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *WebServer) handleAuthVerify2FA(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Password == "" {
		writeError(w, http.StatusBadRequest, "invalid password")
		return
	}
	if s.authWizard == nil {
		writeError(w, http.StatusServiceUnavailable, "auth wizard not initialized")
		return
	}
	resp, err := s.authWizard.Verify2FA(r.Context(), req.Password)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *WebServer) handleAuthLogoutTelegram(w http.ResponseWriter, r *http.Request) {
	if s.authWizard == nil {
		writeError(w, http.StatusServiceUnavailable, "auth wizard not initialized")
		return
	}
	if err := s.authWizard.Logout(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "message": "logged out successfully"})
}

func (s *WebServer) handleGetAccounts(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database not initialized")
		return
	}
	accounts, err := s.db.GetAccounts()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"accounts": accounts,
	})
}

func (s *WebServer) handleSwitchAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Namespace string `json:"namespace"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Namespace == "" {
		writeError(w, http.StatusBadRequest, "invalid namespace")
		return
	}
	if s.authWizard == nil {
		writeError(w, http.StatusServiceUnavailable, "auth wizard not initialized")
		return
	}
	if err := s.authWizard.SwitchAccount(r.Context(), req.Namespace); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"message":   "switched active account successfully",
		"namespace": req.Namespace,
	})
}

func (s *WebServer) handleDeleteAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Namespace string `json:"namespace"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Namespace == "" {
		writeError(w, http.StatusBadRequest, "invalid namespace")
		return
	}
	if s.authWizard == nil {
		writeError(w, http.StatusServiceUnavailable, "auth wizard not initialized")
		return
	}
	if err := s.authWizard.DeleteAccount(r.Context(), req.Namespace); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"message":   "account deleted successfully",
		"namespace": req.Namespace,
	})
}
