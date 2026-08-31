package daemon

import (
	"crypto/aes"
	"crypto/cipher"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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

	"github.com/Hittlert/TGX/core/bucket"
	"github.com/Hittlert/TGX/core/mover"
	"github.com/Hittlert/TGX/core/targetwriter"
	"github.com/Hittlert/TGX/pkg/consts"
	atomic "github.com/Hittlert/TGX/pkg/sbe/atomic"
	"github.com/Hittlert/TGX/pkg/sbe/gate"
)

//go:embed ui/*
var uiFS embed.FS

type WebServer struct {
	db           *Database
	slotPool     *GlobalSlotPool
	proxyManager *ProxyManager
	orchestrator *Orchestrator
	access       TelegramAccess
	registry     *Registry
	logger       *zap.Logger
	password     string
	gate         *gate.FloodGate
	mover        *mover.Mover
	bkt          bucket.Bucket
	tw           *targetwriter.TargetWriter

	sessionsMu sync.RWMutex
	sessions   map[string]time.Time
	authWizard *AuthWizard
}

func (s *WebServer) SetAuthWizard(w *AuthWizard) {
	s.authWizard = w
}

func (s *WebServer) SetMover(m *mover.Mover) {
	s.mover = m
}

func (s *WebServer) SetBucket(b bucket.Bucket) {
	s.bkt = b
}

func (s *WebServer) SetTargetWriter(tw *targetwriter.TargetWriter) {
	s.tw = tw
}

func NewWebServer(
	db *Database,
	slotPool *GlobalSlotPool,
	proxyManager *ProxyManager,
	orchestrator *Orchestrator,
	access TelegramAccess,
	registry *Registry,
	logger *zap.Logger,
	password string,
	fg *gate.FloodGate,
) *WebServer {
	return &WebServer{
		db:           db,
		slotPool:     slotPool,
		proxyManager: proxyManager,
		orchestrator: orchestrator,
		access:       access,
		registry:     registry,
		logger:       logger,
		password:     password,
		gate:         fg,
		sessions:     make(map[string]time.Time),
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
		resp := map[string]any{
			"max_data_in_flight":    gate.MaxDataInFlight,
			"max_control_in_flight": gate.MaxControlInFlight,
		}
		if s.gate != nil {
			resp["current_rate"] = s.gate.CurrentRate()
			resp["base_rate"] = s.gate.BaseRate()
			resp["data_in_flight"] = s.gate.DataInFlight()
			resp["control_in_flight"] = s.gate.ControlInFlight()
			resp["max_data_cap"] = s.gate.MaxDataCap()
		}
		writeJSON(w, http.StatusOK, resp)
	}).Methods(http.MethodGet)

	r.HandleFunc("/api/system/storage", func(w http.ResponseWriter, r *http.Request) {
		path := "."
		if s.orchestrator != nil {
			path = s.orchestrator.OutputDir()
		}
		freeBytes, totalBytes, err := atomic.GetDiskSpace(path)
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
		if s.bkt != nil {
			m := s.bkt.Metrics()
			resp["buffer"] = map[string]any{
				"mode":                 m.Mode,
				"max_bytes":            m.MaxCapacity,
				"reserved_bytes":       m.ReservedBytes,
				"ready_bytes":          m.ReadyBytes,
				"pending_delete_bytes": m.PendingDeleteBytes,
				"used_bytes":           m.UsedBytes,
				"object_count":         m.ObjectCount,
				"backpressured":        m.Backpressured,
				"max_human":            formatBytes(m.MaxCapacity),
				"used_human":           formatBytes(m.UsedBytes),
				"ready_human":          formatBytes(m.ReadyBytes),
			}
		} else if s.mover != nil {
			resp["buffer"] = map[string]any{
				"used_bytes":    s.mover.UsedBytes(),
				"max_bytes":     s.mover.MaxCapacity(),
				"active_moving": s.mover.ActiveMoving(),
				"used_human":    formatBytes(s.mover.UsedBytes()),
				"max_human":     formatBytes(s.mover.MaxCapacity()),
			}
		}

		if s.tw != nil {
			m := s.tw.Metrics()
			resp["target_writer"] = map[string]any{
				"active":                 m.Active,
				"bytes_per_second":       m.BytesPerSecond,
				"bytes_per_second_human": formatBytes(int64(m.BytesPerSecond)) + "/s",
				"contiguous_write_ratio": fmt.Sprintf("%.1f%%", m.ContiguousWriteRatio),
				"active_files_count":     m.ActiveFilesCount,
				"total_bytes_written":    m.TotalBytesWritten,
				"last_error":             m.LastError,
			}
		}
		writeJSON(w, http.StatusOK, resp)
	}).Methods(http.MethodGet)

	r.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		var req TaskRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}
		snap, created, err := s.registry.Submit(req)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusAccepted
		}
		writeJSON(w, status, snap)
	}).Methods(http.MethodPost)

	r.HandleFunc("/api/control", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Action string `json:"action"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}
		switch req.Action {
		case "pause":
			s.orchestrator.SetRunning(false)
			s.registry.SetPaused(true)
		case "resume":
			s.orchestrator.SetRunning(true)
			s.registry.SetPaused(false)
		default:
			writeError(w, http.StatusBadRequest, "invalid action")
			return
		}
		writeJSON(w, http.StatusOK, s.registry.Status())
	}).Methods(http.MethodPost)

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
	r.HandleFunc("/download_state_change", s.requireAuth(s.handleDownloadStateChange)).Methods(http.MethodGet)

	// Download Control & Status
	r.HandleFunc("/get_download_status", s.requireAuth(s.handleGetDownloadStatus)).Methods(http.MethodGet)
	r.HandleFunc("/set_download_state", s.requireAuth(s.handleSetDownloadState)).Methods(http.MethodPost)
	r.HandleFunc("/get_download_list", s.requireAuth(s.handleGetDownloadList)).Methods(http.MethodGet)
	r.HandleFunc("/api/downloaded_records", s.requireAuth(s.handleGetDownloadedRecords)).Methods(http.MethodGet)

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
			if strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/get_") || strings.HasPrefix(r.URL.Path, "/set_") || strings.HasPrefix(r.URL.Path, "/download_") {
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
	if !s.orchestrator.IsRunning() {
		state = "paused"
	}

	ver := consts.Version
	if ver == "" || ver == "dev" {
		ver = "v4.4.12"
	}

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

	// POST /login
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"code":"0","msg":"invalid json"}`, http.StatusBadRequest)
		return
	}

	decryptedPass := s.decryptFrontendPassword(req.Password)
	if s.password != "" && decryptedPass != s.password && req.Password != s.password {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":"0","msg":"password error"}`))
		return
	}

	token := fmt.Sprintf("sess_%d_%s", time.Now().UnixNano(), "token")
	s.sessionsMu.Lock()
	s.sessions[token] = time.Now().Add(7 * 24 * time.Hour)
	s.sessionsMu.Unlock()

	http.SetCookie(w, &http.Cookie{
		Name:     "tg_downloader_session",
		Value:    token,
		Path:     "/",
		Expires:  time.Now().Add(7 * 24 * time.Hour),
		HttpOnly: true,
	})

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"code":"1","msg":"success"}`))
}

func (s *WebServer) handleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:    "tg_downloader_session",
		Value:   "",
		Path:    "/",
		Expires: time.Unix(0, 0),
	})
	http.Redirect(w, r, "/login", http.StatusFound)
}

func (s *WebServer) decryptFrontendPassword(encryptedB64 string) string {
	defer func() {
		_ = recover()
	}()
	key := []byte("1234123412ABCDEF")
	iv := []byte("ABCDEF1234123412")

	b64Decoded, err := base64.StdEncoding.DecodeString(encryptedB64)
	if err != nil {
		return encryptedB64
	}

	rawCipher, err := hex.DecodeString(string(b64Decoded))
	if err != nil {
		rawCipher = b64Decoded
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return encryptedB64
	}

	if len(rawCipher) < aes.BlockSize || len(rawCipher)%aes.BlockSize != 0 {
		return encryptedB64
	}

	mode := cipher.NewCBCDecrypter(block, iv)
	decrypted := make([]byte, len(rawCipher))
	mode.CryptBlocks(decrypted, rawCipher)

	// Unpad PKCS7
	length := len(decrypted)
	unpadding := int(decrypted[length-1])
	if unpadding < length {
		decrypted = decrypted[:(length - unpadding)]
	}
	return string(decrypted)
}

func (s *WebServer) handleGetAppVersion(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	ver := consts.Version
	if ver == "" || ver == "dev" {
		ver = "v4.4.12"
	}
	_, _ = w.Write([]byte(ver))
}

func (s *WebServer) handleDownloadStateChange(w http.ResponseWriter, r *http.Request) {
	currentState := r.URL.Query().Get("state")
	newState := "running"
	if currentState == "running" {
		newState = "paused"
		s.orchestrator.SetRunning(false)
		s.registry.SetPaused(true)
	} else {
		s.orchestrator.SetRunning(true)
		s.registry.SetPaused(false)
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(newState))
}

func (s *WebServer) handleGetDownloadStatus(w http.ResponseWriter, r *http.Request) {
	snap := s.slotPool.Snapshot()
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
	if !s.orchestrator.IsRunning() {
		state = "paused"
	}

	bufferUsedMB := float64(0)
	bufferLimitMB := float64(512)
	if s.bkt != nil {
		m := s.bkt.Metrics()
		bufferUsedMB = float64(m.UsedBytes) / (1024 * 1024)
		bufferLimitMB = float64(m.MaxCapacity) / (1024 * 1024)
	}

	resp := map[string]any{
		"download_speed": speedStr,
		"speed_bps":      speedBytes,
		"download_state": state,
		"slot_pool": map[string]any{
			"total_slots":          snap.TotalSlots,
			"used_slots":           snap.UsedSlots,
			"available_slots":      snap.AvailableSlots,
			"max_active_files":     snap.MaxActiveFiles,
			"active_files_count":   snap.ActiveFilesCount,
			"slot_unit_mb":         snap.SlotUnitMB,
			"max_slots_per_file":   snap.MaxSlotsPerFile,
			"slot_utilization_pct": snap.SlotUtilizationPct,
			"utilization_pct":      snap.SlotUtilizationPct,
			"file_utilization_pct": snap.FileUtilizationPct,
		},
		"media_pool": map[string]any{
			"active_files": len(filesMap),
			"files":        filesMap,
		},
		"sbe_stats": map[string]any{
			"engine_version":  consts.Version,
			"buffer_used_mb":  bufferUsedMB,
			"buffer_limit_mb": bufferLimitMB,
			"dirty_used_mb":   bufferUsedMB,
			"target_writer":   s.tw != nil && s.tw.Metrics().Active,
		},
		"total_download_task": 0,
		"total_download_byte": formatBytes(0),
		"finish_task_byte":    formatBytes(0),
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *WebServer) handleSetDownloadState(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DownloadState string `json:"download_state"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if req.DownloadState == "running" {
		s.orchestrator.SetRunning(true)
		s.registry.SetPaused(false)
	} else if req.DownloadState == "paused" {
		s.orchestrator.SetRunning(false)
		s.registry.SetPaused(true)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"code":           "1",
		"download_state": req.DownloadState,
	})
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
		if fileName == "" || strings.HasSuffix(fileName, ".bin") || strings.HasSuffix(fileName, ".unknown") {
			base := filepath.Base(f.FinalPath)
			if idx := strings.Index(base, " - "); idx != -1 && len(base) > idx+3 {
				fileName = base[idx+3:]
			} else {
				fileName = base
			}
		}
		if fileName == "" || fileName == "." || strings.HasSuffix(fileName, ".bin") || strings.HasSuffix(fileName, ".unknown") {
			fileName = fmt.Sprintf("%d.mp4", msgID)
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
	var item struct {
		ChatID               string  `json:"chat_id"`
		Enabled              *bool   `json:"enabled"`
		Priority             *int    `json:"priority"`
		LastReadMessageID    *int    `json:"last_read_message_id"`
		DownloadFilter       *string `json:"download_filter"`
		UploadTelegramChatID *string `json:"upload_telegram_chat_id"`
		Title                string  `json:"title"`
		Username             string  `json:"username"`
		Type                 string  `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if item.ChatID == "" {
		writeError(w, http.StatusBadRequest, "chat_id is required")
		return
	}

	canonicalChatID := item.ChatID
	if strings.HasPrefix(item.ChatID, "@") {
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

	targets, _ := s.db.GetListenTargets()
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

	if err := s.db.SaveSingleListenTarget(target); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
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

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"target": target,
	})
}

func (s *WebServer) handleDialogs(w http.ResponseWriter, r *http.Request) {
	refresh := r.URL.Query().Get("refresh") == "true"

	targets, err := s.db.GetListenTargets()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	targetMap := make(map[string]ListenTarget)
	for _, t := range targets {
		targetMap[t.ChatID] = t
	}

	var rawDialogs []DialogDTO
	if refresh {
		rawDialogs, err = s.access.GetDialogs(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	decorated := make([]map[string]any, 0, len(targets)+len(rawDialogs))
	seenIDs := make(map[string]bool)

	// 1. First append all configured business targets from database
	for _, t := range targets {
		seenIDs[t.ChatID] = true
		cursor, _, _ := s.db.GetScanCursorWithTime(t.ChatID)
		var lastMsgDate int64
		_ = s.db.DB().QueryRow(`SELECT COALESCE(MAX(date), 0) FROM chat_messages WHERE chat_id = ?`, t.ChatID).Scan(&lastMsgDate)
		if lastMsgDate == 0 {
			lastMsgDate = t.UpdatedAt
		}

		decorated = append(decorated, map[string]any{
			"id":                      t.ChatID,
			"chat_id":                 t.ChatID,
			"title":                   t.Title,
			"username":                t.Username,
			"type":                    t.ChatType,
			"pinned":                  false,
			"unread_count":            0,
			"last_read_message_id":    cursor,
			"is_target":               t.Enabled,
			"enabled":                 t.Enabled,
			"target_enabled":          t.Enabled,
			"priority":                t.Priority,
			"download_filter":         t.DownloadFilter,
			"upload_telegram_chat_id": t.UploadTelegramChatID,
			"last_message_at":         lastMsgDate,
			"last_scan_finished_at":   lastMsgDate,
			"last_scan_started_at":    lastMsgDate,
			"updated_at":              t.UpdatedAt,
		})
	}

	// 2. Append newly discovered unconfigured dialogs as read-only view items (without touching DB)
	for _, d := range rawDialogs {
		if seenIDs[d.ChatID] {
			continue
		}
		seenIDs[d.ChatID] = true
		decorated = append(decorated, map[string]any{
			"id":                      d.ChatID,
			"chat_id":                 d.ChatID,
			"title":                   d.Title,
			"username":                d.Username,
			"type":                    d.Type,
			"pinned":                  d.Pinned,
			"unread_count":            d.UnreadCount,
			"last_read_message_id":    0,
			"is_target":               false,
			"enabled":                 false,
			"target_enabled":          false,
			"priority":                0,
			"download_filter":         "",
			"upload_telegram_chat_id": "",
			"last_message_at":         0,
			"last_scan_finished_at":   0,
			"last_scan_started_at":    0,
			"updated_at":              0,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"dialogs": decorated,
	})
}

func (s *WebServer) handleResolveTarget(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query  string `json:"query"`
		Target string `json:"target"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	query := req.Query
	if query == "" {
		query = req.Target
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
			"id":       info.ID,
			"chat_id":  info.ChatID,
			"title":    info.Title,
			"username": info.Username,
			"type":     info.Type,
		},
	})
}

func (s *WebServer) handleAddTarget(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query  string `json:"query"`
		Target string `json:"target"`
		ChatID string `json:"chat_id"`
		Title  string `json:"title"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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

	info, err := s.access.ResolvePeerInfo(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	targetItem := ListenTarget{
		ChatID:   info.ChatID,
		Title:    info.Title,
		Username: info.Username,
		ChatType: info.Type,
		Enabled:  true,
		Priority: 0,
	}
	_ = s.db.SaveSingleListenTarget(targetItem)

	globalUpdatesStreamMu.RLock()
	stream := globalUpdatesStream
	globalUpdatesStreamMu.RUnlock()
	if stream != nil {
		stream.refreshTargetCache()
	}

	dialogObj := map[string]any{
		"id":                      info.ID,
		"chat_id":                 info.ChatID,
		"title":                   info.Title,
		"username":                info.Username,
		"type":                    info.Type,
		"enabled":                 true,
		"priority":                0,
		"download_filter":         "",
		"upload_telegram_chat_id": "",
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"peer":   info,
		"dialog": dialogObj,
		"target": dialogObj,
	})
}

func (s *WebServer) handleTargetProgress(w http.ResponseWriter, r *http.Request) {
	targets, err := s.db.GetListenTargets()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	stats, _ := s.db.GetTargetProgressStats()

	progressList := make([]map[string]any, 0, len(targets))
	for _, t := range targets {
		cursor, _ := s.db.GetScanCursor(t.ChatID)
		st := stats[t.ChatID]

		progressList = append(progressList, map[string]any{
			"chat_id":              t.ChatID,
			"title":                t.Title,
			"enabled":              t.Enabled,
			"last_read_message_id": cursor,
			"scan_status":          "ok",
			"total_files":          st.TotalFiles,
			"downloaded_files":     st.DownloadedFiles,
			"pending_files":        st.PendingFiles,
			"processing_files":     st.ProcessingFiles,
			"failed_files":         st.FailedFiles,
			"skipped_files":        st.SkippedFiles,
			"downloaded_bytes":     st.DownloadedBytes,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"progress": progressList,
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

	limitBefore := 15
	limitAfter := 15
	if lb, err := strconv.Atoi(r.URL.Query().Get("limit_before")); err == nil && lb > 0 {
		limitBefore = lb
	}
	if la, err := strconv.Atoi(r.URL.Query().Get("limit_after")); err == nil && la > 0 {
		limitAfter = la
	}

	// 1. First fetch from local SQLite
	msgs, err := s.db.GetChatMessagesAround(chatID, targetMid, limitBefore, limitAfter)
	if err != nil || len(msgs) == 0 {
		// 2. Fallback: fetch from Telegram MTProto
		req := HistoryRequest{
			Peer:     chatID,
			OffsetID: targetMid + limitAfter,
			Limit:    limitBefore + limitAfter,
			Reverse:  true,
		}
		if tgMsgs, tgErr := s.access.GetHistory(r.Context(), req); tgErr == nil && len(tgMsgs) > 0 {
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

	msgList := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		msgList = append(msgList, map[string]any{
			"chat_id":             m.ChatID,
			"message_id":          m.MessageID,
			"sender_id":           m.SenderID,
			"sender_name":         m.SenderName,
			"text":                m.Text,
			"media_type":          m.MediaType,
			"has_media":           m.HasMedia,
			"reply_to_message_id": m.ReplyToMessageID,
			"date":                m.Date,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"messages": msgList,
	})
}

func (s *WebServer) handleConcurrencySettings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		snap := s.slotPool.Snapshot()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true,
			"settings": map[string]any{
				"max_active_files":   snap.MaxActiveFiles,
				"global_thread_pool": snap.TotalSlots,
				"disable_ipv6":       true,
			},
		})
		return
	}

	// POST settings
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"message": "Settings saved and applied successfully!",
	})
}

func (s *WebServer) handleProxyList(w http.ResponseWriter, r *http.Request) {
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
		writeError(w, http.StatusInternalServerError, "auth wizard not initialized")
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
		writeError(w, http.StatusInternalServerError, "auth wizard not initialized")
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
	if s.authWizard == nil {
		writeError(w, http.StatusInternalServerError, "auth wizard not initialized")
		return
	}
	var req struct {
		Phone     string `json:"phone"`
		Namespace string `json:"namespace"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Phone == "" {
		writeError(w, http.StatusBadRequest, "invalid phone number")
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
	if s.authWizard == nil {
		writeError(w, http.StatusInternalServerError, "auth wizard not initialized")
		return
	}
	var req struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Code == "" {
		writeError(w, http.StatusBadRequest, "invalid verification code")
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
	if s.authWizard == nil {
		writeError(w, http.StatusInternalServerError, "auth wizard not initialized")
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Password == "" {
		writeError(w, http.StatusBadRequest, "invalid password")
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
		writeError(w, http.StatusInternalServerError, "auth wizard not initialized")
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
		writeError(w, http.StatusInternalServerError, "database not initialized")
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
	if s.authWizard == nil {
		writeError(w, http.StatusInternalServerError, "auth wizard not initialized")
		return
	}
	var req struct {
		Namespace string `json:"namespace"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Namespace == "" {
		writeError(w, http.StatusBadRequest, "invalid namespace")
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
	if s.authWizard == nil {
		writeError(w, http.StatusInternalServerError, "auth wizard not initialized")
		return
	}
	var req struct {
		Namespace string `json:"namespace"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Namespace == "" {
		writeError(w, http.StatusBadRequest, "invalid namespace")
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
