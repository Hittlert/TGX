package daemon

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/auth/qrlogin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/skip2/go-qrcode"
	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/storage"
)

type AuthWizard struct {
	client *telegram.Client
	kv     storage.Storage
	logger *zap.Logger

	mu            sync.RWMutex
	currentFlow   string // "idle", "qr", "phone"
	qrToken       qrlogin.Token
	qrCancel      context.CancelFunc
	phone         string
	phoneCodeHash string
	need2FA       bool
	lastError     string
	isLoggedIn    bool
	currentUser   *tg.User
}

func NewAuthWizard(client *telegram.Client, kv storage.Storage, logger *zap.Logger) *AuthWizard {
	w := &AuthWizard{
		client: client,
		kv:     kv,
		logger: logger,
	}
	_ = w.CheckStatus(context.Background())
	return w
}

func (w *AuthWizard) CheckStatus(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.client == nil {
		w.isLoggedIn = false
		return nil
	}

	authStatus, err := w.client.Auth().Status(ctx)
	if err == nil && authStatus.Authorized {
		w.isLoggedIn = true
		self, err := w.client.Self(ctx)
		if err == nil {
			w.currentUser = self
		}
		return nil
	}

	w.isLoggedIn = false
	return nil
}

func (w *AuthWizard) Status(ctx context.Context) map[string]any {
	_ = w.CheckStatus(ctx)

	w.mu.RLock()
	defer w.mu.RUnlock()

	resp := map[string]any{
		"logged_in": w.isLoggedIn,
		"flow":      w.currentFlow,
		"need_2fa":  w.need2FA,
		"error":     w.lastError,
	}

	if w.currentUser != nil {
		resp["user"] = map[string]any{
			"id":         w.currentUser.ID,
			"first_name": w.currentUser.FirstName,
			"last_name":  w.currentUser.LastName,
			"username":   w.currentUser.Username,
			"phone":      w.currentUser.Phone,
		}
	}
	return resp
}

// StartQR generates a new QR code token and returns the Base64 PNG image.
func (w *AuthWizard) StartQR(ctx context.Context) (map[string]any, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.client == nil {
		return nil, errors.New("telegram client not initialized")
	}

	if w.qrCancel != nil {
		w.qrCancel()
	}

	qrCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	w.qrCancel = cancel

	token, err := w.client.QR().Export(qrCtx)
	if err != nil {
		w.lastError = err.Error()
		return nil, fmt.Errorf("failed to export qr login token: %w", err)
	}

	w.qrToken = token
	w.currentFlow = "qr"
	w.need2FA = false
	w.lastError = ""

	pngBytes, err := qrcode.Encode(token.URL(), qrcode.Medium, 256)
	if err != nil {
		return nil, fmt.Errorf("failed to generate qr png: %w", err)
	}

	b64Image := "data:image/png;base64," + base64.StdEncoding.EncodeToString(pngBytes)

	return map[string]any{
		"ok":              true,
		"qr_url":          token.URL(),
		"qr_image_base64": b64Image,
		"expires_in":      int(time.Until(token.Expires()).Seconds()),
	}, nil
}

// PollQR checks if the user has approved the QR token in their Telegram app.
func (w *AuthWizard) PollQR(ctx context.Context) (map[string]any, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentFlow != "qr" || w.qrToken.URL() == "" {
		return map[string]any{"status": "idle"}, nil
	}

	if time.Now().After(w.qrToken.Expires()) {
		return map[string]any{"status": "expired", "message": "二维码已过期，请刷新"}, nil
	}

	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	authResult, err := w.client.QR().Accept(pollCtx, w.qrToken)
	if err != nil {
		if errors.Is(err, auth.ErrPasswordAuthNeeded) || tgerr.Is(err, "SESSION_PASSWORD_NEEDED") {
			w.need2FA = true
			return map[string]any{"status": "need_2fa", "message": "请输入两步验证 (2FA) 密码"}, nil
		}
		// Still waiting for scan
		return map[string]any{"status": "waiting"}, nil
	}

	if authResult != nil {
		w.isLoggedIn = true
		w.currentFlow = "idle"
		w.need2FA = false
		self, _ := w.client.Self(ctx)
		w.currentUser = self
		return map[string]any{"status": "success", "user": self}, nil
	}

	return map[string]any{"status": "waiting"}, nil
}

// SendPhoneCode requests Telegram to send an SMS/App authentication code to the phone number.
func (w *AuthWizard) SendPhoneCode(ctx context.Context, phone string) (map[string]any, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.client == nil {
		return nil, errors.New("telegram client not initialized")
	}

	sentCode, err := w.client.Auth().SendCode(ctx, phone, auth.SendCodeOptions{})
	if err != nil {
		w.lastError = err.Error()
		return nil, err
	}

	var codeHash string
	switch s := sentCode.(type) {
	case *tg.AuthSentCode:
		codeHash = s.PhoneCodeHash
	case *tg.AuthSentCodeSuccess:
		// Automatically logged in
		w.isLoggedIn = true
		w.currentFlow = "idle"
		self, _ := w.client.Self(ctx)
		w.currentUser = self
		return map[string]any{"ok": true, "status": "success", "user": self}, nil
	}

	w.phone = phone
	w.phoneCodeHash = codeHash
	w.currentFlow = "phone"
	w.need2FA = false
	w.lastError = ""

	return map[string]any{
		"ok":              true,
		"status":          "code_sent",
		"phone":           phone,
		"phone_code_hash": codeHash,
	}, nil
}

// VerifyPhoneCode verifies the code received via SMS/App.
func (w *AuthWizard) VerifyPhoneCode(ctx context.Context, code string) (map[string]any, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.phone == "" || w.phoneCodeHash == "" {
		return nil, errors.New("no pending phone verification")
	}

	_, err := w.client.Auth().SignIn(ctx, w.phone, code, w.phoneCodeHash)
	if err != nil {
		if errors.Is(err, auth.ErrPasswordAuthNeeded) || tgerr.Is(err, "SESSION_PASSWORD_NEEDED") {
			w.need2FA = true
			return map[string]any{"status": "need_2fa", "message": "请输入两步验证 (2FA) 密码"}, nil
		}
		w.lastError = err.Error()
		return nil, err
	}

	w.isLoggedIn = true
	w.currentFlow = "idle"
	w.need2FA = false
	self, _ := w.client.Self(ctx)
	w.currentUser = self

	return map[string]any{"status": "success", "user": self}, nil
}

// Verify2FA verifies the 2-step verification (Cloud Password) using SRP.
func (w *AuthWizard) Verify2FA(ctx context.Context, password string) (map[string]any, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.need2FA {
		return nil, errors.New("2FA verification is not pending")
	}

	_, err := w.client.Auth().Password(ctx, password)
	if err != nil {
		w.lastError = err.Error()
		return nil, err
	}

	w.isLoggedIn = true
	w.currentFlow = "idle"
	w.need2FA = false
	self, _ := w.client.Self(ctx)
	w.currentUser = self

	return map[string]any{"status": "success", "user": self}, nil
}

// Logout clears the Telegram session.
func (w *AuthWizard) Logout(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.client != nil {
		_, _ = w.client.API().AuthLogOut(ctx)
	}
	w.isLoggedIn = false
	w.currentUser = nil
	w.currentFlow = "idle"
	w.need2FA = false
	return nil
}
