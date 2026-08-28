package storage

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/gotd/td/telegram"

	"github.com/Hittlert/TGX/core/storage/keygen"
)

type Session struct {
	kv    Storage
	login bool
	mu    sync.Mutex
}

func NewSession(kv Storage, login bool) telegram.SessionStorage {
	return &Session{kv: kv, login: login}
}

func (s *Session) LoadSession(ctx context.Context) ([]byte, error) {
	if s.login {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	b, err := s.kv.Get(ctx, s.key())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return bytes.Clone(b), nil
}

func (s *Session) StoreSession(ctx context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kv.Set(ctx, s.key(), bytes.Clone(data))
}

func (s *Session) key() string {
	return keygen.New("session")
}
