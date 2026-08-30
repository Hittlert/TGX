package tclient

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/td/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockKVStorage struct {
	data map[string][]byte
}

func (m *mockKVStorage) Get(ctx context.Context, key string) ([]byte, error) {
	if val, ok := m.data[key]; ok {
		return val, nil
	}
	return nil, nil
}

func (m *mockKVStorage) Set(ctx context.Context, key string, val []byte) error {
	m.data[key] = val
	return nil
}

func (m *mockKVStorage) Delete(ctx context.Context, key string) error {
	delete(m.data, key)
	return nil
}

func (m *mockKVStorage) Close() error {
	return nil
}

func TestNewDefaultMiddlewares(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	mws := NewDefaultMiddlewares(ctx, 5*time.Second)
	require.Len(t, mws, 2)
	assert.NotNil(t, mws[0], "recovery middleware")
	assert.NotNil(t, mws[1], "retry middleware")
}

func TestNew_AppIDFallbackAndSessionStorage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	mockKV := &mockKVStorage{data: make(map[string][]byte)}

	// Test New with AppID=0 and KV storage provided
	client, err := New(ctx, Options{
		KV:               mockKV,
		ReconnectTimeout: 10 * time.Second,
	})
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestNew_CustomSessionStorage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	customSession := &session.StorageMemory{}

	client, err := New(ctx, Options{
		AppID:            12345,
		AppHash:          "abcdef",
		Session:          customSession,
		ReconnectTimeout: 10 * time.Second,
	})
	require.NoError(t, err)
	assert.NotNil(t, client)
}
