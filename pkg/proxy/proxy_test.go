package proxy

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDirectProvider_Basic(t *testing.T) {
	dp := NewDirectProvider(5 * time.Second)
	ctx := context.Background()

	dialer, err := dp.GetDialer(ctx, 2)
	require.NoError(t, err)
	assert.NotNil(t, dialer)
	assert.NoError(t, dp.Close())
}

func TestExternalProxyProvider_Parsers(t *testing.T) {
	// SOCKS5
	sp, err := NewExternalProxyProvider("socks5://user:pass@127.0.0.1:1080", 5*time.Second)
	require.NoError(t, err)
	assert.NotNil(t, sp)

	dialer, err := sp.GetDialer(context.Background(), 2)
	require.NoError(t, err)
	assert.NotNil(t, dialer)

	// HTTP
	hp, err := NewExternalProxyProvider("http://proxy.local:8080", 5*time.Second)
	require.NoError(t, err)
	assert.NotNil(t, hp)

	// Invalid scheme
	_, err = NewExternalProxyProvider("ftp://127.0.0.1", 5*time.Second)
	assert.Error(t, err)
}

func TestWatchdog_RecordAndHealth(t *testing.T) {
	var reconnected uint32

	hook := func(oldNode, newNode string) {
		atomic.AddUint32(&reconnected, 1)
	}

	w := NewWatchdog(Config{
		Interval: 100 * time.Millisecond,
		Nodes:    []string{"socks5://127.0.0.1:1080", "socks5://127.0.0.1:1081"},
		Hook:     hook,
	})

	w.recordResult("socks5://127.0.0.1:1080", true, 20*time.Millisecond)
	list := w.NodeHealthList()
	assert.Equal(t, 2, len(list))

	w.recordResult("socks5://127.0.0.1:1080", false, 0)
	w.recordResult("socks5://127.0.0.1:1080", false, 0)

	// After 2 failures -> node marked unhealthy
	w.mu.RLock()
	h := w.healthMap["socks5://127.0.0.1:1080"]
	assert.False(t, h.IsHealthy)
	w.mu.RUnlock()
}

func startMockTCP(t *testing.T) (net.Listener, string) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	return l, l.Addr().String()
}
