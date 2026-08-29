package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProxyManagerManualSelectAndSticky(t *testing.T) {
	currentSelected := "node-1"
	var switchCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/proxies":
			resp := clashProxiesResp{
				Proxies: map[string]struct {
					Name    string   `json:"name"`
					Type    string   `json:"type"`
					Now     string   `json:"now"`
					All     []string `json:"all"`
					History []struct {
						Delay int `json:"delay"`
					} `json:"history"`
				}{
					"proxy": {
						Name: "proxy",
						Type: "Selector",
						Now:  currentSelected,
						All:  []string{"node-1", "node-2", "node-3"},
					},
					"node-1": {Name: "node-1", Type: "VLESS"},
					"node-2": {Name: "node-2", Type: "VLESS"},
					"node-3": {Name: "node-3", Type: "VLESS"},
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
		case "/proxies/proxy":
			if r.Method == http.MethodPut {
				var req map[string]string
				_ = json.NewDecoder(r.Body).Decode(&req)
				currentSelected = req["name"]
				switchCalls.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}
		}
	}))
	defer server.Close()

	pm := NewProxyManager(server.URL, "")

	// 1. Initial list check
	nodes, active, err := pm.GetProxyList(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "node-1", active)
	assert.Len(t, nodes, 3)

	// 2. Manual Select node-2
	err = pm.SwitchProxy(context.Background(), "proxy", "node-2")
	require.NoError(t, err)
	assert.Equal(t, "node-2", pm.GetActiveProxy())
	assert.Equal(t, int32(1), switchCalls.Load())

	// 3. Verify it is sticky
	assert.Equal(t, "node-2", pm.GetActiveProxy())
}

func TestProxyManagerMetricsRecording(t *testing.T) {
	pm := NewProxyManager("http://127.0.0.1:9090", "")

	pm.RecordDownloadSpeed("node-1", 10*1024*1024)
	time.Sleep(10 * time.Millisecond)
	pm.RecordDownloadSpeed("node-1", 20*1024*1024)

	pm.mu.RLock()
	stat := pm.nodeStats["node-1"]
	require.NotNil(t, stat)
	assert.Len(t, stat.Samples, 2)
	pm.mu.RUnlock()
}
