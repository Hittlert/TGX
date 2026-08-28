package proxy

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// ReconnectHook is invoked when a proxy failover occurs, instructing connection pools to cycle.
type ReconnectHook func(oldNode, newNode string)

// NodeHealth tracks the latency and failure count for a proxy node.
type NodeHealth struct {
	Address     string        `json:"address"`
	Protocol    string        `json:"protocol"`
	IsHealthy   bool          `json:"is_healthy"`
	Latency     time.Duration `json:"latency"`
	LastProbe   time.Time     `json:"last_probe"`
	FailCount   int           `json:"fail_count"`
	TotalProbes int           `json:"total_probes"`
}

// Watchdog manages health probes, statistics, and automatic failover.
type Watchdog struct {
	probeTarget string
	nodes       []string
	currentIndex int
	healthMap   map[string]*NodeHealth

	interval    time.Duration
	hook        ReconnectHook
	provider    *ExternalProxyProvider

	mu       sync.RWMutex
	stopChan chan struct{}
	doneChan chan struct{}
}

// Config defines Watchdog probe parameters.
type Config struct {
	ProbeTarget string // e.g. "149.154.167.50:443" (TG DC2) or "google.com:443"
	Interval    time.Duration
	Nodes       []string
	Hook        ReconnectHook
}

// NewWatchdog creates a proxy health monitor.
func NewWatchdog(cfg Config) *Watchdog {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.ProbeTarget == "" {
		cfg.ProbeTarget = "149.154.167.50:443" // Telegram DC2 default
	}

	healthMap := make(map[string]*NodeHealth)
	for _, n := range cfg.Nodes {
		healthMap[n] = &NodeHealth{
			Address:   n,
			IsHealthy: true,
		}
	}

	w := &Watchdog{
		probeTarget: cfg.ProbeTarget,
		nodes:       cfg.Nodes,
		healthMap:   healthMap,
		interval:    cfg.Interval,
		hook:        cfg.Hook,
		stopChan:    make(chan struct{}),
		doneChan:    make(chan struct{}),
	}

	if len(cfg.Nodes) > 0 {
		p, _ := NewExternalProxyProvider(cfg.Nodes[0], 10*time.Second)
		w.provider = p
	}

	return w
}

// Start launches the background health check loop.
func (w *Watchdog) Start() {
	go w.probeLoop()
}

func (w *Watchdog) probeLoop() {
	defer close(w.doneChan)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopChan:
			return
		case <-ticker.C:
			w.runProbe()
		}
	}
}

// runProbe tests all nodes and performs failover if current node fails.
func (w *Watchdog) runProbe() {
	w.mu.Lock()
	nodes := make([]string, len(w.nodes))
	copy(nodes, w.nodes)
	curIdx := w.currentIndex
	w.mu.Unlock()

	if len(nodes) == 0 {
		return
	}

	for _, node := range nodes {
		p, err := NewExternalProxyProvider(node, 5*time.Second)
		if err != nil {
			w.recordResult(node, false, 0)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		dialer, err := p.GetDialer(ctx, 2)
		if err != nil {
			cancel()
			w.recordResult(node, false, 0)
			continue
		}

		start := time.Now()
		conn, err := dialer.DialContext(ctx, "tcp", w.probeTarget)
		latency := time.Since(start)
		cancel()

		if err != nil {
			w.recordResult(node, false, 0)
		} else {
			conn.Close()
			w.recordResult(node, true, latency)
		}
	}

	// Check if current node is down
	w.mu.Lock()
	defer w.mu.Unlock()

	curNode := nodes[curIdx]
	curHealth := w.healthMap[curNode]
	if curHealth != nil && !curHealth.IsHealthy {
		// Try failover to next healthy node
		for i := 0; i < len(nodes); i++ {
			nextIdx := (curIdx + 1 + i) % len(nodes)
			candidate := nodes[nextIdx]
			if w.healthMap[candidate] != nil && w.healthMap[candidate].IsHealthy {
				// Failover!
				oldNode := curNode
				w.currentIndex = nextIdx
				p, err := NewExternalProxyProvider(candidate, 10*time.Second)
				if err == nil {
					w.provider = p
					if w.hook != nil {
						go w.hook(oldNode, candidate)
					}
				}
				break
			}
		}
	}
}

func (w *Watchdog) recordResult(node string, success bool, latency time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	h, ok := w.healthMap[node]
	if !ok {
		h = &NodeHealth{Address: node}
		w.healthMap[node] = h
	}

	h.TotalProbes++
	h.LastProbe = time.Now()

	if success {
		h.IsHealthy = true
		h.Latency = latency
		h.FailCount = 0
	} else {
		h.FailCount++
		if h.FailCount >= 2 {
			h.IsHealthy = false
		}
	}
}

// CurrentProvider returns the active DialerProvider.
func (w *Watchdog) CurrentProvider() DialerProvider {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.provider != nil {
		return w.provider
	}
	return NewDirectProvider(30 * time.Second)
}

// NodeHealthList returns health metrics for all registered proxy nodes.
func (w *Watchdog) NodeHealthList() []NodeHealth {
	w.mu.RLock()
	defer w.mu.RUnlock()

	list := make([]NodeHealth, 0, len(w.healthMap))
	for _, h := range w.healthMap {
		list = append(list, *h)
	}
	return list
}

// Stop terminates the watchdog probe loop.
func (w *Watchdog) Stop() {
	select {
	case <-w.stopChan:
	default:
		close(w.stopChan)
	}
	<-w.doneChan
}

// MockDialer creates a simple net.Dialer mock for testing.
type MockDialer struct {
	dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)
}

func (m *MockDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if m.dialFunc != nil {
		return m.dialFunc(ctx, network, address)
	}
	return nil, fmt.Errorf("mock dialer not configured")
}
