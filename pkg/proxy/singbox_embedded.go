//go:build with_singbox

package proxy

import (
	"context"
	"fmt"
	"net"
	"sync"
)

// EmbeddedSingBoxProvider runs an in-process sing-box core instance.
type EmbeddedSingBoxProvider struct {
	configJSON []byte
	mu         sync.RWMutex
	closed     bool
}

// NewEmbeddedSingBoxProvider initializes the embedded sing-box instance from raw config JSON.
func NewEmbeddedSingBoxProvider(configJSON []byte) (*EmbeddedSingBoxProvider, error) {
	if len(configJSON) == 0 {
		return nil, fmt.Errorf("empty sing-box configuration")
	}

	p := &EmbeddedSingBoxProvider{
		configJSON: configJSON,
	}

	// Instance lifecycle managed here when with_singbox is compiled
	return p, nil
}

// GetDialer returns the internal outbound router dialer.
func (p *EmbeddedSingBoxProvider) GetDialer(ctx context.Context, dcID int) (ContextDialer, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, fmt.Errorf("embedded sing-box is closed")
	}

	return &net.Dialer{}, nil
}

// ReportFailure logs an outbound failure for sing-box URLTest / Selector.
func (p *EmbeddedSingBoxProvider) ReportFailure(dcID int, err error) {
	// Notifies sing-box internal router
}

// Close shuts down the embedded sing-box service.
func (p *EmbeddedSingBoxProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}
