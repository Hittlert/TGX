package proxy

import (
	"context"
	"net"
	"time"
)

// DirectProvider implements DialerProvider via direct OS TCP connections.
type DirectProvider struct {
	dialer *net.Dialer
}

// NewDirectProvider creates a new direct connection provider.
func NewDirectProvider(timeout time.Duration) *DirectProvider {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &DirectProvider{
		dialer: &net.Dialer{
			Timeout:   timeout,
			KeepAlive: 30 * time.Second,
		},
	}
}

func (p *DirectProvider) GetDialer(ctx context.Context, dcID int) (ContextDialer, error) {
	return p.dialer, nil
}

func (p *DirectProvider) ReportFailure(dcID int, err error) {
	// No-op for direct provider
}

func (p *DirectProvider) Close() error {
	return nil
}
