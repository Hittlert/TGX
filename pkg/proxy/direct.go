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

// NewDirectProvider creates a new direct connection provider with socket tuning.
func NewDirectProvider(timeout time.Duration) *DirectProvider {
	return &DirectProvider{
		dialer: NewTunedDialer(timeout),
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
