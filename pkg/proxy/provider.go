package proxy

import (
	"context"
	"net"
)

// ContextDialer is the standard interface for dialing connections with a context.
type ContextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// DialerProvider abstracts the network dialer layer for MTProto DC connections.
type DialerProvider interface {
	GetDialer(ctx context.Context, dcID int) (ContextDialer, error)
	ReportFailure(dcID int, err error)
	Close() error
}
