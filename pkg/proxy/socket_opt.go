package proxy

import (
	"net"
	"syscall"
	"time"
)

const (
	// Optimal socket buffer size (2MB) for high-throughput streaming
	SocketBufferSize = 2 * 1024 * 1024
)

// SocketControl configures optimal socket buffers and TCP keep-alive on the raw socket connection.
func SocketControl(network, address string, c syscall.RawConn) error {
	var controlErr error
	err := c.Control(func(fd uintptr) {
		// 1. Set SO_RCVBUF
		_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, SocketBufferSize)
		// 2. Set SO_SNDBUF
		_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, SocketBufferSize)
		// 3. Set SO_KEEPALIVE
		_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1)
	})
	if err != nil {
		return err
	}
	return controlErr
}

// NewTunedDialer returns a *net.Dialer tuned for maximum throughput and resilience.
func NewTunedDialer(timeout time.Duration) *net.Dialer {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
		Control:   SocketControl,
	}
}
