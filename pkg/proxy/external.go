package proxy

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	xproxy "golang.org/x/net/proxy"
)

// ExternalProxyProvider manages connections through standard SOCKS5/HTTP/HTTPS proxy endpoints.
type ExternalProxyProvider struct {
	rawURL   string
	proxyURL *url.URL
	timeout  time.Duration

	mu     sync.RWMutex
	dialer ContextDialer
}

// NewExternalProxyProvider creates a new proxy provider from a proxy URL string.
func NewExternalProxyProvider(proxyAddr string, timeout time.Duration) (*ExternalProxyProvider, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	u, err := url.Parse(proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy url %s: %w", proxyAddr, err)
	}

	p := &ExternalProxyProvider{
		rawURL:   proxyAddr,
		proxyURL: u,
		timeout:  timeout,
	}

	dialer, err := p.buildDialer(u)
	if err != nil {
		return nil, err
	}
	p.dialer = dialer

	return p, nil
}

func (p *ExternalProxyProvider) buildDialer(u *url.URL) (ContextDialer, error) {
	scheme := strings.ToLower(u.Scheme)
	directDialer := &net.Dialer{
		Timeout:   p.timeout,
		KeepAlive: 30 * time.Second,
	}

	switch scheme {
	case "socks5", "socks5h":
		var auth *xproxy.Auth
		if u.User != nil {
			auth = &xproxy.Auth{
				User: u.User.Username(),
			}
			if pass, ok := u.User.Password(); ok {
				auth.Password = pass
			}
		}
		d, err := xproxy.SOCKS5("tcp", u.Host, auth, directDialer)
		if err != nil {
			return nil, fmt.Errorf("failed to create socks5 dialer: %w", err)
		}
		if cd, ok := d.(ContextDialer); ok {
			return cd, nil
		}
		return &fallbackContextDialer{d: d}, nil

	case "http", "https":
		return &httpConnectDialer{
			proxyURL: u,
			forward:  directDialer,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported proxy scheme: %s", scheme)
	}
}

func (p *ExternalProxyProvider) GetDialer(ctx context.Context, dcID int) (ContextDialer, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dialer, nil
}

func (p *ExternalProxyProvider) ReportFailure(dcID int, err error) {
	// Can be linked to Watchdog
}

func (p *ExternalProxyProvider) Close() error {
	return nil
}

type fallbackContextDialer struct {
	d xproxy.Dialer
}

func (f *fallbackContextDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if cd, ok := f.d.(xproxy.ContextDialer); ok {
		return cd.DialContext(ctx, network, address)
	}
	return f.d.Dial(network, address)
}

type httpConnectDialer struct {
	proxyURL *url.URL
	forward  *net.Dialer
}

func (h *httpConnectDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := h.forward.DialContext(ctx, "tcp", h.proxyURL.Host)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to http proxy %s: %w", h.proxyURL.Host, err)
	}

	req := &http.Request{
		Method: "CONNECT",
		URL:    &url.URL{Opaque: address},
		Host:   address,
		Header: make(http.Header),
	}

	if h.proxyURL.User != nil {
		user := h.proxyURL.User.Username()
		pass, _ := h.proxyURL.User.Password()
		auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
		req.Header.Set("Proxy-Authorization", "Basic "+auth)
	}

	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to write CONNECT request: %w", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read CONNECT response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		conn.Close()
		return nil, fmt.Errorf("proxy CONNECT failed with status: %s", resp.Status)
	}

	return conn, nil
}
