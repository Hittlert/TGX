package netutil

import (
	"context"
	"net"
	"net/url"
	"strings"

	"github.com/go-faster/errors"
	"github.com/iyear/connectproxy"
	"golang.org/x/net/proxy"
)

func init() {
	connectproxy.Register(&connectproxy.Config{
		InsecureSkipVerify: true,
	})
}

type ipv4ProxyDialer struct {
	dialer proxy.ContextDialer
}

func (d ipv4ProxyDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

func (d ipv4ProxyDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err == nil && strings.Contains(host, ":") {
		return nil, errors.Errorf("IPv6 target %s blocked (IPv4 only mode)", addr)
	}
	return d.dialer.DialContext(ctx, "tcp4", addr)
}

func NewProxy(proxyUrl string) (proxy.ContextDialer, error) {
	u, err := url.Parse(proxyUrl)
	if err != nil {
		return nil, errors.Wrap(err, "parse proxy url")
	}
	dialer, err := proxy.FromURL(u, proxy.Direct)
	if err != nil {
		return nil, errors.Wrap(err, "proxy from url")
	}

	if d, ok := dialer.(proxy.ContextDialer); ok {
		return ipv4ProxyDialer{dialer: d}, nil
	}

	return nil, errors.New("proxy dialer is not ContextDialer")
}
