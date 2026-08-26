package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
)

type dialContextFunc func(context.Context, string, string) (net.Conn, error)

type proxySnapshot struct {
	Active      int64 `json:"active"`
	Opened      int64 `json:"opened"`
	BlockedIPv6 int64 `json:"blocked_ipv6"`
}

type connectProxy struct {
	upstream    *url.URL
	dialContext dialContextFunc
	active      atomic.Int64
	opened      atomic.Int64
	blockedIPv6 atomic.Int64
}

func newProxy(upstream string, dialContext dialContextFunc) *connectProxy {
	parsed, err := url.Parse(upstream)
	if err != nil {
		panic(err)
	}
	return &connectProxy{upstream: parsed, dialContext: dialContext}
}

func (p *connectProxy) snapshot() proxySnapshot {
	return proxySnapshot{
		Active:      p.active.Load(),
		Opened:      p.opened.Load(),
		BlockedIPv6: p.blockedIPv6.Load(),
	}
}

func (p *connectProxy) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet && request.URL.Path == "/healthz" {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(struct {
			Status string `json:"status"`
			proxySnapshot
		}{Status: "ok", proxySnapshot: p.snapshot()})
		return
	}
	if request.Method != http.MethodConnect {
		http.Error(writer, "CONNECT required", http.StatusMethodNotAllowed)
		return
	}

	host, _, err := net.SplitHostPort(request.Host)
	if err != nil {
		http.Error(writer, "invalid target", http.StatusBadRequest)
		return
	}
	address, err := netip.ParseAddr(host)
	if err != nil {
		http.Error(writer, "IP literal required", http.StatusForbidden)
		return
	}
	if !address.Is4() {
		p.blockedIPv6.Add(1)
		http.Error(writer, "IPv6 disabled", http.StatusForbidden)
		return
	}

	upstream, upstreamReader, err := p.openUpstream(request.Context(), request.Host)
	if err != nil {
		http.Error(writer, "upstream unavailable", http.StatusBadGateway)
		return
	}
	defer upstream.Close()

	hijacker, ok := writer.(http.Hijacker)
	if !ok {
		http.Error(writer, "hijacking unavailable", http.StatusInternalServerError)
		return
	}
	client, clientBuffer, err := hijacker.Hijack()
	if err != nil {
		return
	}
	defer client.Close()
	if _, err := io.WriteString(clientBuffer, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err := clientBuffer.Flush(); err != nil {
		return
	}

	p.opened.Add(1)
	p.active.Add(1)
	defer p.active.Add(-1)
	copyTunnel(client, clientBuffer.Reader, upstream, upstreamReader)
}

func (p *connectProxy) openUpstream(ctx context.Context, target string) (net.Conn, *bufio.Reader, error) {
	connection, err := p.dialContext(ctx, "tcp", p.upstream.Host)
	if err != nil {
		return nil, nil, err
	}
	request := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: target},
		Host:   target,
		Header: make(http.Header),
	}
	request.Header.Set("Proxy-Connection", "Keep-Alive")
	if p.upstream.User != nil {
		password, _ := p.upstream.User.Password()
		credentials := p.upstream.User.Username() + ":" + password
		request.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(credentials)))
	}
	if err := request.WriteProxy(connection); err != nil {
		_ = connection.Close()
		return nil, nil, err
	}
	reader := bufio.NewReader(connection)
	response, err := http.ReadResponse(reader, request)
	if err != nil {
		_ = connection.Close()
		return nil, nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = response.Body.Close()
		_ = connection.Close()
		return nil, nil, fmt.Errorf("upstream CONNECT: %s", response.Status)
	}
	return connection, reader, nil
}

func copyTunnel(client net.Conn, clientReader io.Reader, upstream net.Conn, upstreamReader io.Reader) {
	done := make(chan struct{}, 2)
	copyDirection := func(destination net.Conn, source io.Reader) {
		_, _ = io.Copy(destination, source)
		if closer, ok := destination.(interface{ CloseWrite() error }); ok {
			_ = closer.CloseWrite()
		}
		done <- struct{}{}
	}
	go copyDirection(upstream, clientReader)
	go copyDirection(client, upstreamReader)
	<-done
	_ = client.Close()
	_ = upstream.Close()
	<-done
}

func validateUpstream(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if parsed.Scheme != "http" || parsed.Host == "" {
		return fmt.Errorf("upstream proxy must be an http URL")
	}
	if strings.Contains(parsed.Host, "/") {
		return fmt.Errorf("invalid upstream proxy host")
	}
	return nil
}
