package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProxyRejectsIPv6WithoutDialingUpstream(t *testing.T) {
	var dials atomic.Int32
	proxy := newProxy("http://127.0.0.1:1", func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, fmt.Errorf("unexpected dial")
	})

	request := httptest.NewRequest(http.MethodConnect, "http://[2001:b28:f23d:f001::a]:443", nil)
	request.Host = "[2001:b28:f23d:f001::a]:443"
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
	if got := dials.Load(); got != 0 {
		t.Fatalf("upstream dials = %d, want 0", got)
	}
	if got := proxy.snapshot(); got.BlockedIPv6 != 1 || got.Active != 0 {
		t.Fatalf("snapshot = %#v", got)
	}
}

func TestProxyTunnelsIPv4ThroughUpstream(t *testing.T) {
	upstream, target := startEchoUpstream(t)
	proxy := newProxy("http://"+upstream, (&net.Dialer{Timeout: time.Second}).DialContext)
	server := httptest.NewServer(proxy)
	t.Cleanup(server.Close)

	client, err := net.DialTimeout("tcp", strings.TrimPrefix(server.URL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if _, err := fmt.Fprintf(client, "CONNECT 149.154.175.50:443 HTTP/1.1\r\nHost: 149.154.175.50:443\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(client)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}
	if _, err := client.Write([]byte("telegram")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len("telegram"))
	if _, err := io.ReadFull(reader, buffer); err != nil {
		t.Fatal(err)
	}
	if string(buffer) != "telegram" {
		t.Fatalf("echo = %q", buffer)
	}
	if got := <-target; got != "149.154.175.50:443" {
		t.Fatalf("upstream target = %q", got)
	}

	deadline := time.Now().Add(time.Second)
	for proxy.snapshot().Opened != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := proxy.snapshot(); got.Opened != 1 || got.BlockedIPv6 != 0 {
		t.Fatalf("snapshot = %#v", got)
	}
}

func TestProxyDoesNotHangOnUpstreamConnectRejection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer connection.Close()
		reader := bufio.NewReader(connection)
		if _, err := http.ReadRequest(reader); err != nil {
			return
		}
		_, _ = io.WriteString(connection, "HTTP/1.1 403 Forbidden\r\n\r\n")
		<-release
	}()

	proxy := newProxy("http://"+listener.Addr().String(), (&net.Dialer{Timeout: time.Second}).DialContext)
	request := httptest.NewRequest(http.MethodConnect, "http://149.154.175.50:443", nil)
	request.Host = "149.154.175.50:443"
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		proxy.ServeHTTP(response, request)
		close(done)
	}()

	select {
	case <-done:
		if response.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want %d", response.Code, http.StatusBadGateway)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("proxy hung while closing rejected upstream CONNECT")
	}
}

func TestHealthReportsConnectionCounters(t *testing.T) {
	proxy := newProxy("http://127.0.0.1:1", (&net.Dialer{}).DialContext)
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()

	proxy.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("content type = %q", contentType)
	}
	if body := response.Body.String(); !strings.Contains(body, `"status":"ok"`) {
		t.Fatalf("body = %q", body)
	}
}

func startEchoUpstream(t *testing.T) (string, <-chan string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	target := make(chan string, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer connection.Close()
		reader := bufio.NewReader(connection)
		request, err := http.ReadRequest(reader)
		if err != nil {
			return
		}
		target <- request.Host
		_, _ = io.WriteString(connection, "HTTP/1.1 200 Connection Established\r\n\r\n")
		_, _ = io.Copy(connection, reader)
	}()
	return listener.Addr().String(), target
}
