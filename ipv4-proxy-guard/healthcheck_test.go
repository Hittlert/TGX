package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestHealthcheckRequiresSuccessfulResponse(t *testing.T) {
	for _, test := range []struct {
		name       string
		statusCode int
		wantError  bool
	}{
		{name: "healthy", statusCode: http.StatusOK},
		{name: "unhealthy", statusCode: http.StatusServiceUnavailable, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: test.statusCode,
					Status:     fmt.Sprintf("%d test", test.statusCode),
					Body:       io.NopCloser(strings.NewReader("status")),
					Header:     make(http.Header),
					Request:    request,
				}, nil
			})}
			err := checkHealth(context.Background(), client, "http://127.0.0.1:18081/healthz")
			if (err != nil) != test.wantError {
				t.Fatalf("checkHealth() error = %v, wantError %v", err, test.wantError)
			}
		})
	}
}

func TestValidateUpstreamRequiresHTTPProxy(t *testing.T) {
	for _, raw := range []string{"", "https://127.0.0.1:6152", "127.0.0.1:6152"} {
		if err := validateUpstream(raw); err == nil {
			t.Fatalf("validateUpstream(%q) accepted", raw)
		}
	}
	if err := validateUpstream("http://127.0.0.1:6152"); err != nil {
		t.Fatalf("valid upstream rejected: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
