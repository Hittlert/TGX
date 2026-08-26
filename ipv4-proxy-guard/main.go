package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		flags := flag.NewFlagSet("healthcheck", flag.ExitOnError)
		url := flags.String("url", "http://127.0.0.1:18081/healthz", "health URL")
		_ = flags.Parse(os.Args[2:])
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := checkHealth(ctx, http.DefaultClient, *url); err != nil {
			log.Fatal(err)
		}
		return
	}

	listen := flag.String("listen", envOrDefault("LISTEN_ADDR", "0.0.0.0:18081"), "listen address")
	upstream := flag.String("upstream", os.Getenv("UPSTREAM_PROXY"), "upstream HTTP proxy URL")
	flag.Parse()
	if err := validateUpstream(*upstream); err != nil {
		log.Fatal(err)
	}

	handler := newProxy(*upstream, (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext)
	server := &http.Server{
		Addr:              *listen,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("IPv4 CONNECT guard listening on %s via %s", *listen, *upstream)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func checkHealth(ctx context.Context, client *http.Client, rawURL string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck returned %s", response.Status)
	}
	return nil
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
