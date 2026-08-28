package main

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/gotd/td/session"

	corestorage "github.com/Hittlert/TG_Downloader/core/storage"
	"github.com/Hittlert/TG_Downloader/pkg/key"
	"github.com/Hittlert/TG_Downloader/pkg/kv"
	"github.com/Hittlert/TG_Downloader/pkg/tclient"
)

type pyrogramSession struct {
	DCID          int    `json:"dc_id"`
	AuthKeyBase64 string `json:"auth_key_base64"`
}

func main() {
	storagePath := flag.String("storage-path", "/data", "tdl bolt storage directory")
	namespace := flag.String("namespace", "default", "tdl namespace")
	flag.Parse()
	if err := importSession(context.Background(), os.Stdin, *storagePath, *namespace); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_, _ = fmt.Fprintln(os.Stdout, "session imported")
}

func importSession(ctx context.Context, input io.Reader, storagePath, namespace string) (resultErr error) {
	if strings.TrimSpace(storagePath) == "" {
		return errors.New("storage path is required")
	}
	if namespace == "" || filepath.Base(namespace) != namespace || strings.ContainsAny(namespace, "/\\\x00") {
		return errors.New("namespace must be a single path component")
	}

	var source pyrogramSession
	decoder := json.NewDecoder(io.LimitReader(input, 8*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&source); err != nil {
		return fmt.Errorf("decode input: %w", err)
	}
	if source.DCID < 1 || source.DCID > 5 {
		return fmt.Errorf("dc_id must be between 1 and 5, got %d", source.DCID)
	}
	authKey, err := base64.StdEncoding.DecodeString(source.AuthKeyBase64)
	if err != nil {
		return fmt.Errorf("decode auth key: %w", err)
	}
	if len(authKey) != 256 {
		return fmt.Errorf("auth key must be 256 bytes, got %d", len(authKey))
	}
	digest := sha1.Sum(authKey)
	data := &session.Data{
		DC:        source.DCID,
		AuthKey:   append([]byte(nil), authKey...),
		AuthKeyID: append([]byte(nil), digest[len(digest)-8:]...),
	}

	store, err := kv.NewWithMap(map[string]string{
		"type": "bolt",
		"path": storagePath,
	})
	if err != nil {
		return fmt.Errorf("open tdl storage: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, store.Close()) }()
	kvd, err := store.Open(namespace)
	if err != nil {
		return fmt.Errorf("open namespace: %w", err)
	}
	loader := &session.Loader{Storage: corestorage.NewSession(kvd, true)}
	if err := loader.Save(ctx, data); err != nil {
		return fmt.Errorf("save session: %w", err)
	}
	if err := kvd.Set(ctx, key.App(), []byte(tclient.AppBuiltin)); err != nil {
		return fmt.Errorf("save app mode: %w", err)
	}
	if err := os.Chmod(filepath.Join(storagePath, namespace), 0o600); err != nil {
		return fmt.Errorf("protect session file: %w", err)
	}
	return nil
}
