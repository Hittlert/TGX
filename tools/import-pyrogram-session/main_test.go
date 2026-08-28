package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/session"

	corestorage "github.com/Hittlert/TG_Downloader/core/storage"
	"github.com/Hittlert/TG_Downloader/pkg/key"
	"github.com/Hittlert/TG_Downloader/pkg/kv"
	"github.com/Hittlert/TG_Downloader/pkg/tclient"
)

func TestImportSessionWritesTDLStorage(t *testing.T) {
	authKey := make([]byte, 256)
	for i := range authKey {
		authKey[i] = byte(i)
	}
	input := []byte(`{"dc_id":1,"auth_key_base64":"` + base64.StdEncoding.EncodeToString(authKey) + `"}`)
	storagePath := t.TempDir()

	if err := importSession(context.Background(), bytes.NewReader(input), storagePath, "default"); err != nil {
		t.Fatal(err)
	}

	store, err := kv.NewWithMap(map[string]string{"type": "bolt", "path": storagePath})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	namespace, err := store.Open("default")
	if err != nil {
		t.Fatal(err)
	}
	data, err := (&session.Loader{Storage: corestorage.NewSession(namespace, false)}).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if data.DC != 1 || !bytes.Equal(data.AuthKey, authKey) {
		t.Fatalf("unexpected imported session: dc=%d key_len=%d", data.DC, len(data.AuthKey))
	}
	var gotdKey crypto.Key
	copy(gotdKey[:], authKey)
	expectedID := gotdKey.WithID().ID
	if !bytes.Equal(data.AuthKeyID, expectedID[:]) {
		t.Fatalf("unexpected auth key ID: %x", data.AuthKeyID)
	}
	app, err := namespace.Get(context.Background(), key.App())
	if err != nil {
		t.Fatal(err)
	}
	if string(app) != tclient.AppBuiltin {
		t.Fatalf("unexpected app mode: %q", app)
	}
	info, err := os.Stat(filepath.Join(storagePath, "default"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("session permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestImportSessionRejectsInvalidInput(t *testing.T) {
	tests := []string{
		`{"dc_id":0,"auth_key_base64":"` + base64.StdEncoding.EncodeToString(make([]byte, 256)) + `"}`,
		`{"dc_id":1,"auth_key_base64":"` + base64.StdEncoding.EncodeToString(make([]byte, 255)) + `"}`,
	}
	for _, input := range tests {
		if err := importSession(context.Background(), bytes.NewBufferString(input), t.TempDir(), "default"); err == nil {
			t.Fatalf("expected invalid input to fail: %s", input)
		}
	}
}
