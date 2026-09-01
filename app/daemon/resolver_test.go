package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type fakeMediaAccess struct {
	media ResolvedMedia
	err   error
	peers []string
}

func (a *fakeMediaAccess) Resolve(_ context.Context, peer string, _ int) (ResolvedMedia, error) {
	a.peers = append(a.peers, peer)
	return a.media, a.err
}

func TestNormalizePeer(t *testing.T) {
	for input, want := range map[string]string{
		"-1001234567890": "1234567890",
		"-12345":         "12345",
		"@username":      "username",
		"username":       "username",
		" 123 ":          "123",
	} {
		if got := normalizePeer(input); got != want {
			t.Fatalf("normalizePeer(%q)=%q, want %q", input, got, want)
		}
	}
}

func TestResolverPreparesMediaAndExactExistingFile(t *testing.T) {
	root := t.TempDir()
	outputRoot := filepath.Join(root, "hdd")
	request := validRequest("existing", 1)
	request.ExpectedSize = 4
	request.FinalPath = "Group/file.bin"
	final := filepath.Join(outputRoot, "Group", "file.bin")
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		t.Fatal(err)
	}
	data := []byte("data")
	if err := os.WriteFile(final, data, 0o644); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry(1, 100, nil)
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(t.Context())
	access := &fakeMediaAccess{media: ResolvedMedia{
		File: fakeDownloadFile{size: 4, dc: 5}, Name: "file.bin", Size: 4, DCID: 5,
	}}
	resolver := newTaskResolver(access, filepath.Join(root, "ssd"), outputRoot)

	element, err := resolver.Resolve(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	path, ok := element.AlreadyComplete()
	if !ok || path != request.FinalPath {
		t.Fatalf("existing result: path=%q ok=%v", path, ok)
	}
	if access.peers[0] != "1234567890" {
		t.Fatalf("peer was not normalized: %#v", access.peers)
	}
	snapshot := task.Snapshot()
	if snapshot.FileName != "file.bin" || snapshot.TotalSize != 4 || snapshot.DCID != 5 {
		t.Fatalf("resolved metadata missing: %#v", snapshot)
	}
}

func TestResolverRejectsCollision(t *testing.T) {
	root := t.TempDir()
	request := validRequest("mismatch", 1)
	request.ExpectedSize = 5
	request.FinalPath = "Group/file.bin"
	registry := NewRegistry(1, 100, nil)
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(t.Context())
	resolver := newTaskResolver(&fakeMediaAccess{media: ResolvedMedia{
		File: fakeDownloadFile{size: 4}, Name: "file.bin", Size: 4,
	}}, filepath.Join(root, "ssd"), filepath.Join(root, "hdd"))
	elem, err := resolver.Resolve(t.Context(), task)
	if err != nil {
		t.Fatalf("expected graceful size update, got %v", err)
	}
	if elem == nil || task.Snapshot().TotalSize != 4 {
		t.Fatalf("expected total size 4, got %d", task.Snapshot().TotalSize)
	}

	request = validRequest("collision", 2)
	request.ExpectedSize = 4
	request.FinalPath = "Group/file.bin"
	final := filepath.Join(root, "hdd", "Group", "file.bin")
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(final, []byte("shorter"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, _ = registry.Submit(request)
	task, _ = registry.Next(t.Context())
	resolver = newTaskResolver(&fakeMediaAccess{media: ResolvedMedia{
		File: fakeDownloadFile{size: 4}, Name: "file.bin", Size: 4,
	}}, filepath.Join(root, "ssd"), filepath.Join(root, "hdd"))
	if _, err := resolver.Resolve(t.Context(), task); err == nil || ErrorClass(err) != "collision" {
		t.Fatalf("collision returned %v", err)
	}
}

func TestResolverClassifiesUnavailableMessage(t *testing.T) {
	registry := NewRegistry(1, 100, nil)
	_, _, _ = registry.Submit(validRequest("deleted", 1))
	task, _ := registry.Next(t.Context())
	resolver := newTaskResolver(&fakeMediaAccess{err: NewTaskError("unavailable", true, errors.New("deleted"))}, t.TempDir(), t.TempDir())
	_, err := resolver.Resolve(t.Context(), task)
	if err == nil || ErrorClass(err) != "unavailable" || !IsUnavailable(err) {
		t.Fatalf("unavailable classification lost: %v", err)
	}
}
