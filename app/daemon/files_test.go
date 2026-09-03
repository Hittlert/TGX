package daemon

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestFileElementPublishesVerifiedContentAndHash(t *testing.T) {
	root := t.TempDir()
	tempRoot := filepath.Join(root, "ssd")
	outputRoot := filepath.Join(root, "hdd")
	content := []byte("0123456789abcdef")
	registry := NewRegistry(1, 100, nil)
	request := validRequest("publish", 1)
	request.ExpectedSize = int64(len(content))
	request.FinalPath = "Group/2026_07/file.bin"
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(t.Context())

	element, err := newFileElement(task, fakeDownloadFile{size: int64(len(content)), dc: 4}, tempRoot, outputRoot, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := element.To().WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}
	result, err := element.Publish()
	if err != nil {
		t.Fatal(err)
	}
	wantHash := fmt.Sprintf("%x", sha256.Sum256(content))
	if result.Path != request.FinalPath || result.SHA256 != wantHash || result.AlreadyExists {
		t.Fatalf("unexpected publish result: %#v", result)
	}
	final, err := os.ReadFile(filepath.Join(outputRoot, filepath.FromSlash(request.FinalPath)))
	if err != nil {
		t.Fatal(err)
	}
	if string(final) != string(content) {
		t.Fatalf("published bytes=%q, want %q", final, content)
	}
}

func TestFileElementRejectsShortTempWithoutVisibleFinal(t *testing.T) {
	root := t.TempDir()
	tempRoot := filepath.Join(root, "ssd")
	outputRoot := filepath.Join(root, "hdd")
	registry := NewRegistry(1, 100, nil)
	request := validRequest("short", 1)
	request.ExpectedSize = 10
	request.FinalPath = "Group/2026_07/file.bin"
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(t.Context())

	element, err := newFileElement(task, fakeDownloadFile{size: 10, dc: 4}, tempRoot, outputRoot, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := element.To().WriteAt([]byte("1234"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := element.Publish(); err == nil {
		t.Fatal("expected publish failure on short temp file")
	}
	finalPath := filepath.Join(outputRoot, filepath.FromSlash(request.FinalPath))
	if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
		t.Fatalf("corrupt final exists: %v", err)
	}
}

func TestFileElementNeverOverwritesCollision(t *testing.T) {
	root := t.TempDir()
	tempRoot := filepath.Join(root, "ssd")
	outputRoot := filepath.Join(root, "hdd")
	content := []byte("0123456789")
	finalPath := filepath.Join(outputRoot, "Group", "2026_07", "file.bin")
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(finalPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	registry := NewRegistry(1, 100, nil)
	request := validRequest("collision", 1)
	request.ExpectedSize = int64(len(content))
	request.FinalPath = "Group/2026_07/file.bin"
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(t.Context())

	element, err := newFileElement(task, fakeDownloadFile{size: int64(len(content)), dc: 4}, tempRoot, outputRoot, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := element.To().WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}
	result, err := element.Publish()
	if err != nil {
		t.Fatal(err)
	}
	if !result.AlreadyExists {
		t.Fatal("expected AlreadyExists true")
	}
}

func TestExistingFileRequiresExactSize(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "file.bin")
	if err := os.WriteFile(path, []byte("1234"), 0o644); err != nil {
		t.Fatal(err)
	}
	if ok, err := existingFile(path, 4); err != nil || !ok {
		t.Fatalf("exact existing file: ok=%v err=%v", ok, err)
	}
	if ok, err := existingFile(path, 5); err == nil || ok {
		t.Fatalf("short collision accepted: ok=%v err=%v", ok, err)
	}
	if ok, err := existingFile(filepath.Join(root, "missing"), 4); err != nil || ok {
		t.Fatalf("missing path: ok=%v err=%v", ok, err)
	}
}

