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
	if _, err := element.To().WriteAt(content[8:], 8); err != nil {
		t.Fatal(err)
	}
	if _, err := element.To().WriteAt(content[:8], 0); err != nil {
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
	if _, err := os.Stat(element.tempPath); !os.IsNotExist(err) {
		t.Fatalf("SSD temp still exists: %v", err)
	}
	stages, err := filepath.Glob(filepath.Join(filepath.Dir(result.absolutePath), ".tdl-stage-*"))
	if err != nil || len(stages) != 0 {
		t.Fatalf("HDD stages leaked: %v %#v", err, stages)
	}
}

func TestFileElementRejectsShortTempWithoutVisibleFinal(t *testing.T) {
	root := t.TempDir()
	registry := NewRegistry(1, 100, nil)
	request := validRequest("short", 1)
	request.ExpectedSize = 16
	request.FinalPath = "Group/short.bin"
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(t.Context())
	element, err := newFileElement(task, fakeDownloadFile{size: 16}, filepath.Join(root, "ssd"), filepath.Join(root, "hdd"), 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = element.To().WriteAt([]byte("short"), 0)
	if _, err := element.Publish(); err == nil {
		t.Fatal("short temporary file was published")
	}
	if _, err := os.Stat(filepath.Join(root, "hdd", "Group", "short.bin")); !os.IsNotExist(err) {
		t.Fatalf("partial final became visible: %v", err)
	}
}

func TestFileElementNeverOverwritesCollision(t *testing.T) {
	root := t.TempDir()
	outputRoot := filepath.Join(root, "hdd")
	finalPath := filepath.Join(outputRoot, "Group", "collision.bin")
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(finalPath, []byte("original!"), 0o644); err != nil {
		t.Fatal(err)
	}

	registry := NewRegistry(1, 100, nil)
	request := validRequest("collision", 1)
	request.ExpectedSize = 8
	request.FinalPath = "Group/collision.bin"
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(t.Context())
	element, err := newFileElement(task, fakeDownloadFile{size: 8}, filepath.Join(root, "ssd"), outputRoot, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = element.To().WriteAt([]byte("new-data"), 0)
	if _, err := element.Publish(); err == nil {
		t.Fatal("existing final file was overwritten")
	}
	content, _ := os.ReadFile(finalPath)
	if string(content) != "original!" {
		t.Fatalf("collision changed existing bytes: %q", content)
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
