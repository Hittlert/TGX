package daemon

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Hittlert/TGX/core/mover"
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

func TestLazySmallFileElement_NoDiskOpsDuringResolveAndPublishesDirectly(t *testing.T) {
	root := t.TempDir()
	outputRoot := filepath.Join(root, "hdd")
	content := []byte("small-image-binary-payload-data")

	registry := NewRegistry(1, 100, nil)
	request := validRequest("small", 1)
	request.ExpectedSize = int64(len(content))
	request.FinalPath = "Photos/2026_08/image.jpg"
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(t.Context())

	// 1. Resolve phase: lazySmallFileElement must NOT touch disk
	element, err := newLazySmallFileElement(task, fakeDownloadFile{size: int64(len(content)), dc: 4}, outputRoot, 0)
	if err != nil {
		t.Fatal(err)
	}

	destDir := filepath.Join(outputRoot, "Photos", "2026_08")
	if _, err := os.Stat(destDir); !os.IsNotExist(err) {
		t.Fatalf("destination directory created prematurely during resolve: %v", err)
	}

	// 2. Download phase: writes to memory buffer
	if _, err := element.To().WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}

	// 3. Publish phase: single serial flush, sync, rename, and memory hash
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

func TestFileElement_MoverIntegration(t *testing.T) {
	root := t.TempDir()
	bufRoot := filepath.Join(root, "buffer")
	outputRoot := filepath.Join(root, "hdd")
	content := []byte("large-file-chunk-data-stream-content")

	m := mover.New(1, 100*1024*1024)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m.Start(ctx)
	defer m.Close()

	registry := NewRegistry(1, 100, nil)
	request := validRequest("mover-test", 1)
	request.ExpectedSize = int64(len(content))
	request.FinalPath = "Movies/2026_08/video.mp4"
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(t.Context())

	// Create fileElement with staging tempRoot and mover
	element, err := newFileElement(task, fakeDownloadFile{size: int64(len(content)), dc: 4}, bufRoot, outputRoot, 0, m)
	if err != nil {
		t.Fatal(err)
	}

	// Write content to buffer
	if _, err := element.To().WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}

	// Pre-reserve capacity
	require.True(t, m.Reserve(int64(len(content))))
	assert.Equal(t, int64(len(content)), m.UsedBytes())

	// Publish delegates to mover
	result, err := element.Publish()
	require.NoError(t, err)

	wantHash := fmt.Sprintf("%x", sha256.Sum256(content))
	assert.Equal(t, wantHash, result.SHA256)
	assert.Equal(t, request.FinalPath, result.Path)

	// Verify target disk file
	final, err := os.ReadFile(filepath.Join(outputRoot, filepath.FromSlash(request.FinalPath)))
	require.NoError(t, err)
	assert.Equal(t, content, final)

	// Verify buffer file was removed
	_, err = os.Stat(element.tempPath)
	assert.True(t, os.IsNotExist(err))

	// Verify buffer capacity was automatically released
	assert.Equal(t, int64(0), m.UsedBytes())
}

func TestLazySmallFileElement_MoverIntegration(t *testing.T) {
	root := t.TempDir()
	outputRoot := filepath.Join(root, "hdd")
	content := []byte("small-image-mover-data")

	m := mover.New(1, 100*1024*1024)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m.Start(ctx)
	defer m.Close()

	registry := NewRegistry(1, 100, nil)
	request := validRequest("small-mover", 1)
	request.ExpectedSize = int64(len(content))
	request.FinalPath = "Photos/2026_08/photo.jpg"
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(t.Context())

	element, err := newLazySmallFileElement(task, fakeDownloadFile{size: int64(len(content)), dc: 4}, outputRoot, 0, m)
	require.NoError(t, err)

	// Write to memory
	_, err = element.To().WriteAt(content, 0)
	require.NoError(t, err)

	// Publish delegates to mover
	result, err := element.Publish()
	require.NoError(t, err)

	wantHash := fmt.Sprintf("%x", sha256.Sum256(content))
	assert.Equal(t, wantHash, result.SHA256)

	// Verify target disk file
	final, err := os.ReadFile(filepath.Join(outputRoot, filepath.FromSlash(request.FinalPath)))
	require.NoError(t, err)
	assert.Equal(t, content, final)
}
