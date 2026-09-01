package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Hittlert/TGX/core/bucket"
	"github.com/Hittlert/TGX/core/targetwriter"
	"github.com/Hittlert/TGX/pkg/spool"
	"github.com/Hittlert/TGX/pkg/writeback"
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

func TestLazySmallFileElement_DirectPublish(t *testing.T) {
	root := t.TempDir()
	outputRoot := filepath.Join(root, "hdd")
	content := []byte("small-image-binary-payload-data")

	registry := NewRegistry(1, 100, nil)
	request := validRequest("small", 1)
	request.ExpectedSize = int64(len(content))
	request.FinalPath = "Photos/2026_08/image.jpg"
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(t.Context())

	element, err := newLazySmallFileElement(task, fakeDownloadFile{size: int64(len(content)), dc: 4}, outputRoot, 0, nil, nil)
	require.NoError(t, err)

	_, err = element.To().WriteAt(content, 0)
	require.NoError(t, err)

	result, err := element.Publish()
	require.NoError(t, err)

	wantHash := fmt.Sprintf("%x", sha256.Sum256(content))
	assert.Equal(t, wantHash, result.SHA256)

	final, err := os.ReadFile(filepath.Join(outputRoot, filepath.FromSlash(request.FinalPath)))
	require.NoError(t, err)
	assert.Equal(t, content, final)
}

func TestBucketFileElement_BucketAndTargetWriterIntegration(t *testing.T) {
	root := t.TempDir()
	outputRoot := filepath.Join(root, "hdd")
	content := []byte("large-file-chunk-data-stream-content")

	bkt, err := bucket.New(bucket.Config{Mode: bucket.ModeMemory, MaxCapacity: 50 * 1024 * 1024})
	require.NoError(t, err)
	defer bkt.Close()

	tw := targetwriter.New(bkt, outputRoot)
	completeChan := make(chan string, 1)
	tw.SetCallbacks(func(taskID, gen, finalPath, shaHash string) {
		completeChan <- finalPath
	}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tw.Start(ctx)
	tw.BeginConsuming()
	defer tw.Close()

	registry := NewRegistry(1, 100, nil)
	request := validRequest("mover-test", 1)
	request.ExpectedSize = int64(len(content))
	request.FinalPath = "Movies/2026_08/video.mp4"
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(t.Context())

	element, err := newBucketFileElement(task, fakeDownloadFile{size: int64(len(content)), dc: 4}, outputRoot, 0, bkt, tw)
	require.NoError(t, err)

	// Network downloads chunks to bucket
	require.NoError(t, bkt.Reserve(ctx, int64(len(content))))
	_, err = element.To().WriteAt(content, 0)
	require.NoError(t, err)

	// Publish completes network phase immediately
	result, err := element.Publish()
	require.NoError(t, err)
	assert.Equal(t, request.FinalPath, result.Path)

	// Wait for TargetWriter to finish writing and atomic commit
	select {
	case finalRel := <-completeChan:
		assert.Equal(t, request.FinalPath, finalRel)
	case <-time.After(3 * time.Second):
		t.Fatal("target writer timed out")
	}

	final, err := os.ReadFile(filepath.Join(outputRoot, filepath.FromSlash(request.FinalPath)))
	require.NoError(t, err)
	assert.True(t, bytes.Equal(content, final))
}

func TestSpoolFileElement_EndToEndStreamingAndPublish(t *testing.T) {
	root := t.TempDir()
	spoolDir := filepath.Join(root, "spool")
	outputRoot := filepath.Join(root, "target")

	store, err := spool.NewFileStore(spoolDir, 50*1024*1024)
	require.NoError(t, err)
	defer store.Close()

	queue := writeback.NewQueue()
	defer queue.Close()

	completeChan := make(chan string, 1)
	cb := writeback.Callbacks{
		OnTaskFinalized: func(taskID, gen, finalRelPath, sha256Hex string, size int64, err error) {
			completeChan <- finalRelPath
		},
	}

	sink := writeback.NewTargetSink(writeback.DefaultConfig(outputRoot), store, queue, cb, nil)
	defer sink.Close()

	content := make([]byte, 512*1024)
	for i := range content {
		content[i] = byte(i % 256)
	}

	registry := NewRegistry(1, 100, nil)
	request := validRequest("spool-test", 1)
	request.ExpectedSize = int64(len(content))
	request.FinalPath = "SpoolMovies/2026_09/video.mp4"
	_, _, _ = registry.Submit(request)
	task, _ := registry.Next(t.Context())

	element, err := newSpoolFileElement(task, fakeDownloadFile{size: int64(len(content)), dc: 4}, outputRoot, 0, store, queue)
	require.NoError(t, err)

	// Network writes chunks to spool element
	_, err = element.To().WriteAt(content, 0)
	require.NoError(t, err)

	// Publish transitions to async moving
	res, err := element.Publish()
	require.NoError(t, err)
	assert.True(t, res.AsyncMoving)
	assert.Equal(t, request.FinalPath, res.Path)

	// Wait for write-back sink to commit file
	select {
	case finalizedPath := <-completeChan:
		assert.Equal(t, request.FinalPath, finalizedPath)
	case <-time.After(3 * time.Second):
		t.Fatal("target sink write-back timed out")
	}

	// Verify target file content
	finalData, err := os.ReadFile(filepath.Join(outputRoot, filepath.FromSlash(request.FinalPath)))
	require.NoError(t, err)
	assert.True(t, bytes.Equal(content, finalData))
}
