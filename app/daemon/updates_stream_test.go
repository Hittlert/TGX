package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/dcpool"
	"github.com/Hittlert/TGX/core/transfer"
	"github.com/Hittlert/TGX/internal/fscommit"
)

func TestUpdatesStream_RealTimeMessageHandling(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.sqlite3")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Insert test listen target
	now := time.Now().Unix()
	_, err = db.DB().Exec(`
		INSERT INTO listen_targets(chat_id, enabled, title, username, chat_type, download_filter, created_at, updated_at)
		VALUES(?, 1, ?, ?, 'channel', '', ?, ?)
	`, "-1001234567890", "Test Channel", "test_channel", now, now)
	if err != nil {
		t.Fatalf("failed to insert target: %v", err)
	}

	logger := zap.NewNop()
	stream := NewUpdatesStream(db, nil, logger)

	// Simulate incoming tg.Message with Document
	msg := &tg.Message{
		ID:     9988,
		PeerID: &tg.PeerChannel{ChannelID: 1234567890},
		Date:   int(now),
		Media: &tg.MessageMediaDocument{
			Document: &tg.Document{
				ID:       11223344,
				MimeType: "video/mp4",
				Size:     1024 * 1024 * 50, // 50MB
				Attributes: []tg.DocumentAttributeClass{
					&tg.DocumentAttributeFilename{FileName: "demo_video.mp4"},
					&tg.DocumentAttributeVideo{Duration: 120, W: 1920, H: 1080},
				},
			},
		},
	}

	stream.handleMessage(context.Background(), msg, tg.Entities{})

	// Verify message was ingested in database
	var count int
	err = db.DB().QueryRow(`SELECT COUNT(*) FROM download_records WHERE chat_id = '-1001234567890' AND message_id = 9988`).Scan(&count)
	if err != nil || count != 1 {
		t.Fatalf("expected 1 record in download_records, got %d (err: %v)", count, err)
	}

	var status string
	var fileName string
	var fileSize int64
	err = db.DB().QueryRow(`SELECT status, file_name, file_size FROM download_records WHERE chat_id = '-1001234567890' AND message_id = 9988`).Scan(&status, &fileName, &fileSize)
	if err != nil {
		t.Fatalf("failed to query download record: %v", err)
	}

	if status != "pending" || fileName != "demo_video.mp4" || fileSize != 1024*1024*50 {
		t.Fatalf("unexpected record: status=%s, fileName=%s, fileSize=%d", status, fileName, fileSize)
	}
}

func TestUpdatesStream_FilterRejection(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.sqlite3")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	// Target only accepts mp4
	_, err = db.DB().Exec(`
		INSERT INTO listen_targets(chat_id, enabled, title, username, chat_type, download_filter, created_at, updated_at)
		VALUES(?, 1, ?, ?, 'channel', 'Media.Name contains ".mp4"', ?, ?)
	`, "-100111222333", "Filtered Channel", "filtered", now, now)
	if err != nil {
		t.Fatalf("failed to insert target: %v", err)
	}

	logger := zap.NewNop()
	stream := NewUpdatesStream(db, nil, logger)

	// Send a zip file message
	msg := &tg.Message{
		ID:     5566,
		PeerID: &tg.PeerChannel{ChannelID: 111222333},
		Date:   int(now),
		Media: &tg.MessageMediaDocument{
			Document: &tg.Document{
				ID:       99999,
				MimeType: "application/zip",
				Size:     5000,
				Attributes: []tg.DocumentAttributeClass{
					&tg.DocumentAttributeFilename{FileName: "archive.zip"},
				},
			},
		},
	}

	stream.handleMessage(context.Background(), msg, tg.Entities{})

	// Verify message was filtered out and NOT added to download_records
	var count int
	err = db.DB().QueryRow(`SELECT COUNT(*) FROM download_records WHERE chat_id = '-100111222333' AND message_id = 5566`).Scan(&count)
	if err != nil || count != 0 {
		t.Fatalf("expected 0 records for filtered message, got %d", count)
	}
}

func TestUpdatesStream_ChannelMetadataUpdate(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.sqlite3")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	_, err = db.DB().Exec(`
		INSERT INTO listen_targets(chat_id, enabled, title, username, chat_type, download_filter, created_at, updated_at)
		VALUES(?, 1, ?, ?, 'channel', '', ?, ?)
	`, "-100777888999", "Old Title", "old_user", now, now)
	if err != nil {
		t.Fatalf("failed to insert target: %v", err)
	}

	logger := zap.NewNop()
	stream := NewUpdatesStream(db, nil, logger)

	entities := tg.Entities{
		Channels: map[int64]*tg.Channel{
			777888999: {
				ID:       777888999,
				Title:    "New Title",
				Username: "new_username",
			},
		},
	}

	stream.handleChannelUpdate(context.Background(), 777888999, entities)

	var title, username string
	err = db.DB().QueryRow(`SELECT title, username FROM listen_targets WHERE chat_id = '-100777888999'`).Scan(&title, &username)
	if err != nil {
		t.Fatalf("failed to query updated target: %v", err)
	}

	if title != "New Title" || username != "new_username" {
		t.Fatalf("metadata not updated in stream: title=%s, username=%s", title, username)
	}
}

type mockPoolInvoker struct {
	data []byte
}

func (m *mockPoolInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	req, ok := input.(*tg.UploadGetFileRequest)
	if !ok {
		return nil
	}
	offset := int(req.Offset)
	limit := req.Limit
	if offset >= len(m.data) {
		buf := &bin.Buffer{}
		emptyFile := &tg.UploadFile{
			Type:  &tg.StorageFilePartial{},
			Bytes: []byte{},
		}
		if err := emptyFile.Encode(buf); err != nil {
			return err
		}
		return output.Decode(buf)
	}
	end := offset + limit
	if end > len(m.data) {
		end = len(m.data)
	}
	chunk := m.data[offset:end]
	buf := &bin.Buffer{}
	uploadFile := &tg.UploadFile{
		Type:  &tg.StorageFilePartial{},
		Bytes: chunk,
	}
	if err := uploadFile.Encode(buf); err != nil {
		return err
	}
	return output.Decode(buf)
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

type mockE2EPool struct {
	invoker tg.Invoker
}

func (p *mockE2EPool) Client(ctx context.Context, dc int) *tg.Client { return tg.NewClient(p.invoker) }
func (p *mockE2EPool) Takeout(ctx context.Context, dc int) *tg.Client { return tg.NewClient(p.invoker) }
func (p *mockE2EPool) TakeoutInvoker(ctx context.Context, dc int) tg.Invoker { return p.invoker }
func (p *mockE2EPool) Default(ctx context.Context) *tg.Client { return tg.NewClient(p.invoker) }
func (p *mockE2EPool) Invoker(ctx context.Context, dc int) tg.Invoker { return p.invoker }
func (p *mockE2EPool) DefaultInvoker(ctx context.Context) tg.Invoker { return p.invoker }
func (p *mockE2EPool) CDN(ctx context.Context, dc int, max int64) (tg.Invoker, io.Closer, error) {
	return p.invoker, nopCloser{}, nil
}
func (p *mockE2EPool) Close() error { return nil }

type mockMediaFile struct {
	loc tg.InputFileLocationClass
	sz  int64
	dc  int
}

func (m mockMediaFile) Location() tg.InputFileLocationClass { return m.loc }
func (m mockMediaFile) Size() int64                         { return m.sz }
func (m mockMediaFile) DC() int                             { return m.dc }

type mockTelegramAccess struct {
	pool    dcpool.Pool
	payload []byte
}

func (m *mockTelegramAccess) GetDialogs(ctx context.Context) ([]DialogDTO, error) { return nil, nil }
func (m *mockTelegramAccess) GetHistory(ctx context.Context, req HistoryRequest) ([]MessageDTO, error) {
	return nil, nil
}
func (m *mockTelegramAccess) ResolvePeerInfo(ctx context.Context, queryStr string) (DialogDTO, error) {
	return DialogDTO{}, nil
}
func (m *mockTelegramAccess) Resolve(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
	return ResolvedMedia{
		Name: "sample_video.mp4",
		Size: int64(len(m.payload)),
		DCID: 2,
		Date: 1725400000,
		File: mockMediaFile{
			loc: &tg.InputDocumentFileLocation{
				ID:         12345,
				AccessHash: 67890,
			},
			sz: int64(len(m.payload)),
			dc: 2,
		},
	}, nil
}
func (m *mockTelegramAccess) ResolveBatch(ctx context.Context, peer string, messageIDs []int) (map[int]ResolvedMedia, error) {
	return nil, nil
}
func (m *mockTelegramAccess) SyncPeers(ctx context.Context) error { return nil }
func (m *mockTelegramAccess) Pool() dcpool.Pool                  { return m.pool }

func TestUpdatesStream_EndToEnd_Pipeline(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.sqlite3")
	saveDir := filepath.Join(tempDir, "downloads")
	if err := os.MkdirAll(saveDir, 0o755); err != nil {
		t.Fatalf("failed to create saveDir: %v", err)
	}

	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	now := time.Now().Unix()
	_, err = db.DB().Exec(`
		INSERT INTO listen_targets(chat_id, enabled, title, username, chat_type, download_filter, created_at, updated_at)
		VALUES(?, 1, ?, ?, 'channel', '', ?, ?)
	`, "-1001234567890", "Test Channel", "test_channel", now, now)
	if err != nil {
		t.Fatalf("failed to insert target: %v", err)
	}

	payload := []byte("Hello TGX direct SSD download e2e test payload! Verification of single invariant owner flow.")
	hasher := sha256.New()
	hasher.Write(payload)
	expectedSHA := hex.EncodeToString(hasher.Sum(nil))

	pool := &mockE2EPool{invoker: &mockPoolInvoker{data: payload}}
	access := &mockTelegramAccess{pool: pool, payload: payload}

	registry := NewRegistry(100, 100, time.Now)
	transferMgr := transfer.NewTransferManager(transfer.Options{
		FileConcurrency: 2,
		MaxFileThreads:  2,
		MaxDataInFlight: 10,
	})
	ssdAdmission := fscommit.NewSSDAdmission(saveDir, 100*1024*1024)

	orch := NewOrchestrator(db, transferMgr, ssdAdmission, nil, access, registry, zap.NewNop(), saveDir)
	archiveDir := filepath.Join(tempDir, "archive")
	_ = os.MkdirAll(archiveDir, 0o755)
	archiveWorker, _ := NewArchiveWorker(db, saveDir, archiveDir, zap.NewNop())
	orch.SetArchiveWorker(archiveWorker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	orch.Start(ctx)
	stream := NewUpdatesStream(db, orch, zap.NewNop())

	// Simulate incoming tg.Message with Document and no pre-planned path
	msg := &tg.Message{
		ID:     8877,
		PeerID: &tg.PeerChannel{ChannelID: 1234567890},
		Date:   int(now),
		Media: &tg.MessageMediaDocument{
			Document: &tg.Document{
				ID:       554433,
				MimeType: "video/mp4",
				Size:     int64(len(payload)),
				Attributes: []tg.DocumentAttributeClass{
					&tg.DocumentAttributeFilename{FileName: "sample_video.mp4"},
				},
			},
		},
	}

	stream.handleMessage(ctx, msg, tg.Entities{})

	// Await task terminal state
	deadline := time.Now().Add(5 * time.Second)
	var finalRec *DownloadRecord
	for time.Now().Before(deadline) {
		rec, recErr := db.GetDownloadRecord("-1001234567890", 8877)
		if recErr == nil && rec != nil && rec.Status == "success" {
			finalRec = rec
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalRec == nil {
		rec, _ := db.GetDownloadRecord("-1001234567890", 8877)
		t.Fatalf("expected download record to reach 'success' status within deadline, actual: %#v", rec)
	}

	if finalRec.SHA256 != expectedSHA {
		t.Fatalf("record SHA256 mismatch: got %s, want %s", finalRec.SHA256, expectedSHA)
	}

	// Verify file on SSD exists and content matches
	diskPath := filepath.Join(saveDir, filepath.FromSlash(finalRec.SavePath))
	data, readErr := os.ReadFile(diskPath)
	if readErr != nil {
		t.Fatalf("failed to read downloaded file on SSD: %v", readErr)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("file content on disk does not match payload")
	}

	// Verify .part file has been removed
	if _, statErr := os.Stat(diskPath + ".part"); !os.IsNotExist(statErr) {
		t.Fatalf(".part file still exists on SSD")
	}

	// Verify archive job was queued
	jobs, jobsErr := db.GetDueArchiveJobs(10)
	if jobsErr != nil {
		t.Fatalf("failed to query archive jobs: %v", jobsErr)
	}
	if len(jobs) != 1 || jobs[0].MessageID != 8877 {
		t.Fatalf("expected 1 archive job for message 8877, got %v", jobs)
	}
}

