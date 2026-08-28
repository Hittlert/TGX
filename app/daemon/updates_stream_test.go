package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"go.uber.org/zap"
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
