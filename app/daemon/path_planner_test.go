package daemon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPathPlanner_PeerTypes(t *testing.T) {
	p := PathPlanner{}
	fixedDate := int64(1725148800) // 2024-09-01 00:00:00 UTC -> 2024_09

	tests := []struct {
		name         string
		peer         string
		title        string
		msgID        int
		rawName      string
		mediaType    string
		expectedPath string
	}{
		{
			name:         "Channel",
			peer:         "-1001111111",
			title:        "News Channel",
			msgID:        10,
			rawName:      "report.pdf",
			mediaType:    "document",
			expectedPath: "News Channel/2024_09/10 - report.pdf",
		},
		{
			name:         "Supergroup",
			peer:         "-1002222222",
			title:        "Tech Community",
			msgID:        20,
			rawName:      "code.go",
			mediaType:    "document",
			expectedPath: "Tech Community/2024_09/20 - code.go",
		},
		{
			name:         "Group",
			peer:         "-33333333",
			title:        "Study Group",
			msgID:        30,
			rawName:      "lecture.mp4",
			mediaType:    "video",
			expectedPath: "Study Group/2024_09/30 - lecture.mp4",
		},
		{
			name:         "Bot",
			peer:         "8844705144",
			title:        "Memento Bot",
			msgID:        40,
			rawName:      "archive.zip",
			mediaType:    "document",
			expectedPath: "Memento Bot/2024_09/40 - archive.zip",
		},
		{
			name:         "Private User (empty title falls back to peer ID)",
			peer:         "12345678",
			title:        "",
			msgID:        50,
			rawName:      "notes.txt",
			mediaType:    "document",
			expectedPath: "12345678/2024_09/50 - notes.txt",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := p.Plan(tc.peer, tc.title, tc.msgID, tc.rawName, tc.mediaType, fixedDate)
			if got != tc.expectedPath {
				t.Fatalf("expected path %q, got %q", tc.expectedPath, got)
			}
		})
	}
}

func TestPathPlanner_MediaExtensions(t *testing.T) {
	p := PathPlanner{}
	fixedDate := int64(1725148800) // 2024_09

	tests := []struct {
		name         string
		mediaType    string
		rawName      string
		msgID        int
		expectedPath string
	}{
		{
			name:         "Unnamed Photo -> .jpg",
			mediaType:    "photo",
			rawName:      "",
			msgID:        101,
			expectedPath: "Target/2024_09/101 - 101.jpg",
		},
		{
			name:         "Unnamed Audio -> .mp3",
			mediaType:    "audio",
			rawName:      "",
			msgID:        102,
			expectedPath: "Target/2024_09/102 - 102.mp3",
		},
		{
			name:         "Unnamed Video -> .mp4",
			mediaType:    "video",
			rawName:      "",
			msgID:        103,
			expectedPath: "Target/2024_09/103 - 103.mp4",
		},
		{
			name:         "Unnamed Document -> .bin (never .mp4!)",
			mediaType:    "document",
			rawName:      "",
			msgID:        104,
			expectedPath: "Target/2024_09/104 - 104.bin",
		},
		{
			name:         "Named Document preserves real extension",
			mediaType:    "document",
			rawName:      "data.tar.gz",
			msgID:        105,
			expectedPath: "Target/2024_09/105 - data.tar.gz",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := p.Plan("123", "Target", tc.msgID, tc.rawName, tc.mediaType, fixedDate)
			if got != tc.expectedPath {
				t.Fatalf("expected %q, got %q", tc.expectedPath, got)
			}
		})
	}
}

func TestPathPlanner_EmptyAndUnknownFilenameFallback(t *testing.T) {
	p := PathPlanner{}
	fixedDate := int64(1725148800)

	tests := []struct {
		name         string
		rawName      string
		mediaType    string
		msgID        int
		expectedPath string
	}{
		{
			name:         "Empty string",
			rawName:      "",
			mediaType:    "unknown",
			msgID:        201,
			expectedPath: "Chat/2024_09/201 - 201.bin",
		},
		{
			name:         "Single dot",
			rawName:      ".",
			mediaType:    "unknown",
			msgID:        202,
			expectedPath: "Chat/2024_09/202 - 202.bin",
		},
		{
			name:         "Default msgID.bin",
			rawName:      "203.bin",
			mediaType:    "photo",
			msgID:        203,
			expectedPath: "Chat/2024_09/203 - 203.jpg",
		},
		{
			name:         "Unknown extension suffix",
			rawName:      "sample.unknown",
			mediaType:    "audio",
			msgID:        204,
			expectedPath: "Chat/2024_09/204 - 204.mp3",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := p.Plan("123", "Chat", tc.msgID, tc.rawName, tc.mediaType, fixedDate)
			if got != tc.expectedPath {
				t.Fatalf("expected %q, got %q", tc.expectedPath, got)
			}
		})
	}
}

func TestPathPlanner_InvalidCharactersAndTraversal(t *testing.T) {
	p := PathPlanner{}
	fixedDate := int64(1725148800)

	// Directory traversal attempt in title
	t.Run("Title traversal attempt", func(t *testing.T) {
		got := p.Plan("123", "../../../etc/passwd", 301, "safe.txt", "document", fixedDate)
		if strings.HasPrefix(got, "..") || strings.HasPrefix(got, "/") || strings.Contains(got, "/../") {
			t.Fatalf("path escaped root: %q", got)
		}
	})

	// Traversal in file name
	t.Run("Filename traversal attempt", func(t *testing.T) {
		got := p.Plan("123", "MyChannel", 302, "../../etc/shadow", "document", fixedDate)
		if strings.Contains(got, "/../") || strings.HasPrefix(got, "..") {
			t.Fatalf("filename traversal escaped: %q", got)
		}
	})

	// Forbidden filename characters: \ / : * ? " < > |
	t.Run("Forbidden characters sanitization", func(t *testing.T) {
		got := p.Plan("123", "My:Channel*Name?", 303, "file<name>|\".pdf", "document", fixedDate)
		for _, ch := range []string{":", "*", "?", "<", ">", "|", "\""} {
			if strings.Contains(got, ch) {
				t.Fatalf("forbidden character %q still in planned path: %q", ch, got)
			}
		}
	})
}

func TestPathPlanner_MonthBoundaryAndRestartDeterminism(t *testing.T) {
	p := PathPlanner{}

	// 2023-12-31 23:59:59 UTC -> 2023_12
	t1 := int64(1704067199)
	path1 := p.Plan("100", "Channel", 401, "video.mp4", "video", t1)
	if !strings.Contains(path1, "2023_12") {
		t.Fatalf("expected 2023_12, got %q", path1)
	}

	// 2024-01-01 00:00:00 UTC -> 2024_01
	t2 := int64(1704067200)
	path2 := p.Plan("100", "Channel", 401, "video.mp4", "video", t2)
	if !strings.Contains(path2, "2024_01") {
		t.Fatalf("expected 2024_01, got %q", path2)
	}

	// Restart determinism: 100 runs must be byte-for-byte identical
	for i := 0; i < 100; i++ {
		recheck := p.Plan("100", "Channel", 401, "video.mp4", "video", t1)
		if recheck != path1 {
			t.Fatalf("path plan not deterministic on run %d: %q vs %q", i, recheck, path1)
		}
	}
}

func TestPathPlanner_RealtimeAndDBScanEquivalence(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("new db: %v", err)
	}
	defer db.Close()

	msgDate := int64(1725148800)
	chatID := "-100987654321"
	targetTitle := "Archive Vault"

	// 1. Setup target
	_, _ = db.Execute(`
		INSERT INTO listen_targets (chat_id, enabled, title, username, priority, created_at, updated_at)
		VALUES (?, 1, ?, 'vault', 10, ?, ?)
	`, chatID, targetTitle, msgDate, msgDate)

	// 2. Realtime stream simulated record
	realtimeRec := DownloadRecord{
		ChatID:      chatID,
		MessageID:   501,
		Status:      "pending",
		FileName:    "report.pdf",
		MediaType:   "document",
		FileSize:    2048,
		Date:        msgDate,
		CreatedAt:   msgDate + 5,
		TargetTitle: targetTitle,
	}

	// 3. DB Scan record simulated via chat_messages + download_records
	_, _ = db.Execute(`
		INSERT INTO chat_messages (chat_id, message_id, text, media_type, has_media, date, created_at, updated_at)
		VALUES (?, 501, '', 'document', 1, ?, ?, ?)
	`, chatID, msgDate, msgDate+1, msgDate+1)

	_, _ = db.Execute(`
		INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, media_type, file_size, created_at, updated_at)
		VALUES (?, 501, 'pending', 'report.pdf', '', 'document', 2048, ?, ?)
	`, chatID, msgDate+2, msgDate+2)

	scannedRecords, err := db.GetPendingDownloads(10)
	if err != nil || len(scannedRecords) == 0 {
		t.Fatalf("failed to scan pending downloads: %v", err)
	}
	scannedRec := scannedRecords[0]

	// 4. Compare planned paths
	planner := PathPlanner{}
	realtimePath := planner.Plan(realtimeRec.ChatID, realtimeRec.TargetTitle, realtimeRec.MessageID, realtimeRec.FileName, realtimeRec.MediaType, realtimeRec.Date)
	scannedPath := planner.Plan(scannedRec.ChatID, scannedRec.TargetTitle, scannedRec.MessageID, scannedRec.FileName, scannedRec.MediaType, scannedRec.Date)

	if realtimePath != scannedPath {
		t.Fatalf("realtime path %q != scanned path %q", realtimePath, scannedPath)
	}

	expectedCanonical := "Archive Vault/2024_09/501 - report.pdf"
	if realtimePath != expectedCanonical {
		t.Fatalf("expected %q, got %q", expectedCanonical, realtimePath)
	}
}

func TestOrchestrator_PlannedPathAcceptedBeforeFilesystemCreation(t *testing.T) {
	orch, reg, db, saveDir := setupTestOrchestrator(t, nil, nil)
	defer db.Close()

	chatID := "chat_pre_fail"
	msgID := 601
	gen := "gen_conflict"

	// Seed download_records already in terminal 'unavailable' state
	// BeginDownload strictly rejects unavailable!
	now := time.Now().Unix()
	_, _ = db.Execute(`
		INSERT INTO download_records (chat_id, message_id, status, file_name, save_path, media_type, file_size, attempt_generation, created_at, updated_at)
		VALUES (?, ?, 'unavailable', 'file.bin', 'planned/path.bin', 'document', 100, ?, ?, ?)
	`, chatID, msgID, gen, now, now)

	taskReq := TaskRequest{
		ID:          "task_fail_before_fs",
		Peer:        chatID,
		MessageID:   msgID,
		TargetTitle: "Vault",
		MediaType:   "document",
		FileName:    "file.bin",
		Date:        now,
	}
	_, _, _ = reg.Submit(taskReq)
	task, _ := reg.Next(context.Background())

	// Execute downloadOne
	orch.downloadOne(context.Background(), task)

	// Verify task failed with db_conflict
	snap := task.Snapshot()
	if snap.State != StateFailed {
		t.Fatalf("expected StateFailed, got %q", snap.State)
	}
	if snap.ErrorClass != "db_conflict" {
		t.Fatalf("expected ErrorClass 'db_conflict', got: %q (err: %s)", snap.ErrorClass, snap.Error)
	}

	// Verify NO files or part files were created on filesystem!
	plannedRel := PathPlanner{}.Plan(chatID, "Vault", msgID, "file.bin", "document", now)
	finalAbs := filepath.Join(saveDir, filepath.FromSlash(plannedRel))
	partAbs := finalAbs + ".part"

	if _, err := os.Stat(partAbs); !os.IsNotExist(err) {
		t.Fatalf("part file MUST NOT exist when BeginDownload fails! Found: %s", partAbs)
	}
	if _, err := os.Stat(finalAbs); !os.IsNotExist(err) {
		t.Fatalf("final file MUST NOT exist when BeginDownload fails! Found: %s", finalAbs)
	}
}
