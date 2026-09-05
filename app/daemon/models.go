package daemon

import (
	"strings"

	"github.com/gotd/td/tg"
)

type ListenTarget struct {
	ChatID               string `json:"chat_id"`
	Enabled              bool   `json:"enabled"`
	Title                string `json:"title"`
	Username             string `json:"username"`
	ChatType             string `json:"type"`
	DownloadFilter       string `json:"download_filter"`
	UploadTelegramChatID string `json:"upload_telegram_chat_id"`
	Priority             int    `json:"priority"`
	LastReadMessageID    int    `json:"last_read_message_id"`
	CreatedAt            int64  `json:"created_at,omitempty"`
	UpdatedAt            int64  `json:"updated_at,omitempty"`
	Revision             int    `json:"revision,omitempty"`
}

type ChatMessage struct {
	ChatID           string `json:"chat_id"`
	MessageID        int    `json:"message_id"`
	SenderID         string `json:"sender_id"`
	SenderName       string `json:"sender_name"`
	Text             string `json:"text"`
	MediaType        string `json:"media_type"`
	HasMedia         bool   `json:"has_media"`
	ReplyToMessageID int    `json:"reply_to_message_id"`
	Date             int64  `json:"date"`
	FileName         string `json:"file_name,omitempty"`
	FileSize         int64  `json:"file_size,omitempty"`
}

type DownloadRecord struct {
	ChatID              string `json:"chat_id"`
	MessageID           int    `json:"message_id"`
	Status              string `json:"status"` // pending, resolving, downloading, committing, success, failed, unavailable
	FileName            string `json:"file_name"`
	SavePath            string `json:"save_path"`
	MediaType           string `json:"media_type"`
	FileSize            int64  `json:"file_size"`
	SHA256              string `json:"sha256,omitempty"`
	Error               string `json:"error,omitempty"`
	ErrorStage          string `json:"error_stage,omitempty"`
	ErrorOp             string `json:"error_op,omitempty"`
	ErrorClass          string `json:"error_class,omitempty"`
	Retryable           bool   `json:"retryable,omitempty"`
	RetryOwner          string `json:"retry_owner,omitempty"`
	CreatedAt           int64  `json:"created_at"`
	UpdatedAt           int64  `json:"updated_at"`
	DownloadedAt        int64  `json:"downloaded_at,omitempty"`
	ProcessingStartedAt int64  `json:"processing_started_at,omitempty"`
	Attempts            int    `json:"attempts"`
	AttemptGeneration   string `json:"attempt_generation,omitempty"`
	NextRetryAt         int64  `json:"next_retry_at"`
	TargetTitle         string `json:"target_title,omitempty"`
	Date                int64  `json:"date,omitempty"`
}

type ArchiveJob struct {
	ChatID       string `json:"chat_id"`
	MessageID    int    `json:"message_id"`
	RelativePath string `json:"relative_path"`
	ExpectedSize int64  `json:"expected_size"`
	SHA256       string `json:"sha256"`
	State        string `json:"state"` // pending, copying, archived, conflict
	Attempts     int    `json:"attempts"`
	NextRetryAt  int64  `json:"next_retry_at"`
	ClaimID      string `json:"claim_id,omitempty"`
	LastError    string `json:"last_error,omitempty"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type ArchiveStats struct {
	BacklogFiles  int   `json:"archive_backlog_files"`
	BacklogBytes  int64 `json:"archive_backlog_bytes"`
	ActiveWorkers int   `json:"archive_active_workers"`
	ArchivedFiles int   `json:"archive_archived_files"`
	ConflictCount int   `json:"archive_conflict_count"`
	ArchivedBytes int64 `json:"archive_archived_bytes"`
}

type PublishResult struct {
	Path              string `json:"path"`
	SHA256            string `json:"sha256,omitempty"`
	AlreadyExists     bool   `json:"already_exists,omitempty"`
	WireBytes         int64  `json:"wire_bytes,omitempty"`
	ReplayBytes       int64  `json:"replay_bytes,omitempty"`
	RequestCount      int64  `json:"request_count,omitempty"`
	PhysicalRetries   int64  `json:"physical_retries,omitempty"`
	PhysicalAttemptID string `json:"physical_attempt_id,omitempty"`
	absolutePath      string
}

func normalizePeer(peer string) string {
	return strings.TrimPrefix(strings.TrimSpace(peer), "@")
}

type MediaFile interface {
	Location() tg.InputFileLocationClass
	Size() int64
	DC() int
}

type ResolvedMedia struct {
	File      MediaFile
	Name      string
	Size      int64
	DCID      int
	Date      int64
	MediaType string
}

// Target-related DTOs for Issue #12

type TargetDialogDTO struct {
	ID                   string `json:"id"`
	ChatID               string `json:"chat_id"`
	Title                string `json:"title"`
	Username             string `json:"username"`
	Type                 string `json:"type"`
	Pinned               bool   `json:"pinned"`
	UnreadCount          int    `json:"unread_count"`
	TopMessageID         int    `json:"top_message_id"`
	LastReadMessageID    int    `json:"last_read_message_id"`
	IsTarget             bool   `json:"is_target"`
	Enabled              bool   `json:"enabled"`
	TargetEnabled        bool   `json:"target_enabled"`
	Priority             int    `json:"priority"`
	DownloadFilter       string `json:"download_filter"`
	UploadTelegramChatID string `json:"upload_telegram_chat_id"`
	LastMessageAt        int64  `json:"last_message_at"`
	LastScanFinishedAt   int64  `json:"last_scan_finished_at"`
	LastScanStartedAt    int64  `json:"last_scan_started_at"`
	UpdatedAt            int64  `json:"updated_at"`
}

type DialogsResponseDTO struct {
	OK      bool              `json:"ok"`
	Dialogs []TargetDialogDTO `json:"dialogs"`
	Error   string            `json:"error,omitempty"`
}

type AddTargetRequestDTO struct {
	Query  string `json:"query"`
	Target string `json:"target"`
	ChatID string `json:"chat_id"`
	Title  string `json:"title"`
}

type AddTargetResponseDTO struct {
	OK     bool            `json:"ok"`
	Peer   DialogDTO       `json:"peer,omitempty"`
	Dialog TargetDialogDTO `json:"dialog,omitempty"`
	Target TargetDialogDTO `json:"target,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type UpdateSingleTargetRequestDTO struct {
	ChatID               string  `json:"chat_id"`
	Enabled              *bool   `json:"enabled"`
	Priority             *int    `json:"priority"`
	LastReadMessageID    *int    `json:"last_read_message_id"`
	DownloadFilter       *string `json:"download_filter"`
	UploadTelegramChatID *string `json:"upload_telegram_chat_id"`
	Title                string  `json:"title"`
	Username             string  `json:"username"`
	Type                 string  `json:"type"`
}

type UpdateSingleTargetResponseDTO struct {
	OK     bool         `json:"ok"`
	Target ListenTarget `json:"target,omitempty"`
	Error  string       `json:"error,omitempty"`
}

type TargetProgressItemDTO struct {
	ChatID            string `json:"chat_id"`
	Title             string `json:"title"`
	Enabled           bool   `json:"enabled"`
	LastReadMessageID int    `json:"last_read_message_id"`
	ScanStatus        string `json:"scan_status"`
	ScanError         string `json:"scan_error,omitempty"`
	TotalFiles        int    `json:"total_files"`
	DownloadedFiles   int    `json:"downloaded_files"`
	PendingFiles      int    `json:"pending_files"`
	ProcessingFiles   int    `json:"processing_files"`
	FailedFiles       int    `json:"failed_files"`
	SkippedFiles      int    `json:"skipped_files"`
	DownloadedBytes   int64  `json:"downloaded_bytes"`
}

type TargetProgressResponseDTO struct {
	OK       bool                    `json:"ok"`
	Progress []TargetProgressItemDTO `json:"progress"`
	Error    string                  `json:"error,omitempty"`
}

type ChatContextMessageDTO struct {
	ChatID           string `json:"chat_id"`
	MessageID        int    `json:"message_id"`
	SenderID         string `json:"sender_id"`
	SenderName       string `json:"sender_name"`
	Text             string `json:"text"`
	MediaType        string `json:"media_type"`
	HasMedia         bool   `json:"has_media"`
	ReplyToMessageID int    `json:"reply_to_message_id,omitempty"`
	Date             int64  `json:"date"`
}

type ChatContextResponseDTO struct {
	OK       bool                    `json:"ok"`
	Messages []ChatContextMessageDTO `json:"messages"`
	Limit    int                     `json:"limit"`
	Error    string                  `json:"error,omitempty"`
}
