package daemon

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/expr-lang/expr"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/tmedia"
	"github.com/Hittlert/TGX/pkg/texpr"
)

// UpdatesStream listens to real-time MTProto server push events and streams tasks directly into SBE.
type UpdatesStream struct {
	db           *Database
	orchestrator *Orchestrator
	logger       *zap.Logger
	dispatcher   tg.UpdateDispatcher

	targetCacheMu    sync.RWMutex
	targetByID       map[int64]ListenTarget
	targetByUsername map[string]ListenTarget
}

var (
	globalUpdatesStreamMu sync.RWMutex
	globalUpdatesStream   *UpdatesStream
)

func SetGlobalUpdatesStream(s *UpdatesStream) {
	globalUpdatesStreamMu.Lock()
	defer globalUpdatesStreamMu.Unlock()
	globalUpdatesStream = s
}

func GlobalUpdateHandler() tg.UpdateDispatcher {
	dispatcher := tg.NewUpdateDispatcher()
	dispatcher.OnNewChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
		globalUpdatesStreamMu.RLock()
		s := globalUpdatesStream
		globalUpdatesStreamMu.RUnlock()
		if s != nil {
			s.handleMessage(ctx, u.Message, e)
		}
		return nil
	})
	dispatcher.OnNewMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
		globalUpdatesStreamMu.RLock()
		s := globalUpdatesStream
		globalUpdatesStreamMu.RUnlock()
		if s != nil {
			s.handleMessage(ctx, u.Message, e)
		}
		return nil
	})
	dispatcher.OnEditChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateEditChannelMessage) error {
		globalUpdatesStreamMu.RLock()
		s := globalUpdatesStream
		globalUpdatesStreamMu.RUnlock()
		if s != nil {
			s.handleMessage(ctx, u.Message, e)
		}
		return nil
	})
	dispatcher.OnEditMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateEditMessage) error {
		globalUpdatesStreamMu.RLock()
		s := globalUpdatesStream
		globalUpdatesStreamMu.RUnlock()
		if s != nil {
			s.handleMessage(ctx, u.Message, e)
		}
		return nil
	})
	dispatcher.OnChannel(func(ctx context.Context, e tg.Entities, u *tg.UpdateChannel) error {
		globalUpdatesStreamMu.RLock()
		s := globalUpdatesStream
		globalUpdatesStreamMu.RUnlock()
		if s != nil {
			s.handleChannelUpdate(ctx, u.ChannelID, e)
		}
		return nil
	})
	dispatcher.OnChat(func(ctx context.Context, e tg.Entities, u *tg.UpdateChat) error {
		globalUpdatesStreamMu.RLock()
		s := globalUpdatesStream
		globalUpdatesStreamMu.RUnlock()
		if s != nil {
			s.handleChatUpdate(ctx, u.ChatID, e)
		}
		return nil
	})
	return dispatcher
}

func NewUpdatesStream(db *Database, orchestrator *Orchestrator, logger *zap.Logger) *UpdatesStream {
	s := &UpdatesStream{
		db:               db,
		orchestrator:     orchestrator,
		logger:           logger,
		dispatcher:       tg.NewUpdateDispatcher(),
		targetByID:       make(map[int64]ListenTarget),
		targetByUsername: make(map[string]ListenTarget),
	}
	s.registerHandlers()
	s.refreshTargetCache()
	return s
}

func (s *UpdatesStream) Dispatcher() tg.UpdateDispatcher {
	return s.dispatcher
}

func (s *UpdatesStream) refreshTargetCache() {
	if s.db == nil {
		return
	}
	targets, err := s.db.GetListenTargets()
	if err != nil {
		return
	}

	s.targetCacheMu.Lock()
	defer s.targetCacheMu.Unlock()
	s.targetByID = make(map[int64]ListenTarget)
	s.targetByUsername = make(map[string]ListenTarget)
	for _, t := range targets {
		if t.Enabled {
			cleanID := strings.TrimPrefix(strings.TrimSpace(t.ChatID), "@")
			if strings.HasPrefix(cleanID, "-100") && len(cleanID) > 4 {
				if id, err := strconv.ParseInt(cleanID[4:], 10, 64); err == nil {
					s.targetByID[id] = t
				}
			} else if id, err := strconv.ParseInt(cleanID, 10, 64); err == nil {
				if id < 0 {
					s.targetByID[-id] = t
				} else {
					s.targetByID[id] = t
				}
			}

			if t.Username != "" {
				u := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(t.Username), "@"))
				if u != "" {
					s.targetByUsername[u] = t
				}
			}
		}
	}
}

func (s *UpdatesStream) matchTarget(msg *tg.Message, e tg.Entities) (ListenTarget, bool) {
	if msg == nil {
		return ListenTarget{}, false
	}

	s.targetCacheMu.RLock()
	defer s.targetCacheMu.RUnlock()

	// 1. Match by PeerID (Typed MTProto Peer)
	switch p := msg.PeerID.(type) {
	case *tg.PeerChannel:
		if t, ok := s.targetByID[p.ChannelID]; ok && t.Enabled {
			return t, true
		}
		if ch, ok := e.Channels[p.ChannelID]; ok && ch != nil && ch.Username != "" {
			if t, ok := s.targetByUsername[strings.ToLower(ch.Username)]; ok && t.Enabled {
				return t, true
			}
		}
	case *tg.PeerChat:
		if t, ok := s.targetByID[p.ChatID]; ok && t.Enabled {
			return t, true
		}
	case *tg.PeerUser:
		if t, ok := s.targetByID[p.UserID]; ok && t.Enabled {
			return t, true
		}
		if u, ok := e.Users[p.UserID]; ok && u != nil && u.Username != "" {
			if t, ok := s.targetByUsername[strings.ToLower(u.Username)]; ok && t.Enabled {
				return t, true
			}
		}
	}

	// 2. Match by FromID
	if msg.FromID != nil {
		switch p := msg.FromID.(type) {
		case *tg.PeerChannel:
			if t, ok := s.targetByID[p.ChannelID]; ok && t.Enabled {
				return t, true
			}
		case *tg.PeerUser:
			if t, ok := s.targetByID[p.UserID]; ok && t.Enabled {
				return t, true
			}
			if u, ok := e.Users[p.UserID]; ok && u != nil && u.Username != "" {
				if t, ok := s.targetByUsername[strings.ToLower(u.Username)]; ok && t.Enabled {
					return t, true
				}
			}
		}
	}

	return ListenTarget{}, false
}

func (s *UpdatesStream) registerHandlers() {
	// 1. Real-time Channel Messages (Broadcasting & Supergroups)
	s.dispatcher.OnNewChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
		s.handleMessage(ctx, u.Message, e)
		return nil
	})

	// 2. Real-time Regular Messages (Private chats & Basic groups)
	s.dispatcher.OnNewMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
		s.handleMessage(ctx, u.Message, e)
		return nil
	})

	// 3. Edited Channel / Regular Messages (Bots updating placeholders with final video)
	s.dispatcher.OnEditChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateEditChannelMessage) error {
		s.handleMessage(ctx, u.Message, e)
		return nil
	})

	s.dispatcher.OnEditMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateEditMessage) error {
		s.handleMessage(ctx, u.Message, e)
		return nil
	})

	// 4. Real-time Channel & Group metadata changes (Title, Username, etc.)
	s.dispatcher.OnChannel(func(ctx context.Context, e tg.Entities, u *tg.UpdateChannel) error {
		s.handleChannelUpdate(ctx, u.ChannelID, e)
		return nil
	})

	s.dispatcher.OnChat(func(ctx context.Context, e tg.Entities, u *tg.UpdateChat) error {
		s.handleChatUpdate(ctx, u.ChatID, e)
		return nil
	})
}

func (s *UpdatesStream) handleMessage(ctx context.Context, msgClass tg.MessageClass, e tg.Entities) {
	msg, ok := msgClass.(*tg.Message)
	if !ok || msg == nil {
		return
	}

	target, enabled := s.matchTarget(msg, e)
	if !enabled {
		return
	}

	chatID := target.ChatID

	var media *tmedia.Media
	hasMedia := false
	if msg.Media != nil {
		media, hasMedia = tmedia.ExtractMedia(msg.Media)
	} else if m, ok := msg.GetMedia(); ok {
		media, hasMedia = tmedia.ExtractMedia(m)
	}
	mediaType := ""
	fileName := ""
	var fileSize int64

	if hasMedia && media != nil {
		fileName = media.Name
		fileSize = media.Size
		if strings.HasSuffix(fileName, ".jpg") || strings.HasSuffix(fileName, ".png") {
			mediaType = "photo"
		} else if strings.HasSuffix(fileName, ".mp3") || strings.HasSuffix(fileName, ".ogg") {
			mediaType = "audio"
		} else {
			mediaType = "video"
		}
	}

	s.logger.Info("⚡ [Stream Engine] Incoming real-time message",
		zap.String("chat_id", chatID),
		zap.String("title", target.Title),
		zap.Int("message_id", msg.ID),
		zap.Bool("has_media", hasMedia),
		zap.String("file_name", fileName),
		zap.Int64("size", fileSize),
	)

	// Filter matching
	if target.DownloadFilter != "" && hasMedia {
		env := texpr.ConvertEnvMessage(msg)
		prog, err := expr.Compile(target.DownloadFilter, expr.Env(env), expr.AsBool())
		if err == nil {
			result, err := expr.Run(prog, env)
			if err == nil {
				if matched, ok := result.(bool); ok && !matched {
					s.logger.Debug("stream message filtered out",
						zap.String("chat_id", chatID),
						zap.Int("message_id", msg.ID),
						zap.String("filter", target.DownloadFilter),
					)
					return
				}
			}
		}
	}

	senderID := ""
	senderName := ""
	if msg.FromID != nil {
		senderID = extractPeerID(msg.FromID)
	}

	chatMsg := ChatMessage{
		ChatID:           chatID,
		MessageID:        msg.ID,
		SenderID:         senderID,
		SenderName:       senderName,
		Text:             msg.Message,
		MediaType:        mediaType,
		FileName:         fileName,
		FileSize:         fileSize,
		HasMedia:         hasMedia,
		ReplyToMessageID: 0,
		Date:             int64(msg.Date),
	}

	if s.db != nil {
		if err := s.db.IngestMessage(chatMsg); err != nil {
			s.logger.Error("failed to ingest stream message in transaction",
				zap.String("chat_id", chatID),
				zap.Int("message_id", msg.ID),
				zap.Error(err),
			)
			return
		}
	}

	s.logger.Info("⚡ [Stream Engine] Real-time message detected",
		zap.String("target", target.Title),
		zap.String("chat_id", chatID),
		zap.Int("message_id", msg.ID),
		zap.String("file_name", fileName),
		zap.Int64("size", fileSize),
	)

	// Instantly trigger dispatch into SBE queue with zero latency
	if s.orchestrator != nil && hasMedia {
		rec := DownloadRecord{
			ChatID:      chatID,
			MessageID:   msg.ID,
			Status:      "pending",
			FileName:    fileName,
			MediaType:   mediaType,
			FileSize:    fileSize,
			CreatedAt:   int64(msg.Date),
			TargetTitle: target.Title,
		}
		s.orchestrator.TriggerStreamDispatch(ctx, rec)
	}
}

func (s *UpdatesStream) handleChannelUpdate(ctx context.Context, channelID int64, e tg.Entities) {
	chatID := fmt.Sprintf("-100%d", channelID)
	if ch, ok := e.Channels[channelID]; ok && ch != nil {
		s.logger.Debug("⚡ [Stream Engine] Channel metadata updated",
			zap.String("chat_id", chatID),
			zap.String("title", ch.Title),
			zap.String("username", ch.Username),
		)
		if s.db != nil {
			_, _ = s.db.DB().Exec(`
				UPDATE listen_targets SET title = ?, username = ?, updated_at = ? WHERE chat_id = ?
			`, ch.Title, ch.Username, time.Now().Unix(), chatID)
		}
		s.refreshTargetCache()
	}
}

func (s *UpdatesStream) handleChatUpdate(ctx context.Context, chatIDInt int64, e tg.Entities) {
	chatID := fmt.Sprintf("-%d", chatIDInt)
	if c, ok := e.Chats[chatIDInt]; ok && c != nil {
		title := c.Title
		s.logger.Debug("⚡ [Stream Engine] Chat metadata updated",
			zap.String("chat_id", chatID),
			zap.String("title", title),
		)
		if s.db != nil && title != "" {
			_, _ = s.db.DB().Exec(`
				UPDATE listen_targets SET title = ?, updated_at = ? WHERE chat_id = ?
			`, title, time.Now().Unix(), chatID)
		}
		s.refreshTargetCache()
	}
}

func extractPeerID(peer tg.PeerClass) string {
	if peer == nil {
		return ""
	}
	switch p := peer.(type) {
	case *tg.PeerChannel:
		return fmt.Sprintf("-100%d", p.ChannelID)
	case *tg.PeerChat:
		return fmt.Sprintf("-%d", p.ChatID)
	case *tg.PeerUser:
		return fmt.Sprintf("%d", p.UserID)
	default:
		return ""
	}
}
