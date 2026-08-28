package daemon

import (
	"context"
	"fmt"
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

	targetCacheMu sync.RWMutex
	targetCache   map[string]ListenTarget
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
		db:           db,
		orchestrator: orchestrator,
		logger:       logger,
		dispatcher:   tg.NewUpdateDispatcher(),
		targetCache:  make(map[string]ListenTarget),
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
	s.targetCache = make(map[string]ListenTarget)
	for _, t := range targets {
		if t.Enabled {
			s.targetCache[t.ChatID] = t
			s.targetCache[strings.ToLower(t.ChatID)] = t

			cleanID := strings.TrimPrefix(t.ChatID, "@")
			s.targetCache[cleanID] = t
			s.targetCache[strings.ToLower(cleanID)] = t

			if strings.HasPrefix(cleanID, "-100") && len(cleanID) > 4 {
				s.targetCache[cleanID[4:]] = t
			} else if strings.HasPrefix(cleanID, "-") && len(cleanID) > 1 {
				s.targetCache[cleanID[1:]] = t
			}

			if t.Username != "" {
				u := strings.TrimPrefix(t.Username, "@")
				s.targetCache[u] = t
				s.targetCache[strings.ToLower(u)] = t
				s.targetCache["@"+u] = t
				s.targetCache["@"+strings.ToLower(u)] = t
			}
		}
	}
}

func (s *UpdatesStream) matchTarget(msg *tg.Message, e tg.Entities) (ListenTarget, bool) {
	if msg == nil {
		return ListenTarget{}, false
	}

	// 1. Match by PeerID
	peerID := extractPeerID(msg.PeerID)
	if peerID != "" {
		if target, ok := s.lookupTarget(peerID); ok {
			return target, true
		}
	}

	// 2. Match by FromID (for Bot direct messages or specific senders)
	if msg.FromID != nil {
		fromID := extractPeerID(msg.FromID)
		if fromID != "" {
			if target, ok := s.lookupTarget(fromID); ok {
				return target, true
			}
		}
	}

	// 3. Match by Entities (Channel or User username)
	switch p := msg.PeerID.(type) {
	case *tg.PeerChannel:
		if ch, ok := e.Channels[p.ChannelID]; ok && ch != nil && ch.Username != "" {
			if target, ok := s.lookupTarget(ch.Username); ok {
				return target, true
			}
		}
	case *tg.PeerUser:
		if u, ok := e.Users[p.UserID]; ok && u != nil && u.Username != "" {
			if target, ok := s.lookupTarget(u.Username); ok {
				return target, true
			}
		}
	}

	if msg.FromID != nil {
		if p, ok := msg.FromID.(*tg.PeerUser); ok {
			if u, ok := e.Users[p.UserID]; ok && u != nil && u.Username != "" {
				if target, ok := s.lookupTarget(u.Username); ok {
					return target, true
				}
			}
		}
	}

	return ListenTarget{}, false
}

func (s *UpdatesStream) lookupTarget(key string) (ListenTarget, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return ListenTarget{}, false
	}

	s.targetCacheMu.RLock()
	t, ok := s.targetCache[key]
	if !ok {
		t, ok = s.targetCache[strings.ToLower(key)]
	}
	if !ok {
		clean := strings.TrimPrefix(key, "@")
		t, ok = s.targetCache[clean]
		if !ok {
			t, ok = s.targetCache[strings.ToLower(clean)]
		}
		if !ok && strings.HasPrefix(clean, "-100") && len(clean) > 4 {
			t, ok = s.targetCache[clean[4:]]
		}
	}
	s.targetCacheMu.RUnlock()

	if ok && t.Enabled {
		return t, true
	}

	if s.db != nil {
		var target ListenTarget
		var enabledInt int
		cleanKey := strings.TrimPrefix(key, "@")
		queryKey := "%" + cleanKey + "%"
		err := s.db.DB().QueryRow(`
			SELECT chat_id, enabled, title, username, download_filter 
			FROM listen_targets 
			WHERE enabled = 1 AND (chat_id = ? OR chat_id = ? OR username LIKE ? OR chat_id LIKE ?)
			LIMIT 1
		`, key, "@"+cleanKey, queryKey, queryKey).Scan(
			&target.ChatID, &enabledInt, &target.Title, &target.Username, &target.DownloadFilter,
		)
		if err == nil && enabledInt == 1 {
			target.Enabled = true
			s.targetCacheMu.Lock()
			s.targetCache[key] = target
			s.targetCacheMu.Unlock()
			return target, true
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
			s.logger.Warn("failed to ingest stream message", zap.Error(err))
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
