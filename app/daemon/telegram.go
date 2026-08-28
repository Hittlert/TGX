package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/telegram/query/dialogs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/Hittlert/TG_Downloader/core/dcpool"
	"github.com/Hittlert/TG_Downloader/core/downloader"
	"github.com/Hittlert/TG_Downloader/core/tmedia"
	"github.com/Hittlert/TG_Downloader/core/util/tutil"
)

type telegramMediaAccess struct {
	pool        dcpool.Pool
	manager     *peers.Manager
	syncMu      sync.Mutex
	syncTimeout time.Duration
}

func newTelegramMediaAccess(pool dcpool.Pool, manager *peers.Manager, syncTimeout time.Duration) *telegramMediaAccess {
	return &telegramMediaAccess{pool: pool, manager: manager, syncTimeout: syncTimeout}
}

func (a *telegramMediaAccess) Resolve(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
	resolvedPeer, err := tutil.GetInputPeer(ctx, a.manager, peer)
	if err != nil {
		if syncErr := a.SyncPeers(ctx); syncErr == nil {
			resolvedPeer, err = tutil.GetInputPeer(ctx, a.manager, peer)
		}
	}
	if err != nil {
		return ResolvedMedia{}, classifyTelegramError(err, "resolve peer")
	}
	message, err := tutil.GetSingleMessage(ctx, a.pool.Default(ctx), resolvedPeer.InputPeer(), messageID)
	if err != nil {
		return ResolvedMedia{}, classifyTelegramError(err, "resolve message")
	}
	media, ok := tmedia.GetMedia(message)
	if !ok || media.Size <= 0 {
		return ResolvedMedia{}, NewTaskError("unavailable", true, fmt.Errorf("message %s/%d has no downloadable media", peer, messageID))
	}
	file := telegramFile{media: media}
	return ResolvedMedia{
		File: file, Name: media.Name, Size: media.Size, DCID: media.DC, Date: media.Date,
	}, nil
}

func (a *telegramMediaAccess) SyncPeers(ctx context.Context) error {
	a.syncMu.Lock()
	defer a.syncMu.Unlock()
	syncCtx, cancel := context.WithTimeout(ctx, a.syncTimeout)
	defer cancel()
	return query.GetDialogs(a.pool.Default(syncCtx)).BatchSize(100).ForEach(syncCtx, func(ctx context.Context, elem dialogs.Elem) error {
		id := tutil.GetInputPeerID(elem.Peer)
		if id == 0 && elem.Dialog != nil {
			id = tutil.GetPeerID(elem.Dialog.GetPeer())
		}
		users := make([]tg.UserClass, 0, 1)
		if user, ok := elem.Entities.User(id); ok {
			users = append(users, user)
		}
		chats := make([]tg.ChatClass, 0, 1)
		if chat, ok := elem.Entities.Chat(id); ok {
			chats = append(chats, chat)
		}
		if channel, ok := elem.Entities.Channel(id); ok {
			chats = append(chats, channel)
		}
		return a.manager.Apply(ctx, users, chats)
	})
}

type telegramFile struct {
	media *tmedia.Media
}

var _ downloader.File = telegramFile{}

func (f telegramFile) Location() tg.InputFileLocationClass { return f.media.InputFileLoc }
func (f telegramFile) Size() int64                         { return f.media.Size }
func (f telegramFile) DC() int                             { return f.media.DC }

func classifyTelegramError(err error, operation string) error {
	unavailable := errors.Is(err, tutil.ErrMessageDeleted) || tgerr.Is(err,
		"CHANNEL_PRIVATE",
		"CHAT_ADMIN_REQUIRED",
		"USER_BANNED_IN_CHANNEL",
		"PEER_ID_INVALID",
		"MESSAGE_ID_INVALID",
	)
	if unavailable {
		return NewTaskError("unavailable", true, fmt.Errorf("%s: %w", operation, err))
	}
	return NewTaskError("telegram", false, fmt.Errorf("%s: %w", operation, err))
}
