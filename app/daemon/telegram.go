package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/Hittlert/TGX/core/dcpool"
	"github.com/Hittlert/TGX/core/downloader"
	"github.com/Hittlert/TGX/core/tmedia"
	"github.com/Hittlert/TGX/core/util/tutil"
)

type telegramMediaAccess struct {
	pool        dcpool.Pool
	manager     *peers.Manager
	syncMu      sync.Mutex
	lastSync    time.Time
	sf          singleflight.Group
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
	if !a.lastSync.IsZero() && time.Since(a.lastSync) < 30*time.Second {
		a.syncMu.Unlock()
		return nil
	}
	a.syncMu.Unlock()

	_, err, _ := a.sf.Do("sync_peers", func() (interface{}, error) {
		a.syncMu.Lock()
		if !a.lastSync.IsZero() && time.Since(a.lastSync) < 30*time.Second {
			a.syncMu.Unlock()
			return nil, nil
		}
		a.syncMu.Unlock()

		syncCtx, cancel := context.WithTimeout(context.Background(), a.syncTimeout)
		defer cancel()

		var syncErr error
		iter := query.GetDialogs(a.pool.Default(syncCtx)).BatchSize(100).Iter()
		for iter.Next(syncCtx) {
			elem := iter.Value()
			usersMap := elem.Entities.Users()
			chatsMap := elem.Entities.Chats()
			channelsMap := elem.Entities.Channels()

			users := make([]tg.UserClass, 0, len(usersMap))
			for _, u := range usersMap {
				users = append(users, u)
			}
			chats := make([]tg.ChatClass, 0, len(chatsMap)+len(channelsMap))
			for _, c := range chatsMap {
				chats = append(chats, c)
			}
			for _, c := range channelsMap {
				chats = append(chats, c)
			}
			if len(users) > 0 || len(chats) > 0 {
				if applyErr := a.manager.Apply(syncCtx, users, chats); applyErr != nil {
					syncErr = applyErr
					break
				}
			}
		}

		if iterErr := iter.Err(); iterErr != nil && syncErr == nil {
			syncErr = iterErr
		}

		if syncErr != nil {
			return nil, classifyTelegramError(syncErr, "sync dialogs")
		}

		a.syncMu.Lock()
		a.lastSync = time.Now()
		a.syncMu.Unlock()
		return nil, nil
	})
	if err != nil {
		return err
	}
	return ctx.Err()
}

type telegramFile struct {
	media *tmedia.Media
}

var _ downloader.File = telegramFile{}

func (f telegramFile) Location() tg.InputFileLocationClass { return f.media.InputFileLoc }
func (f telegramFile) Size() int64                         { return f.media.Size }
func (f telegramFile) DC() int                             { return f.media.DC }

func classifyTelegramError(err error, operation string) error {
	errStr := strings.ToLower(err.Error())
	unavailable := errors.Is(err, tutil.ErrMessageDeleted) || tgerr.Is(err,
		"CHANNEL_INVALID",
		"CHANNEL_PRIVATE",
		"CHAT_ID_INVALID",
		"USER_ID_INVALID",
		"CHAT_ADMIN_REQUIRED",
		"USER_BANNED_IN_CHANNEL",
		"PEER_ID_INVALID",
		"MESSAGE_ID_INVALID",
		"FILEREF_UPGRADE_NEEDED",
		"FILE_REFERENCE_EXPIRED",
		"FILE_REFERENCE_INVALID",
		"FILE_ID_INVALID",
	) || strings.Contains(errStr, "connection failed")
	if unavailable {
		return NewTaskError("unavailable", true, fmt.Errorf("%s: %w", operation, err))
	}
	return NewTaskError("telegram", false, fmt.Errorf("%s: %w", operation, err))
}
