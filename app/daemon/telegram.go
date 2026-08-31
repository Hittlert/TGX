package daemon

import (
	"context"
	"errors"
	"fmt"
	"sort"
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
	"github.com/Hittlert/TGX/pkg/sbe/gate"
)

type TelegramAccess interface {
	GetDialogs(ctx context.Context) ([]DialogDTO, error)
	GetHistory(ctx context.Context, req HistoryRequest) ([]MessageDTO, error)
	ResolvePeerInfo(ctx context.Context, queryStr string) (DialogDTO, error)
	Resolve(ctx context.Context, peer string, messageID int) (ResolvedMedia, error)
	ResolveBatch(ctx context.Context, peer string, messageIDs []int) (map[int]ResolvedMedia, error)
	SyncPeers(ctx context.Context) error
}

type messageCacheEntry struct {
	msg     *tg.Message
	expires time.Time
}

type telegramMediaAccess struct {
	parentCtx   context.Context
	pool        dcpool.Pool
	manager     *peers.Manager
	gate        *gate.FloodGate
	syncMu      sync.Mutex
	lastSync    time.Time
	sf          singleflight.Group
	syncTimeout time.Duration

	msgMu    sync.RWMutex
	msgCache map[string]messageCacheEntry
}

func newTelegramMediaAccess(parentCtx context.Context, pool dcpool.Pool, manager *peers.Manager, syncTimeout time.Duration, gate *gate.FloodGate) *telegramMediaAccess {
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	return &telegramMediaAccess{
		parentCtx:   parentCtx,
		pool:        pool,
		manager:     manager,
		syncTimeout: syncTimeout,
		gate:        gate,
		msgCache:    make(map[string]messageCacheEntry),
	}
}

const maxMsgCacheEntries = 1000

func (a *telegramMediaAccess) cleanExpiredCacheLocked(now time.Time) {
	for k, v := range a.msgCache {
		if now.After(v.expires) {
			delete(a.msgCache, k)
		}
	}
	if len(a.msgCache) >= maxMsgCacheEntries {
		type entryItem struct {
			key string
			exp time.Time
		}
		entries := make([]entryItem, 0, len(a.msgCache))
		for k, v := range a.msgCache {
			entries = append(entries, entryItem{key: k, exp: v.expires})
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].exp.Before(entries[j].exp)
		})
		toRemove := len(a.msgCache) - (maxMsgCacheEntries - 100)
		for i := 0; i < toRemove && i < len(entries); i++ {
			delete(a.msgCache, entries[i].key)
		}
	}
}

func (a *telegramMediaAccess) mediaFromMessage(peer string, messageID int, message *tg.Message) (ResolvedMedia, error) {
	if message == nil {
		return ResolvedMedia{}, NewTaskError("unavailable", true, fmt.Errorf("message %s/%d is empty", peer, messageID))
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

func (a *telegramMediaAccess) Resolve(ctx context.Context, peer string, messageID int) (ResolvedMedia, error) {
	cacheKey := fmt.Sprintf("%s:%d", peer, messageID)
	a.msgMu.RLock()
	if entry, ok := a.msgCache[cacheKey]; ok && time.Now().Before(entry.expires) {
		a.msgMu.RUnlock()
		return a.mediaFromMessage(peer, messageID, entry.msg)
	}
	a.msgMu.RUnlock()

	resolvedPeer, err := tutil.GetInputPeer(ctx, a.manager, peer)
	if err != nil {
		if syncErr := a.SyncPeers(ctx); syncErr == nil {
			resolvedPeer, err = tutil.GetInputPeer(ctx, a.manager, peer)
		}
	}
	if err != nil {
		return ResolvedMedia{}, classifyTelegramError(err, "resolve peer")
	}

	// Deterministic bucket key: bucketStart is aligned to 50-message boundary
	bucketStart := (messageID / 50) * 50
	if bucketStart == 0 {
		bucketStart = 1
	}
	batchKey := fmt.Sprintf("batch:%s:%d", peer, bucketStart)

	// SingleFlight DoChan with daemon-owned context to isolate followers from leader cancellation
	ch := a.sf.DoChan(batchKey, func() (interface{}, error) {
		fetchCtx, fetchCancel := context.WithTimeout(a.parentCtx, 15*time.Second)
		defer fetchCancel()

		if a.gate != nil {
			if err := a.gate.AcquireControlSlot(fetchCtx); err != nil {
				return nil, err
			}
			defer a.gate.ReleaseControlSlot()
		}

		// Prefetch entire 50-message bucket starting at bucketStart
		ids := make([]int, 0, 50)
		for i := 0; i < 50; i++ {
			ids = append(ids, bucketStart+i)
		}

		messages, fetchErr := tutil.GetMessagesBatch(fetchCtx, a.pool.Default(fetchCtx), resolvedPeer.InputPeer(), ids)
		if fetchErr != nil {
			// Fallback to single message
			singleMsg, sErr := tutil.GetSingleMessage(fetchCtx, a.pool.Default(fetchCtx), resolvedPeer.InputPeer(), messageID)
			if sErr != nil {
				return nil, sErr
			}
			a.msgMu.Lock()
			a.cleanExpiredCacheLocked(time.Now())
			a.msgCache[cacheKey] = messageCacheEntry{msg: singleMsg, expires: time.Now().Add(60 * time.Second)}
			a.msgMu.Unlock()
			return nil, nil
		}

		a.msgMu.Lock()
		now := time.Now()
		a.cleanExpiredCacheLocked(now)
		for _, msg := range messages {
			if msg != nil {
				k := fmt.Sprintf("%s:%d", peer, msg.ID)
				a.msgCache[k] = messageCacheEntry{msg: msg, expires: now.Add(60 * time.Second)}
			}
		}
		a.msgMu.Unlock()
		return nil, nil
	})

	select {
	case <-ctx.Done():
		return ResolvedMedia{}, ctx.Err()
	case <-ch:
	}

	// Read from populated cache
	a.msgMu.RLock()
	entry, ok := a.msgCache[cacheKey]
	a.msgMu.RUnlock()
	if ok && time.Now().Before(entry.expires) {
		return a.mediaFromMessage(peer, messageID, entry.msg)
	}

	// Fallback single message resolution
	if a.gate != nil {
		if err := a.gate.AcquireControlSlot(ctx); err != nil {
			return ResolvedMedia{}, err
		}
		defer a.gate.ReleaseControlSlot()
	}
	msg, err := tutil.GetSingleMessage(ctx, a.pool.Default(ctx), resolvedPeer.InputPeer(), messageID)
	if err != nil {
		return ResolvedMedia{}, classifyTelegramError(err, "resolve message")
	}
	a.msgMu.Lock()
	a.cleanExpiredCacheLocked(time.Now())
	a.msgCache[cacheKey] = messageCacheEntry{msg: msg, expires: time.Now().Add(60 * time.Second)}
	a.msgMu.Unlock()
	return a.mediaFromMessage(peer, messageID, msg)
}

func (a *telegramMediaAccess) ResolveBatch(ctx context.Context, peer string, messageIDs []int) (map[int]ResolvedMedia, error) {
	if len(messageIDs) == 0 {
		return make(map[int]ResolvedMedia), nil
	}

	resolvedPeer, err := tutil.GetInputPeer(ctx, a.manager, peer)
	if err != nil {
		if syncErr := a.SyncPeers(ctx); syncErr == nil {
			resolvedPeer, err = tutil.GetInputPeer(ctx, a.manager, peer)
		}
	}
	if err != nil {
		return nil, classifyTelegramError(err, "resolve peer")
	}

	result := make(map[int]ResolvedMedia)
	var missingIDs []int

	a.msgMu.RLock()
	now := time.Now()
	for _, id := range messageIDs {
		cacheKey := fmt.Sprintf("%s:%d", peer, id)
		if entry, ok := a.msgCache[cacheKey]; ok && now.Before(entry.expires) {
			if media, err := a.mediaFromMessage(peer, id, entry.msg); err == nil {
				result[id] = media
			}
		} else {
			missingIDs = append(missingIDs, id)
		}
	}
	a.msgMu.RUnlock()

	if len(missingIDs) == 0 {
		return result, nil
	}

	if a.gate != nil {
		if err := a.gate.AcquireControlSlot(ctx); err != nil {
			return nil, err
		}
		defer a.gate.ReleaseControlSlot()
	}

	messages, err := tutil.GetMessagesBatch(ctx, a.pool.Default(ctx), resolvedPeer.InputPeer(), missingIDs)
	if err != nil {
		return nil, classifyTelegramError(err, "resolve messages batch")
	}

	a.msgMu.Lock()
	a.cleanExpiredCacheLocked(now)
	for _, msg := range messages {
		if msg != nil {
			cacheKey := fmt.Sprintf("%s:%d", peer, msg.ID)
			a.msgCache[cacheKey] = messageCacheEntry{msg: msg, expires: time.Now().Add(60 * time.Second)}
			if media, mErr := a.mediaFromMessage(peer, msg.ID, msg); mErr == nil {
				result[msg.ID] = media
			}
		}
	}
	a.msgMu.Unlock()

	return result, nil
}

func (a *telegramMediaAccess) SyncPeers(ctx context.Context) error {
	a.syncMu.Lock()
	if !a.lastSync.IsZero() && time.Since(a.lastSync) < 30*time.Second {
		a.syncMu.Unlock()
		return nil
	}
	a.syncMu.Unlock()

	ch := a.sf.DoChan("sync_peers", func() (interface{}, error) {
		a.syncMu.Lock()
		if !a.lastSync.IsZero() && time.Since(a.lastSync) < 30*time.Second {
			a.syncMu.Unlock()
			return nil, nil
		}
		a.syncMu.Unlock()

		if a.gate != nil {
			if err := a.gate.AcquireControlSlot(a.parentCtx); err != nil {
				return nil, err
			}
			defer a.gate.ReleaseControlSlot()
		}

		syncCtx, cancel := context.WithTimeout(a.parentCtx, a.syncTimeout)
		defer cancel()

		seenUsers := make(map[string]tg.UserClass)
		seenChats := make(map[string]tg.ChatClass)

		var syncErr error
		iter := query.GetDialogs(a.pool.Default(syncCtx)).BatchSize(100).Iter()
		for iter.Next(syncCtx) {
			elem := iter.Value()
			usersMap := elem.Entities.Users()
			chatsMap := elem.Entities.Chats()
			channelsMap := elem.Entities.Channels()

			var newUsers []tg.UserClass
			for id, u := range usersMap {
				key := fmt.Sprintf("user:%d", id)
				if _, exists := seenUsers[key]; !exists {
					seenUsers[key] = u
					newUsers = append(newUsers, u)
				}
			}
			var newChats []tg.ChatClass
			for id, c := range chatsMap {
				key := fmt.Sprintf("chat:%d", id)
				if _, exists := seenChats[key]; !exists {
					seenChats[key] = c
					newChats = append(newChats, c)
				}
			}
			for id, c := range channelsMap {
				key := fmt.Sprintf("channel:%d", id)
				if _, exists := seenChats[key]; !exists {
					seenChats[key] = c
					newChats = append(newChats, c)
				}
			}
			if len(newUsers) > 0 || len(newChats) > 0 {
				if applyErr := a.manager.Apply(syncCtx, newUsers, newChats); applyErr != nil {
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

	select {
	case <-ctx.Done():
		return ctx.Err()
	case res := <-ch:
		return res.Err
	}
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
