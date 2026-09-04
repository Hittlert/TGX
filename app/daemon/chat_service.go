package daemon

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Hittlert/TGX/core/tmedia"
	"github.com/Hittlert/TGX/core/util/tutil"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/telegram/query/dialogs"
	"github.com/gotd/td/tg"
)

type DialogDTO struct {
	ID           int64  `json:"id"`
	ChatID       string `json:"chat_id"`
	Title        string `json:"title"`
	Username     string `json:"username"`
	Type         string `json:"type"`
	Pinned       bool   `json:"pinned"`
	UnreadCount  int    `json:"unread_count"`
	TopMessageID int    `json:"top_message_id"`
}

type MessageDTO struct {
	ID               int    `json:"id"`
	ChatID           string `json:"chat_id"`
	Text             string `json:"text"`
	SenderID         string `json:"sender_id"`
	SenderName       string `json:"sender_name"`
	MediaType        string `json:"media_type"`
	HasMedia         bool   `json:"has_media"`
	FileName         string `json:"file_name,omitempty"`
	FileSize         int64  `json:"file_size,omitempty"`
	ReplyToMessageID int    `json:"reply_to_message_id,omitempty"`
	MediaGroupID     string `json:"media_group_id,omitempty"`
	Date             int64  `json:"date"`
}

type HistoryRequest struct {
	Peer     string `json:"peer"`
	OffsetID int    `json:"offset_id"`
	Limit    int    `json:"limit"`
	Reverse  bool   `json:"reverse"`
}

type ResolveRequest struct {
	Query string `json:"query"`
}

func (a *telegramMediaAccess) GetDialogs(ctx context.Context) ([]DialogDTO, error) {
	var result []DialogDTO
	seenUsers := make(map[string]tg.UserClass)
	seenChats := make(map[string]tg.ChatClass)

	err := query.GetDialogs(a.pool.Default(ctx)).BatchSize(100).ForEach(ctx, func(ctx context.Context, elem dialogs.Elem) error {
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
			if applyErr := a.manager.Apply(ctx, newUsers, newChats); applyErr != nil {
				return applyErr
			}
		}
		id := tutil.GetInputPeerID(elem.Peer)
		if id == 0 && elem.Dialog != nil {
			if d, ok := elem.Dialog.(*tg.Dialog); ok {
				id = tutil.GetPeerID(d.GetPeer())
			}
		}
		if id == 0 {
			return nil
		}

		item := DialogDTO{
			ID: id,
		}

		var formattedChatID string
		var title, username, chatType string

		if channel, ok := elem.Entities.Channel(id); ok {
			formattedChatID = fmt.Sprintf("-100%d", channel.ID)
			title = channel.Title
			if channel.Username != "" {
				username = channel.Username
			}
			if channel.Megagroup {
				chatType = "supergroup"
			} else {
				chatType = "channel"
			}
		} else if chat, ok := elem.Entities.Chat(id); ok {
			formattedChatID = fmt.Sprintf("-%d", chat.ID)
			title = chat.Title
			chatType = "group"
		} else if user, ok := elem.Entities.User(id); ok {
			formattedChatID = strconv.FormatInt(user.ID, 10)
			nameParts := []string{user.FirstName, user.LastName}
			var cleanParts []string
			for _, p := range nameParts {
				if strings.TrimSpace(p) != "" {
					cleanParts = append(cleanParts, strings.TrimSpace(p))
				}
			}
			title = strings.Join(cleanParts, " ")
			if title == "" {
				title = user.Username
			}
			if user.Username != "" {
				username = user.Username
			}
			if user.Bot {
				chatType = "bot"
			} else {
				chatType = "private"
			}
		} else {
			formattedChatID = strconv.FormatInt(id, 10)
			title = fmt.Sprintf("Chat %d", id)
			chatType = "unknown"
		}

		item.ChatID = formattedChatID
		item.Title = title
		item.Username = username
		item.Type = chatType

		if elem.Dialog != nil {
			if d, ok := elem.Dialog.(*tg.Dialog); ok {
				item.Pinned = d.Pinned
				item.UnreadCount = d.UnreadCount
				item.TopMessageID = d.TopMessage
			}
		}

		result = append(result, item)
		return nil
	})

	if err != nil && len(result) == 0 {
		return nil, err
	}
	if err == nil {
		a.syncMu.Lock()
		a.lastSync = time.Now()
		a.syncMu.Unlock()
	}
	return result, nil
}

func (a *telegramMediaAccess) GetHistory(ctx context.Context, req HistoryRequest) ([]MessageDTO, error) {
	peerStr := normalizePeer(req.Peer)
	resolvedPeer, err := tutil.GetInputPeer(ctx, a.manager, peerStr)
	if err != nil {
		if syncErr := a.SyncPeers(ctx); syncErr == nil {
			resolvedPeer, err = tutil.GetInputPeer(ctx, a.manager, peerStr)
		}
	}
	if err != nil {
		return nil, classifyTelegramError(err, "resolve peer for history")
	}

	limit := req.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	offsetID := req.OffsetID
	addOffset := 0
	minID := 0
	if req.Reverse && offsetID > 0 {
		addOffset = -limit
		minID = req.OffsetID
	}

	rawResp, err := a.pool.Default(ctx).MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
		Peer:      resolvedPeer.InputPeer(),
		OffsetID:  offsetID,
		AddOffset: addOffset,
		Limit:     limit,
		MinID:     minID,
	})
	if err != nil {
		return nil, classifyTelegramError(err, "messages.getHistory")
	}

	var rawMsgs []tg.MessageClass
	switch resp := rawResp.(type) {
	case *tg.MessagesMessages:
		rawMsgs = resp.Messages
	case *tg.MessagesMessagesSlice:
		rawMsgs = resp.Messages
	case *tg.MessagesChannelMessages:
		rawMsgs = resp.Messages
	}

	var messagesList []MessageDTO
	for _, val := range rawMsgs {
		m, ok := val.(*tg.Message)
		if !ok {
			continue
		}

		dto := MessageDTO{
			ID:     m.ID,
			ChatID: req.Peer,
			Text:   m.Message,
			Date:   int64(m.Date),
		}

		if m.FromID != nil {
			dto.SenderID = strconv.FormatInt(tutil.GetPeerID(m.FromID), 10)
		}
		if m.ReplyTo != nil {
			if reply, ok := m.ReplyTo.(*tg.MessageReplyHeader); ok {
				dto.ReplyToMessageID = reply.ReplyToMsgID
			}
		}
		if m.GroupedID != 0 {
			dto.MediaGroupID = strconv.FormatInt(m.GroupedID, 10)
		}

		dto.MediaType = "text"
		if media, ok := tmedia.GetMedia(m); ok && media != nil {
			dto.HasMedia = true
			dto.FileName = media.Name
			dto.FileSize = media.Size
		}

		if m.Media != nil {
			switch mm := m.Media.(type) {
			case *tg.MessageMediaPhoto:
				dto.MediaType = "photo"
				dto.HasMedia = true
			case *tg.MessageMediaDocument:
				dto.MediaType = "document"
				dto.HasMedia = true
				if doc, ok := mm.Document.(*tg.Document); ok {
					for _, attr := range doc.Attributes {
						switch attr.(type) {
						case *tg.DocumentAttributeVideo:
							dto.MediaType = "video"
						case *tg.DocumentAttributeAudio:
							dto.MediaType = "audio"
						case *tg.DocumentAttributeAnimated:
							dto.MediaType = "animation"
						case *tg.DocumentAttributeSticker:
							dto.MediaType = "sticker"
						}
					}
				}
			}
		}

		messagesList = append(messagesList, dto)
	}

	if req.Reverse {
		for i, j := 0, len(messagesList)-1; i < j; i, j = i+1, j-1 {
			messagesList[i], messagesList[j] = messagesList[j], messagesList[i]
		}
	}

	return messagesList, nil
}

func (a *telegramMediaAccess) ResolvePeerInfo(ctx context.Context, queryStr string) (DialogDTO, error) {
	peerStr := normalizePeer(queryStr)
	resolvedPeer, err := tutil.GetInputPeer(ctx, a.manager, peerStr)
	if err != nil {
		if syncErr := a.SyncPeers(ctx); syncErr == nil {
			resolvedPeer, err = tutil.GetInputPeer(ctx, a.manager, peerStr)
		}
	}
	if err != nil {
		return DialogDTO{}, classifyTelegramError(err, "resolve peer info")
	}

	id := resolvedPeer.ID()
	uname, _ := resolvedPeer.Username()
	res := DialogDTO{
		ID:       id,
		Title:    resolvedPeer.VisibleName(),
		Username: uname,
	}
	switch p := resolvedPeer.(type) {
	case peers.Channel:
		res.ChatID = fmt.Sprintf("-100%d", p.ID())
		if p.IsBroadcast() {
			res.Type = "channel"
		} else {
			res.Type = "supergroup"
		}
	case peers.Chat:
		res.ChatID = fmt.Sprintf("-%d", p.ID())
		res.Type = "group"
	case peers.User:
		res.ChatID = strconv.FormatInt(p.ID(), 10)
		if p.Raw().Bot {
			res.Type = "bot"
		} else {
			res.Type = "private"
		}
	default:
		res.ChatID = strconv.FormatInt(id, 10)
		res.Type = "unknown"
	}
	return res, nil
}
