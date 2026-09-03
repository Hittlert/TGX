package tutil

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-faster/errors"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// ErrMessageDeleted is returned when a message is detected as deleted.
var ErrMessageDeleted = errors.New("message may be deleted")

// ParseMessageLink return dialog id, msg id, error
func ParseMessageLink(ctx context.Context, manager *peers.Manager, s string) (peers.Peer, int, error) {
	parse := func(from, msg string) (peers.Peer, int, error) {
		ch, err := GetInputPeer(ctx, manager, from)
		if err != nil {
			return nil, 0, errors.Wrap(err, "input peer")
		}

		m, err := strconv.Atoi(msg)
		if err != nil {
			return nil, 0, errors.Wrap(err, "parse message id")
		}

		return ch, m, nil
	}

	u, err := url.Parse(s)
	if err != nil {
		return nil, 0, err
	}

	paths := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")

	// https://t.me/opencfdchannel/4434?comment=360409
	if c := u.Query().Get("comment"); c != "" {
		peer, err := GetInputPeer(ctx, manager, paths[0])
		if err != nil {
			return nil, 0, errors.Wrap(err, "input peer")
		}

		ch, ok := peer.(peers.Channel)
		if !ok || !ch.IsBroadcast() {
			return nil, 0, errors.New("not channel")
		}

		raw, err := ch.FullRaw(ctx)
		if err != nil {
			return nil, 0, errors.Wrap(err, "full raw")
		}

		linked, ok := raw.GetLinkedChatID()
		if !ok {
			return nil, 0, errors.New("no linked chat")
		}

		return parse(strconv.FormatInt(linked, 10), c)
	}

	switch len(paths) {
	case 2:
		// https://t.me/telegram/193
		// https://t.me/myhostloc/1485524?thread=1485523
		return parse(paths[0], paths[1])
	case 3:
		// https://t.me/c/1697797156/151
		// https://t.me/iFreeKnow/45662/55005
		if paths[0] == "c" {
			return parse(paths[1], paths[2])
		}

		// "45662" means topic id, we don't need it
		return parse(paths[0], paths[2])
	case 4:
		// https://t.me/c/1492447836/251015/251021
		if paths[0] != "c" {
			return nil, 0, fmt.Errorf("invalid message link")
		}

		// "251015" means topic id, we don't need it
		return parse(paths[1], paths[3])
	default:
		return nil, 0, fmt.Errorf("invalid message link: %s", s)
	}
}

func GetInputPeer(ctx context.Context, manager *peers.Manager, from string) (peers.Peer, error) {
	from = strings.TrimSpace(from)
	from = strings.TrimPrefix(from, "@")
	if from == "" {
		return nil, errors.New("empty peer identifier")
	}

	// 1. Check if input is a numeric ID (standard integer or legacy Bot API prefix representation)
	var numericID int64
	var isNumeric bool
	if strings.HasPrefix(from, "-100") && len(from) > 4 {
		if id, err := strconv.ParseInt(from[4:], 10, 64); err == nil {
			numericID = id
			isNumeric = true
		}
	} else if id, err := strconv.ParseInt(from, 10, 64); err == nil {
		if id < 0 {
			numericID = -id
		} else {
			numericID = id
		}
		isNumeric = true
	}

	if isNumeric {
		// Resolve via authoritative typed peers.Manager methods
		if p, err := manager.ResolveChannelID(ctx, numericID); err == nil {
			return p, nil
		}
		if p, err := manager.ResolveChatID(ctx, numericID); err == nil {
			return p, nil
		}
		if p, err := manager.ResolveUserID(ctx, numericID); err == nil {
			return p, nil
		}
	}

	// 2. Resolve username / domain via manager
	p, err := manager.Resolve(ctx, from)
	if err == nil {
		return p, nil
	}

	if isNumeric {
		return nil, fmt.Errorf("failed to resolve peer from id %d: %w", numericID, err)
	}
	return nil, fmt.Errorf("failed to resolve peer %s: %w", from, err)
}

func GetPeerID(peer tg.PeerClass) int64 {
	switch p := peer.(type) {
	case *tg.PeerUser:
		return p.UserID
	case *tg.PeerChat:
		return p.ChatID
	case *tg.PeerChannel:
		return p.ChannelID
	}
	return 0
}

func GetInputPeerID(peer tg.InputPeerClass) int64 {
	switch p := peer.(type) {
	case *tg.InputPeerUser:
		return p.UserID
	case *tg.InputPeerChat:
		return p.ChatID
	case *tg.InputPeerChannel:
		return p.ChannelID
	}

	return 0
}

func GetBlockedDialogs(ctx context.Context, client *tg.Client) (map[int64]struct{}, error) {
	blocks, err := query.GetBlocked(client).BatchSize(100).Collect(ctx)
	if err != nil {
		return nil, err
	}

	blockids := make(map[int64]struct{})
	for _, b := range blocks {
		blockids[GetPeerID(b.Contact.PeerID)] = struct{}{}
	}
	return blockids, nil
}

func FileExists(msg tg.MessageClass) bool {
	m, ok := msg.(*tg.Message)
	if !ok {
		return false
	}

	md, ok := m.GetMedia()
	if !ok {
		return false
	}

	switch md.(type) {
	case *tg.MessageMediaDocument, *tg.MessageMediaPhoto:
		return true
	default:
		return false
	}
}

func isDefinitiveError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if _, isFlood := tgerr.AsFloodWait(err); isFlood {
		return true
	}
	if tgerr.Is(err,
		"CHANNEL_PRIVATE",
		"CHANNEL_INVALID",
		"CHAT_ADMIN_REQUIRED",
		"CHAT_WRITE_FORBIDDEN",
		"USER_BANNED_IN_CHANNEL",
		"PEER_ID_INVALID",
		"AUTH_KEY_UNREGISTERED",
		"SESSION_REVOKED",
		"SESSION_EXPIRED",
	) {
		return true
	}
	return false
}

func GetSingleMessage(ctx context.Context, c *tg.Client, peer tg.InputPeerClass, msg int) (*tg.Message, error) {
	// 1. Direct exact message ID lookup (robust against bot messages, topic shifts, and deleted gaps)
	var directErr error
	switch p := peer.(type) {
	case *tg.InputPeerChannel:
		req := &tg.ChannelsGetMessagesRequest{
			Channel: &tg.InputChannel{ChannelID: p.ChannelID, AccessHash: p.AccessHash},
			ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: msg}},
		}
		res, err := c.ChannelsGetMessages(ctx, req)
		if err == nil {
			var messageList []tg.MessageClass
			switch msgs := res.(type) {
			case *tg.MessagesChannelMessages:
				messageList = msgs.Messages
			case *tg.MessagesMessagesSlice:
				messageList = msgs.Messages
			case *tg.MessagesMessages:
				messageList = msgs.Messages
			}
			for _, mClass := range messageList {
				if m, ok := mClass.(*tg.Message); ok && m.ID == msg {
					return m, nil
				}
			}
		} else {
			directErr = err
			if isDefinitiveError(err) {
				return nil, err
			}
		}
	default:
		req := []tg.InputMessageClass{&tg.InputMessageID{ID: msg}}
		res, err := c.MessagesGetMessages(ctx, req)
		if err == nil {
			switch msgs := res.(type) {
			case *tg.MessagesMessages:
				for _, mClass := range msgs.Messages {
					if m, ok := mClass.(*tg.Message); ok && m.ID == msg {
						return m, nil
					}
				}
			case *tg.MessagesMessagesSlice:
				for _, mClass := range msgs.Messages {
					if m, ok := mClass.(*tg.Message); ok && m.ID == msg {
						return m, nil
					}
				}
			case *tg.MessagesChannelMessages:
				for _, mClass := range msgs.Messages {
					if m, ok := mClass.(*tg.Message); ok && m.ID == msg {
						return m, nil
					}
				}
			}
		} else {
			directErr = err
			if isDefinitiveError(err) {
				return nil, err
			}
		}
	}

	// 2. Fallback to GetHistory iterator if direct lookup did not return the message
	it := query.Messages(c).
		GetHistory(peer).OffsetID(msg + 1).
		BatchSize(1).Iter()

	if !it.Next(ctx) {
		if it.Err() != nil {
			return nil, errors.Wrap(it.Err(), "get single message")
		}
		if directErr != nil {
			return nil, errors.Wrap(directErr, "get single message direct")
		}
		return nil, fmt.Errorf("the message %d/%d: %w", GetInputPeerID(peer), msg, ErrMessageDeleted)
	}

	m, ok := it.Value().Msg.(*tg.Message)
	if !ok {
		return nil, errors.Errorf("invalid message %d", msg)
	}

	// check if message is deleted
	if m.GetID() != msg {
		return nil, fmt.Errorf("the message %d/%d: %w", GetInputPeerID(peer), msg, ErrMessageDeleted)
	}

	return m, nil
}

// GetMessagesBatch fetches a batch of message IDs in a single RPC (up to 100 per chunk), aggregated by peer.
func GetMessagesBatch(ctx context.Context, c *tg.Client, peer tg.InputPeerClass, msgIDs []int) (map[int]*tg.Message, error) {
	if len(msgIDs) == 0 {
		return make(map[int]*tg.Message), nil
	}
	result := make(map[int]*tg.Message)

	for i := 0; i < len(msgIDs); i += 100 {
		end := i + 100
		if end > len(msgIDs) {
			end = len(msgIDs)
		}
		chunk := msgIDs[i:end]

		var inputIDs []tg.InputMessageClass
		for _, id := range chunk {
			inputIDs = append(inputIDs, &tg.InputMessageID{ID: id})
		}

		switch p := peer.(type) {
		case *tg.InputPeerChannel:
			req := &tg.ChannelsGetMessagesRequest{
				Channel: &tg.InputChannel{ChannelID: p.ChannelID, AccessHash: p.AccessHash},
				ID:      inputIDs,
			}
			res, err := c.ChannelsGetMessages(ctx, req)
			if err != nil {
				return nil, err
			}
			switch msgs := res.(type) {
			case *tg.MessagesChannelMessages:
				for _, mClass := range msgs.Messages {
					if m, ok := mClass.(*tg.Message); ok {
						result[m.ID] = m
					}
				}
			case *tg.MessagesMessagesSlice:
				for _, mClass := range msgs.Messages {
					if m, ok := mClass.(*tg.Message); ok {
						result[m.ID] = m
					}
				}
			case *tg.MessagesMessages:
				for _, mClass := range msgs.Messages {
					if m, ok := mClass.(*tg.Message); ok {
						result[m.ID] = m
					}
				}
			}
		default:
			res, err := c.MessagesGetMessages(ctx, inputIDs)
			if err != nil {
				return nil, err
			}
			switch msgs := res.(type) {
			case *tg.MessagesMessages:
				for _, mClass := range msgs.Messages {
					if m, ok := mClass.(*tg.Message); ok {
						result[m.ID] = m
					}
				}
			case *tg.MessagesMessagesSlice:
				for _, mClass := range msgs.Messages {
					if m, ok := mClass.(*tg.Message); ok {
						result[m.ID] = m
					}
				}
			case *tg.MessagesChannelMessages:
				for _, mClass := range msgs.Messages {
					if m, ok := mClass.(*tg.Message); ok {
						result[m.ID] = m
					}
				}
			}
		}
	}
	return result, nil
}

type Messages []*tg.Message

func (m Messages) Len() int {
	return len(m)
}

func (m Messages) Less(i, j int) bool {
	return m[i].ID < m[j].ID
}

func (m Messages) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func GetGroupedMessages(ctx context.Context, c *tg.Client, peer tg.InputPeerClass, msg *tg.Message) ([]*tg.Message, error) {
	group, ok := msg.GetGroupedID()
	if !ok {
		return nil, errors.New("not grouped message")
	}
	// https://telegram.org/blog/albums-saved-messages
	// Each album can include up to 10 photos or videos
	batchSize := 20

	it := query.Messages(c).GetHistory(peer).
		OffsetID(msg.ID + 11). // from latest to oldest
		BatchSize(batchSize).Iter()

	messages := make([]*tg.Message, 0, batchSize)
	for i := 0; it.Next(ctx) && i < batchSize; i++ {
		m, ok := it.Value().Msg.(*tg.Message)
		if !ok {
			continue
		}
		groupID, ok := m.GetGroupedID()
		if !ok {
			continue
		}
		if groupID != group {
			continue
		}

		// append argument msg to the end of messages because of it may have been modified.
		// Like forward edit flag.
		if m.ID == msg.ID {
			messages = append(messages, msg)
		} else {
			messages = append(messages, m)
		}
	}

	// reverse messages from oldest to latest, so we can forward them in order
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}

	return messages, nil
}

var threadsLevels = []struct {
	threads int
	size    int64
}{
	{1, 2 << 20},
	{2, 10 << 20},
	{4, 50 << 20},
	{8, 200 << 20},
}

func BestThreads(size int64, max int) int {
	// Get best threads num for download, based on file size
	for _, thread := range threadsLevels {
		if size < thread.size {
			return min(thread.threads, max)
		}
	}
	return max
}
