package transfer

import (
	"context"
	"io"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// MasterClientAdapter adapts a downloader.Client into gotd's downloader.Client,
// routing physical requests through the shared DataGate.
type MasterClientAdapter struct {
	client downloader.Client
	gate   *DataGate
	dcID   int
}

// NewMasterClientAdapter wraps a client for the given DC ID with DataGate protection.
func NewMasterClientAdapter(client downloader.Client, gate *DataGate, dcID int) *MasterClientAdapter {
	return &MasterClientAdapter{
		client: client,
		gate:   gate,
		dcID:   dcID,
	}
}

func (m *MasterClientAdapter) UploadGetFile(ctx context.Context, request *tg.UploadGetFileRequest) (tg.UploadFileClass, error) {
	if m.gate != nil {
		release, err := m.gate.Acquire(ctx, m.dcID)
		if err != nil {
			return nil, err
		}
		defer release()
	}

	res, err := m.client.UploadGetFile(ctx, request)
	if d, isFlood := tgerr.AsFloodWait(err); isFlood && m.gate != nil {
		m.gate.TriggerFloodWait(m.dcID, d)
	}
	return res, err
}

func (m *MasterClientAdapter) UploadGetFileHashes(ctx context.Context, request *tg.UploadGetFileHashesRequest) ([]tg.FileHash, error) {
	if m.gate != nil {
		release, err := m.gate.Acquire(ctx, m.dcID)
		if err != nil {
			return nil, err
		}
		defer release()
	}

	res, err := m.client.UploadGetFileHashes(ctx, request)
	if d, isFlood := tgerr.AsFloodWait(err); isFlood && m.gate != nil {
		m.gate.TriggerFloodWait(m.dcID, d)
	}
	return res, err
}

func (m *MasterClientAdapter) UploadReuploadCDNFile(ctx context.Context, request *tg.UploadReuploadCDNFileRequest) ([]tg.FileHash, error) {
	if m.gate != nil {
		release, err := m.gate.Acquire(ctx, m.dcID)
		if err != nil {
			return nil, err
		}
		defer release()
	}

	res, err := m.client.UploadReuploadCDNFile(ctx, request)
	if d, isFlood := tgerr.AsFloodWait(err); isFlood && m.gate != nil {
		m.gate.TriggerFloodWait(m.dcID, d)
	}
	return res, err
}

func (m *MasterClientAdapter) UploadGetCDNFileHashes(ctx context.Context, request *tg.UploadGetCDNFileHashesRequest) ([]tg.FileHash, error) {
	if m.gate != nil {
		release, err := m.gate.Acquire(ctx, m.dcID)
		if err != nil {
			return nil, err
		}
		defer release()
	}

	res, err := m.client.UploadGetCDNFileHashes(ctx, request)
	if d, isFlood := tgerr.AsFloodWait(err); isFlood && m.gate != nil {
		m.gate.TriggerFloodWait(m.dcID, d)
	}
	return res, err
}

func (m *MasterClientAdapter) UploadGetWebFile(ctx context.Context, request *tg.UploadGetWebFileRequest) (*tg.UploadWebFile, error) {
	if m.gate != nil {
		release, err := m.gate.Acquire(ctx, m.dcID)
		if err != nil {
			return nil, err
		}
		defer release()
	}

	res, err := m.client.UploadGetWebFile(ctx, request)
	if d, isFlood := tgerr.AsFloodWait(err); isFlood && m.gate != nil {
		m.gate.TriggerFloodWait(m.dcID, d)
	}
	return res, err
}

// CDNClientAdapter wraps a CDN invoker with DataGate protection.
type CDNClientAdapter struct {
	client tg.Invoker
	gate   *DataGate
	cdnDC  int
}

func (c *CDNClientAdapter) UploadGetCDNFile(ctx context.Context, request *tg.UploadGetCDNFileRequest) (tg.UploadCDNFileClass, error) {
	if c.gate != nil {
		release, err := c.gate.Acquire(ctx, c.cdnDC)
		if err != nil {
			return nil, err
		}
		defer release()
	}

	res, err := tg.NewClient(c.client).UploadGetCDNFile(ctx, request)
	if d, isFlood := tgerr.AsFloodWait(err); isFlood && c.gate != nil {
		c.gate.TriggerFloodWait(c.cdnDC, d)
	}
	return res, err
}

// RawCDNProvider allows creating a CDN invoker.
type RawCDNProvider interface {
	CDN(ctx context.Context, dc int, max int64) (downloader.CDN, io.Closer, error)
}

// CDNProviderAdapter implements gotd's downloader.CDNProvider.
type CDNProviderAdapter struct {
	gate     *DataGate
	provider func(ctx context.Context, dc int, max int64) (tg.Invoker, io.Closer, error)
}

// NewCDNProviderAdapter creates a CDNProviderAdapter.
func NewCDNProviderAdapter(gate *DataGate, provider func(ctx context.Context, dc int, max int64) (tg.Invoker, io.Closer, error)) *CDNProviderAdapter {
	return &CDNProviderAdapter{
		gate:     gate,
		provider: provider,
	}
}

func (p *CDNProviderAdapter) CDN(ctx context.Context, dc int, max int64) (downloader.CDN, io.Closer, error) {
	if p.provider == nil {
		return nil, nil, nil
	}
	invoker, closer, err := p.provider(ctx, dc, max)
	if err != nil {
		return nil, nil, err
	}
	cdnClient := &CDNClientAdapter{
		client: invoker,
		gate:   p.gate,
		cdnDC:  dc,
	}
	return cdnClient, closer, nil
}
