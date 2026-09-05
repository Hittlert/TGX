package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// InvocationError directly and authoritatively binds a physical invocation attempt ID and range label
// to an error returned from an RPC call, eliminating concurrent key mismatch or race conditions.
type InvocationError struct {
	Err               error
	PhysicalAttemptID string
	RangeLabel        string
}

func (e *InvocationError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *InvocationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ExtractPhysicalAttempt retrieves the authoritative physical invocation identity and range label from an error.
func ExtractPhysicalAttempt(err error) (physicalID string, rangeLabel string, ok bool) {
	var invErr *InvocationError
	if errors.As(err, &invErr) {
		return invErr.PhysicalAttemptID, invErr.RangeLabel, true
	}
	return "", "", false
}

// CDNProviderFunc defines the function signature for acquiring a CDN invoker.
type CDNProviderFunc func(ctx context.Context, dc int, max int64) (tg.Invoker, io.Closer, error)

// GatedInvoker wraps a tg.Invoker with DataGate in-flight RPC limits and FloodWait tracking.
type GatedInvoker struct {
	raw  tg.Invoker
	gate *DataGate
	dcID int
}

// NewGatedInvoker creates an invoker wrapped with DataGate protection.
func NewGatedInvoker(invoker tg.Invoker, gate *DataGate, dcID int) *GatedInvoker {
	return &GatedInvoker{
		raw:  invoker,
		gate: gate,
		dcID: dcID,
	}
}

type physAttemptCtxKey struct{}

// ContextWithPhysicalAttemptID wraps ctx with the authoritative physical attempt ID.
func ContextWithPhysicalAttemptID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, physAttemptCtxKey{}, id)
}

// PhysicalAttemptIDFromContext extracts the authoritative physical attempt ID from ctx.
func PhysicalAttemptIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(physAttemptCtxKey{}).(string); ok {
		return v
	}
	return ""
}

// extractRequestRange extracts the canonical range identifier and range bounds from an RPC input encoder.
func extractRequestRange(input bin.Encoder) (string, int64, int) {
	if input == nil {
		return "rpc", 0, 0
	}
	switch req := input.(type) {
	case *tg.UploadGetFileRequest:
		return fmt.Sprintf("chunk-%d", req.Offset), req.Offset, req.Limit
	case *tg.UploadGetCDNFileRequest:
		return fmt.Sprintf("cdn-%d", req.Offset), req.Offset, req.Limit
	default:
		return "rpc", 0, 0
	}
}

// Invoke implements tg.Invoker, enforcing the global in-flight limit, FloodWait cooldowns,
// request counts, and wire byte accounting.
func (g *GatedInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	tc, hasTask := TransferTaskFromContext(ctx)
	var physID string
	var rangeLabel string
	if hasTask {
		rangeLabel, _, _ = extractRequestRange(input)
		physID, _ = tc.NextInvocationID(rangeLabel)
	}

	if hasTask && tc.RequestCount != nil {
		for {
			cur := atomic.LoadInt64(tc.RequestCount)
			if tc.RequestBudget > 0 && cur >= tc.RequestBudget {
				if hasTask {
					tc.RecordFailedAttempt(rangeLabel, physID, ErrRequestBudgetExhausted)
				}
				if physID != "" {
					return &InvocationError{
						Err:               ErrRequestBudgetExhausted,
						PhysicalAttemptID: physID,
						RangeLabel:        rangeLabel,
					}
				}
				return ErrRequestBudgetExhausted
			}
			if atomic.CompareAndSwapInt64(tc.RequestCount, cur, cur+1) {
				break
			}
		}
	}

	if g.gate != nil {
		release, err := g.gate.Acquire(ctx, g.dcID)
		if err != nil {
			if hasTask {
				tc.RecordFailedAttempt(rangeLabel, physID, err)
			}
			if physID != "" {
				return &InvocationError{
					Err:               err,
					PhysicalAttemptID: physID,
					RangeLabel:        rangeLabel,
				}
			}
			return err
		}
		defer release()
	}

	invokeCtx := ctx
	if hasTask && physID != "" {
		invokeCtx = ContextWithPhysicalAttemptID(ctx, physID)
	}

	err := g.raw.Invoke(invokeCtx, input, output)
	if hasTask {
		if err != nil {
			tc.RecordFailedAttempt(rangeLabel, physID, err)
		}
		if tc.OnInvocation != nil {
			tc.OnInvocation(physID, err)
		}
	}
	if d, isFlood := tgerr.AsFloodWait(err); isFlood && g.gate != nil {
		g.gate.TriggerFloodWait(g.dcID, d)
	}
	if hasTask && tc.WireBytes != nil {
		if b := extractWireBytes(output); b > 0 {
			atomic.AddInt64(tc.WireBytes, b)
		}
	}
	if err != nil && physID != "" {
		return &InvocationError{
			Err:               err,
			PhysicalAttemptID: physID,
			RangeLabel:        rangeLabel,
		}
	}
	return err
}

// extractWireBytes returns the payload size in bytes delivered in an RPC response.
func extractWireBytes(output bin.Decoder) int64 {
	if output == nil {
		return 0
	}
	switch v := output.(type) {
	case *tg.UploadFile:
		return int64(len(v.Bytes))
	case *tg.UploadFileBox:
		if v != nil && v.File != nil {
			if f, ok := v.File.(*tg.UploadFile); ok && f != nil {
				return int64(len(f.Bytes))
			}
		}
	case interface{ GetBytes() []byte }:
		return int64(len(v.GetBytes()))
	}
	return 0
}

// GatedClient wraps a *tg.Client with gotd downloader.CDNProvider support.
type GatedClient struct {
	*tg.Client
	gate        *DataGate
	cdnProvider CDNProviderFunc
}

var (
	_ downloader.Client      = (*GatedClient)(nil)
	_ downloader.CDNProvider = (*GatedClient)(nil)
)

// NewGatedClient creates a thin gated downloader client with optional CDN support.
func NewGatedClient(invoker tg.Invoker, gate *DataGate, dcID int, cdn CDNProviderFunc) *GatedClient {
	gatedInv := NewGatedInvoker(invoker, gate, dcID)
	return &GatedClient{
		Client:      tg.NewClient(gatedInv),
		gate:        gate,
		cdnProvider: cdn,
	}
}

// CDN implements downloader.CDNProvider for gotd official downloader.
func (c *GatedClient) CDN(ctx context.Context, dc int, max int64) (downloader.CDN, io.Closer, error) {
	if c.cdnProvider == nil {
		return nil, nil, nil
	}
	invoker, closer, err := c.cdnProvider(ctx, dc, max)
	if err != nil {
		return nil, nil, err
	}
	gatedCDN := NewGatedInvoker(invoker, c.gate, dc)
	return tg.NewClient(gatedCDN), closer, nil
}

// MasterClientAdapter is kept as an alias to GatedClient for backward compatibility.
type MasterClientAdapter = GatedClient

// NewMasterClientAdapterWithCDN creates a GatedClient.
func NewMasterClientAdapterWithCDN(invoker tg.Invoker, gate *DataGate, dcID int, cdn CDNProviderFunc) *GatedClient {
	return NewGatedClient(invoker, gate, dcID, cdn)
}
