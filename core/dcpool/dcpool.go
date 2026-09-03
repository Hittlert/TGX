package dcpool

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/Hittlert/TGX/core/logctx"
	"github.com/Hittlert/TGX/core/middlewares/takeout"
	"github.com/Hittlert/TGX/pkg/sbe/gate"
)

var testMode = false

// EnableTestMode enables test mode, which disables takeout and pooling and directly returns original client.
func EnableTestMode() {
	testMode = true
}

type Pool interface {
	Client(ctx context.Context, dc int) *tg.Client
	Takeout(ctx context.Context, dc int) *tg.Client
	Default(ctx context.Context) *tg.Client
	CDN(ctx context.Context, dc int, max int64) (tg.Invoker, io.Closer, error)
	Close() error
}

type pool struct {
	api         *telegram.Client
	size        int64
	mu          sync.RWMutex
	dcLocks     map[int]*sync.Mutex
	dcLocksMu   sync.Mutex
	middlewares []telegram.Middleware
	floodGate   *gate.FloodGate

	invokers   map[int]tg.Invoker
	closes     map[int]func() error
	dcFailures map[int]time.Time
	takeout    int64
}

func NewPool(c *telegram.Client, size int64, middlewares ...telegram.Middleware) Pool {
	return NewPoolWithGate(c, size, nil, middlewares...)
}

func NewPoolWithGate(c *telegram.Client, size int64, fg *gate.FloodGate, middlewares ...telegram.Middleware) Pool {
	return &pool{
		api:         c,
		size:        size,
		dcLocks:     make(map[int]*sync.Mutex),
		middlewares: middlewares,
		floodGate:   fg,
		invokers:    make(map[int]tg.Invoker),
		closes:      make(map[int]func() error),
		dcFailures:  make(map[int]time.Time),
	}
}

func (p *pool) current() int {
	return p.api.Config().ThisDC
}

func (p *pool) getDCLock(dc int) *sync.Mutex {
	p.dcLocksMu.Lock()
	defer p.dcLocksMu.Unlock()
	if l, ok := p.dcLocks[dc]; ok {
		return l
	}
	l := &sync.Mutex{}
	p.dcLocks[dc] = l
	return l
}

func (p *pool) Client(ctx context.Context, dc int) *tg.Client {
	return tg.NewClient(p.invoker(ctx, dc))
}

func (p *pool) invoker(ctx context.Context, dc int) tg.Invoker {
	// self-hosted Telegram server can't properly handle pooling connections,
	// so directly return original client
	if testMode {
		return p.api
	}

	p.mu.RLock()
	if i, ok := p.invokers[dc]; ok {
		p.mu.RUnlock()
		return i
	}
	if failTime, ok := p.dcFailures[dc]; ok && time.Since(failTime) < 5*time.Second {
		p.mu.RUnlock()
		return failedInvoker{dc: dc, err: fmt.Errorf("foreign DC %d in connection cooldown", dc)}
	}
	p.mu.RUnlock()

	dcLock := p.getDCLock(dc)
	dcLock.Lock()
	defer dcLock.Unlock()

	p.mu.RLock()
	if i, ok := p.invokers[dc]; ok {
		p.mu.RUnlock()
		return i
	}
	if failTime, ok := p.dcFailures[dc]; ok && time.Since(failTime) < 5*time.Second {
		p.mu.RUnlock()
		return failedInvoker{dc: dc, err: fmt.Errorf("foreign DC %d in connection cooldown", dc)}
	}
	p.mu.RUnlock()

	var (
		invoker telegram.CloseInvoker
		err     error
	)
	if dc == 0 || dc == p.current() {
		invoker, err = p.api.Pool(p.size)
	} else {
		for attempt := 0; attempt < 5; attempt++ {
			if ctx.Err() != nil {
				return failedInvoker{dc: dc, err: ctx.Err()}
			}
			invoker, err = p.api.DC(ctx, dc, p.size)
			if err == nil {
				break
			}
			if d, isFlood := tgerr.AsFloodWait(err); isFlood {
				logctx.From(ctx).Warn("DC transfer flood wait, backing off", zap.Int("dc", dc), zap.Duration("wait", d))
				if p.floodGate != nil {
					p.floodGate.TriggerFloodWait(dc, d)
				}
				select {
				case <-ctx.Done():
					return failedInvoker{dc: dc, err: ctx.Err()}
				case <-time.After(d + 1*time.Second):
				}
				continue
			}
			if p.floodGate != nil {
				p.floodGate.TriggerTransportError(err)
			}
			select {
			case <-ctx.Done():
				return failedInvoker{dc: dc, err: ctx.Err()}
			case <-time.After(time.Duration(attempt+1) * 500 * time.Millisecond):
			}
		}
	}

	if err != nil {
		p.mu.Lock()
		p.dcFailures[dc] = time.Now()
		p.mu.Unlock()
		if p.floodGate != nil {
			p.floodGate.TriggerTransportError(err)
		}
		logctx.From(ctx).Error("create invoker", zap.Int("dc_id", dc), zap.Error(err))
		return failedInvoker{dc: dc, err: err}
	}

	p.mu.Lock()
	delete(p.dcFailures, dc)
	p.closes[dc] = invoker.Close
	mws := p.middlewares
	if p.floodGate != nil {
		mws = append([]telegram.Middleware{p.floodGate.Middleware(dc)}, mws...)
	}
	p.invokers[dc] = chainMiddlewares(invoker, mws...)
	res := p.invokers[dc]
	p.mu.Unlock()

	return res
}

type failedInvoker struct {
	dc  int
	err error
}

func (f failedInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	return fmt.Errorf("DC %d connection failed: %w", f.dc, f.err)
}

func (p *pool) Default(ctx context.Context) *tg.Client {
	if p.floodGate != nil {
		return tg.NewClient(chainMiddlewares(p.api, p.floodGate.Middleware(p.current())))
	}
	return tg.NewClient(p.api)
}

func (p *pool) Close() (err error) {
	p.mu.Lock()
	takeoutID := p.takeout
	closes := make([]func() error, 0, len(p.closes))
	for _, c := range p.closes {
		closes = append(closes, c)
	}
	p.mu.Unlock()

	if takeoutID != 0 {
		err = takeout.UnTakeout(context.TODO(), p.Takeout(context.TODO(), p.current()).Invoker())
	}

	for _, c := range closes {
		err = multierr.Append(err, c())
	}

	return err
}

func (p *pool) Takeout(ctx context.Context, dc int) *tg.Client {
	if testMode {
		return tg.NewClient(p.invoker(ctx, dc))
	}

	p.mu.Lock()
	if p.takeout == 0 {
		sid, err := takeout.Takeout(ctx, p.api)
		if err != nil {
			logctx.From(ctx).Warn("takeout error", zap.Error(err))
			p.mu.Unlock()
			return tg.NewClient(p.invoker(ctx, dc))
		}
		p.takeout = sid
		logctx.From(ctx).Info("get takeout id", zap.Int64("id", sid))
	}
	takeoutID := p.takeout
	p.mu.Unlock()

	return tg.NewClient(chainMiddlewares(p.invoker(ctx, dc), takeout.Middleware(takeoutID)))
}

func (p *pool) CDN(ctx context.Context, dc int, max int64) (tg.Invoker, io.Closer, error) {
	if p.api == nil {
		return nil, nil, fmt.Errorf("api client is nil")
	}
	closeInvoker, err := p.api.CDN(ctx, dc, max)
	if err != nil {
		return nil, nil, err
	}
	return closeInvoker, closeInvoker, nil
}

