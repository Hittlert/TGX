package dcpool

import (
	"context"
	"fmt"
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
	Close() error
}

type pool struct {
	api         *telegram.Client
	size        int64
	mu          *sync.Mutex
	middlewares []telegram.Middleware
	floodGate   *gate.FloodGate

	invokers map[int]tg.Invoker
	closes   map[int]func() error
	takeout  int64
}

func NewPool(c *telegram.Client, size int64, middlewares ...telegram.Middleware) Pool {
	return NewPoolWithGate(c, size, nil, middlewares...)
}

func NewPoolWithGate(c *telegram.Client, size int64, fg *gate.FloodGate, middlewares ...telegram.Middleware) Pool {
	return &pool{
		api:         c,
		size:        size,
		mu:          &sync.Mutex{},
		middlewares: middlewares,
		floodGate:   fg,
		invokers:    make(map[int]tg.Invoker),
		closes:      make(map[int]func() error),
	}
}

func (p *pool) current() int {
	return p.api.Config().ThisDC
}

func (p *pool) Client(ctx context.Context, dc int) *tg.Client {
	p.mu.Lock()
	defer p.mu.Unlock()

	return tg.NewClient(p.invoker(ctx, dc))
}

func (p *pool) invoker(ctx context.Context, dc int) tg.Invoker {
	// self-hosted Telegram server can't properly handle pooling connections,
	// so directly return original client
	if testMode {
		return p.api
	}

	if i, ok := p.invokers[dc]; ok {
		return i
	}

	// lazy init
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
			select {
			case <-ctx.Done():
				return failedInvoker{dc: dc, err: ctx.Err()}
			case <-time.After(time.Duration(attempt+1) * 500 * time.Millisecond):
			}
		}
	}

	if err != nil {
		logctx.From(ctx).Error("create invoker", zap.Int("dc_id", dc), zap.Error(err))
		return failedInvoker{dc: dc, err: err}
	}

	p.closes[dc] = invoker.Close
	mws := p.middlewares
	if p.floodGate != nil {
		mws = append([]telegram.Middleware{p.floodGate.Middleware(dc)}, mws...)
	}
	p.invokers[dc] = chainMiddlewares(invoker, mws...)

	return p.invokers[dc]
}

type failedInvoker struct {
	dc  int
	err error
}

func (f failedInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	return fmt.Errorf("DC %d connection failed: %w", f.dc, f.err)
}

func (p *pool) Default(ctx context.Context) *tg.Client {
	return p.Client(ctx, p.current())
}

func (p *pool) Close() (err error) {
	if p.takeout != 0 {
		err = takeout.UnTakeout(context.TODO(), p.Takeout(context.TODO(), p.current()).Invoker())
	}

	for _, c := range p.closes {
		err = multierr.Append(err, c())
	}

	return err
}

func (p *pool) Takeout(ctx context.Context, dc int) *tg.Client {
	p.mu.Lock()
	defer p.mu.Unlock()

	// lazy init
	if p.takeout == 0 {
		sid, err := takeout.Takeout(ctx, p.api)
		if err != nil {
			logctx.From(ctx).Warn("takeout error", zap.Error(err))
			// ignore init delay error and return non-takeout client without re-acquiring p.mu
			return tg.NewClient(p.invoker(ctx, dc))
		}
		p.takeout = sid
		logctx.From(ctx).Info("get takeout id", zap.Int64("id", sid))
	}

	return tg.NewClient(chainMiddlewares(p.invoker(ctx, dc), takeout.Middleware(p.takeout)))
}
