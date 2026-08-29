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

	invokers map[int]tg.Invoker
	closes   map[int]func() error
	takeout  int64
}

func NewPool(c *telegram.Client, size int64, middlewares ...telegram.Middleware) Pool {
	return &pool{
		api:         c,
		size:        size,
		mu:          &sync.Mutex{},
		middlewares: middlewares,
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
	if dc == p.current() { // can't transfer dc to current dc
		invoker, err = p.api.Pool(p.size)
	} else {
		// Foreign DC: keep pool size compact (max 2) to prevent auth.exportAuthorization flood on primary DC
		foreignPoolSize := int64(2)
		if p.size > 0 && p.size < foreignPoolSize {
			foreignPoolSize = p.size
		}
		for attempt := 0; attempt < 5; attempt++ {
			invoker, err = p.api.DC(context.Background(), dc, foreignPoolSize)
			if err == nil {
				break
			}
			if d, isFlood := tgerr.AsFloodWait(err); isFlood {
				logctx.From(ctx).Warn("DC transfer flood wait, backing off", zap.Int("dc", dc), zap.Duration("wait", d))
				time.Sleep(d + 1*time.Second)
				continue
			}
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
		}
	}

	if err != nil {
		logctx.From(ctx).Error("create invoker", zap.Int("dc_id", dc), zap.Error(err))
		return failedInvoker{dc: dc, err: err}
	}

	p.closes[dc] = invoker.Close
	p.invokers[dc] = chainMiddlewares(invoker, p.middlewares...)

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
			// ignore init delay error and return non-takeout client
			return p.Client(ctx, dc)
		}
		p.takeout = sid
		logctx.From(ctx).Info("get takeout id", zap.Int64("id", sid))
	}

	return tg.NewClient(chainMiddlewares(p.invoker(ctx, dc), takeout.Middleware(p.takeout)))
}
