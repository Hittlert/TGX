package tclient

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/gotd/contrib/clock"
	tdclock "github.com/gotd/td/clock"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"
	"golang.org/x/net/proxy"

	"github.com/Hittlert/TGX/core/logctx"
	"github.com/Hittlert/TGX/core/middlewares/recovery"
	"github.com/Hittlert/TGX/core/middlewares/retry"
	"github.com/Hittlert/TGX/core/storage"
	"github.com/Hittlert/TGX/core/util/netutil"
	"github.com/Hittlert/TGX/core/util/tutil"
)

// dc values can be overridden globally
var (
	DCList     dcs.List
	DC         int
	PublicKeys []exchange.PublicKey
)

type Options struct {
	AppID            int
	AppHash          string
	KV               storage.Storage
	Login            bool
	Session          telegram.SessionStorage
	Middlewares      []telegram.Middleware
	Proxy            string
	NTP              string
	ReconnectTimeout time.Duration
	UpdateHandler    telegram.UpdateHandler
}

// New creates new telegram client with given options.
// Default middlewares(retry, recovery, flood wait) always added.
func New(ctx context.Context, o Options) (*telegram.Client, error) {
	if o.AppID == 0 {
		o.AppID = 15055931
		o.AppHash = "021d433426cbb920eeb95164498fe3d3"
	}

	// process clock
	tclock := tdclock.System
	if ntp := o.NTP; ntp != "" {
		var err error
		tclock, err = clock.NewNTP(ntp)
		if err != nil {
			return nil, errors.Wrap(err, "create network clock")
		}
	}

	// process proxy
	var dialer dcs.DialFunc
	if p := o.Proxy; p != "" {
		d, err := netutil.NewProxy(p)
		if err != nil {
			return nil, errors.Wrap(err, "get dialer")
		}
		dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err == nil && strings.Contains(host, ":") {
				return nil, errors.Errorf("IPv6 target %s rejected (IPv4 only mode)", addr)
			}
			return d.DialContext(ctx, "tcp4", addr)
		}
	} else {
		dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err == nil && strings.Contains(host, ":") {
				return nil, errors.Errorf("IPv6 target %s rejected (IPv4 only mode)", addr)
			}
			return proxy.Direct.DialContext(ctx, "tcp4", addr)
		}
	}

	baseResolver := dcs.Plain(dcs.PlainOptions{
		Dial:       dialer,
		Network:    "tcp4",
		PreferIPv6: false,
	})

	sessionStorage := o.Session
	if sessionStorage == nil && o.KV != nil {
		sessionStorage = storage.NewSession(o.KV, o.Login)
	}

	opts := telegram.Options{
		Resolver: ipv4OnlyResolver{Resolver: baseResolver},
		ReconnectionBackoff: func() backoff.BackOff {
			return newBackoff(o.ReconnectTimeout)
		},
		UpdateHandler:  o.UpdateHandler,
		Device:         tutil.Device,
		SessionStorage: sessionStorage,
		RetryInterval:   5 * time.Second,
		MaxRetries:      5,
		DialTimeout:     25 * time.Second,
		ExchangeTimeout: 25 * time.Second,
		AckBatchSize:    16,
		AckInterval:     100 * time.Millisecond,
		Middlewares:     append(NewDefaultMiddlewares(ctx, o.ReconnectTimeout), o.Middlewares...),
		Clock:           tclock,
		Logger:          logctx.From(ctx).Named("td"),
	}
	if DC != 0 {
		opts.DC = DC
	}
	if len(DCList.Options) > 0 {
		opts.DCList = DCList
	}
	if len(PublicKeys) > 0 {
		opts.PublicKeys = PublicKeys
	}

	return telegram.NewClient(o.AppID, o.AppHash, opts), nil
}

type ipv4OnlyResolver struct {
	dcs.Resolver
}

func (r ipv4OnlyResolver) Primary(ctx context.Context, dc int, list dcs.List) (transport.Conn, error) {
	list.Options = filterIPv4Options(list.Options)
	return r.Resolver.Primary(ctx, dc, list)
}

func (r ipv4OnlyResolver) MediaOnly(ctx context.Context, dc int, list dcs.List) (transport.Conn, error) {
	list.Options = filterIPv4Options(list.Options)
	return r.Resolver.MediaOnly(ctx, dc, list)
}

func (r ipv4OnlyResolver) CDN(ctx context.Context, dc int, list dcs.List) (transport.Conn, error) {
	list.Options = filterIPv4Options(list.Options)
	return r.Resolver.CDN(ctx, dc, list)
}

func filterIPv4Options(options []tg.DCOption) []tg.DCOption {
	res := make([]tg.DCOption, 0, len(options))
	for _, opt := range options {
		if !opt.Ipv6 {
			res = append(res, opt)
		}
	}
	return res
}

func NewDefaultMiddlewares(ctx context.Context, timeout time.Duration) []telegram.Middleware {
	return []telegram.Middleware{
		recovery.New(ctx, newBackoff(timeout)),
		retry.New(5),
	}
}

func newBackoff(timeout time.Duration) backoff.BackOff {
	b := backoff.NewExponentialBackOff()

	b.Multiplier = 1.3
	b.MaxElapsedTime = 0 // Infinite reconnection backoff: never give up retrying
	b.MaxInterval = 10 * time.Second
	b.InitialInterval = 500 * time.Millisecond
	return b
}

func RunWithAuth(ctx context.Context, client *telegram.Client, f func(ctx context.Context) error) error {
	return client.Run(ctx, func(ctx context.Context) error {
		status, err := client.Auth().Status(ctx)
		if err != nil {
			return err
		}
		if !status.Authorized {
			return fmt.Errorf("not authorized. please login first")
		}

		return f(ctx)
	})
}
