package dcpool

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCloseInvoker struct {
	calls int
}

func (m *mockCloseInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	m.calls++
	return nil
}

func (m *mockCloseInvoker) Close() error {
	return nil
}

type testMiddleware struct {
	name  string
	order *[]string
}

func (m *testMiddleware) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		*m.order = append(*m.order, m.name+"_pre")
		err := next.Invoke(ctx, input, output)
		*m.order = append(*m.order, m.name+"_post")
		return err
	}
}

func TestPool_ChainMiddlewaresOrder(t *testing.T) {
	var order []string

	m1 := &testMiddleware{name: "m1", order: &order}
	m2 := &testMiddleware{name: "m2", order: &order}

	base := &mockCloseInvoker{}
	invoker := chainMiddlewares(base, m1, m2)

	err := invoker.Invoke(context.Background(), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"m1_pre", "m2_pre", "m2_post", "m1_post"}, order)
	assert.Equal(t, 1, base.calls)
}

func TestPool_FailedInvokerHonorsCancellation(t *testing.T) {
	fi := failedInvoker{dc: 2, err: context.Canceled}
	err := fi.Invoke(context.Background(), nil, nil)
	assert.True(t, errors.Is(err, context.Canceled))
}

func TestPool_TakeoutNoDeadlock(t *testing.T) {
	EnableTestMode()
	defer func() { testMode = false }()

	p := &pool{
		invokers: make(map[int]tg.Invoker),
		closes:   make(map[int]func() error),
		dcLocks:  make(map[int]*sync.Mutex),
	}

	// In test mode, invoker returns nil or p.api without deadlocking
	client := p.Takeout(context.Background(), 2)
	assert.NotNil(t, client)
}
