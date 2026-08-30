package dcpool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Hittlert/TGX/pkg/sbe/gate"
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

func TestPool_PerDCMiddlewareAttachment(t *testing.T) {
	fg := gate.NewFloodGate(100, 10)

	p := &pool{
		mu:        &sync.Mutex{},
		floodGate: fg,
		invokers:  make(map[int]tg.Invoker),
		closes:    make(map[int]func() error),
	}

	mockInvoker := &mockCloseInvoker{}
	p.closes[2] = mockInvoker.Close
	p.invokers[2] = chainMiddlewares(mockInvoker, fg.Middleware(2))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Invoke via middleware for DC 2
	err := p.invokers[2].Invoke(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, mockInvoker.calls)

	// Trigger FloodWait on DC 2
	fg.TriggerFloodWait(2, 500*time.Millisecond)
	assert.True(t, fg.IsDCCooledDown(2), "DC 2 must be cooled down")
	assert.False(t, fg.IsDCCooledDown(1), "DC 1 must not be cooled down")
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
