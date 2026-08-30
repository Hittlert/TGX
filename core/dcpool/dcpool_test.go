package dcpool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"

	"github.com/Hittlert/TGX/pkg/sbe/gate"
)

type mockCloseInvoker struct {
	invokedDC int
}

func (m *mockCloseInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	return nil
}

func (m *mockCloseInvoker) Close() error {
	return nil
}

func TestPool_PerDCMiddlewareAttachment(t *testing.T) {
	fg := gate.NewFloodGate(100, 10)

	// Create pool and mock invokers
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

	// Trigger FloodWait on DC 2
	fg.TriggerFloodWait(2, 500*time.Millisecond)
	assert.True(t, fg.IsDCCooledDown(2), "DC 2 must be cooled down")
	assert.False(t, fg.IsDCCooledDown(1), "DC 1 must not be cooled down")
}
