package dcpool

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Hittlert/TGX/pkg/sbe/gate"
)

func TestPool_PerDCMiddlewareAttachment(t *testing.T) {
	EnableTestMode()
	fg := gate.NewFloodGate(100, 10)

	p := NewPoolWithGate(&telegram.Client{}, 8, fg)
	require.NotNil(t, p)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cl := p.Client(ctx, 2)
	assert.NotNil(t, cl)

	// Trigger FloodWait on DC 2
	fg.TriggerFloodWait(2, 500*time.Millisecond)
	assert.True(t, fg.IsDCCooledDown(2))
	assert.False(t, fg.IsDCCooledDown(1), "DC 1 must not be cooled down")

	_ = p.Close()
}
