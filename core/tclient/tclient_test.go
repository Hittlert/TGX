package tclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDefaultMiddlewares(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	mws := NewDefaultMiddlewares(ctx, 5*time.Second)
	require.Len(t, mws, 2)
	assert.NotNil(t, mws[0], "recovery middleware")
	assert.NotNil(t, mws[1], "retry middleware")
}
