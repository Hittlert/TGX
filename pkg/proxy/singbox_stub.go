//go:build !with_singbox

package proxy

import (
	"context"
	"errors"
)

var (
	ErrEmbeddedSingBoxDisabled = errors.New("embedded sing-box is not enabled in this binary build (build with -tags with_singbox)")
)

// EmbeddedSingBoxProvider is a stub when compiled without with_singbox tag.
type EmbeddedSingBoxProvider struct{}

// NewEmbeddedSingBoxProvider returns an error when with_singbox tag is absent.
func NewEmbeddedSingBoxProvider(configJSON []byte) (*EmbeddedSingBoxProvider, error) {
	return nil, ErrEmbeddedSingBoxDisabled
}

func (e *EmbeddedSingBoxProvider) GetDialer(ctx context.Context, dcID int) (ContextDialer, error) {
	return nil, ErrEmbeddedSingBoxDisabled
}

func (e *EmbeddedSingBoxProvider) ReportFailure(dcID int, err error) {}

func (e *EmbeddedSingBoxProvider) Close() error {
	return nil
}
