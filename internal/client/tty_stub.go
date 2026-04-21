//go:build !linux

package client

import (
	"context"
	"errors"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

type ttyInputSession struct{}

func startTTYInput(
	ctx context.Context,
	conn *protocol.Conn,
	opts ExecOptions,
) (*ttyInputSession, <-chan error, error) {
	_ = ctx
	_ = conn
	_ = opts

	return nil, nil, errors.New("interactive TTY is not supported on this platform")
}

func (s *ttyInputSession) Close() {}
