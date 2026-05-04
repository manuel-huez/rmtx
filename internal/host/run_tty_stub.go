//go:build !linux && !windows

package host

import (
	"context"
	"errors"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func (s *Server) runTTYCommand(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	workspace string,
	workdir string,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
) (int, error) {
	_ = ctx
	_ = cancel
	_ = conn
	_ = workspace
	_ = workdir
	_ = request
	_ = preparedRuntime

	return 1, errors.New("interactive TTY is not supported on this platform")
}
