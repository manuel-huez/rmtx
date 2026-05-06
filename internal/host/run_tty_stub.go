//go:build !linux && !windows

package host

import (
	"context"
	"errors"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func (s *Server) runTTYCommand(
	_ context.Context,
	_ context.CancelFunc,
	_ *protocol.Conn,
	_ string,
	_ string,
	_ protocol.RunRequest,
	_ *preparedRuntime,
	_ *hostLogSubscription,
) (int, error) {
	return 1, errors.New("interactive TTY is not supported on this platform")
}

func (s *Server) consumeQueuedTTYInput(
	_ *protocol.Conn,
	_ *ttyInputForwarding,
	_ func(),
) error {
	return errors.New("interactive TTY is not supported on this platform")
}
