//go:build !windows

package host

import (
	"context"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func hostIsWindows() bool {
	return false
}

func (s *Server) platformRuntimeStateDir(
	context.Context,
	protocol.RuntimeSpec,
	*hostLogSubscription,
) (string, error) {
	return s.opts.StateDir, nil
}
