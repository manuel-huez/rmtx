//go:build !windows

package host

import (
	"context"

	"github.com/manuel-huez/rmtx/internal/config"
)

func hostIsWindows() bool {
	return false
}

func (s *Server) platformRuntimeStateDir(
	context.Context,
	config.RuntimeConfig,
	*hostLogSubscription,
) (string, error) {
	return s.opts.StateDir, nil
}
