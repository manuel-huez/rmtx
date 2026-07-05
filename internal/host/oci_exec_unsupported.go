//go:build !linux && !windows

package host

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strings"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func (s *Server) platformOCIChildCommand(
	context.Context,
	ociChildCommandRequest,
) (*exec.Cmd, commandCleanup, error) {
	return nil, noopCommandCleanup, fmt.Errorf(
		"OCI runtime is not supported on %s hosts yet",
		runtime.GOOS,
	)
}

func nvidiaRuntime(mode string) (nvidiaRuntimeSpec, error) {
	if strings.EqualFold(strings.TrimSpace(mode), "nvidia") {
		return nvidiaRuntimeSpec{}, fmt.Errorf(
			"NVIDIA CUDA runtime is not supported on %s hosts yet",
			runtime.GOOS,
		)
	}

	return nvidiaRuntimeSpec{}, nil
}

type nvidiaRuntimeSpec struct {
	Binds        []ociBind
	Env          []string
	PathPrefixes []string
}

func nvidiaUnavailableError(err error) error {
	return err
}

func pruneWSLStagedRootFS(context.Context, []string) ([]protocol.ContextArtifact, int64, error) {
	return nil, 0, nil
}
