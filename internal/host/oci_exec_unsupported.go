//go:build !linux && !windows

package host

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
)

func (s *Server) ociChildCommand(
	ctx context.Context,
	spec ociChildSpec,
	contextDir string,
) (*exec.Cmd, commandCleanup, error) {
	_ = ctx
	_ = spec
	_ = contextDir

	return nil, noopCommandCleanup, fmt.Errorf("OCI runtime is not supported on %s hosts yet", runtime.GOOS)
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
	Binds []ociBind
	Env   []string
}

func nvidiaUnavailableError(err error) error {
	return err
}
