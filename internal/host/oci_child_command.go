package host

import (
	"context"
	"errors"
	"os/exec"
)

type ociChildCommandRequest struct {
	spec       ociChildSpec
	runtimeDir string
	runLogs    *hostLogSubscription
}

func (s *Server) ociChildCommand(
	ctx context.Context,
	spec ociChildSpec,
	runtimeDir string,
	runLogs *hostLogSubscription,
) (*exec.Cmd, commandCleanup, error) {
	if len(spec.Command) == 0 {
		return nil, noopCommandCleanup, errors.New("OCI command is required")
	}

	return s.platformOCIChildCommand(ctx, ociChildCommandRequest{
		spec:       spec,
		runtimeDir: runtimeDir,
		runLogs:    runLogs,
	})
}
