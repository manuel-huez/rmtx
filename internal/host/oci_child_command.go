package host

import (
	"context"
	"errors"
	"os/exec"
)

type ociChildCommandRequest struct {
	spec       ociChildSpec
	contextDir string
	runLogs    *hostLogSubscription
}

func (s *Server) ociChildCommand(
	ctx context.Context,
	spec ociChildSpec,
	contextDir string,
	runLogs *hostLogSubscription,
) (*exec.Cmd, commandCleanup, error) {
	if len(spec.Command) == 0 {
		return nil, noopCommandCleanup, errors.New("OCI command is required")
	}

	return s.platformOCIChildCommand(ctx, ociChildCommandRequest{
		spec:       spec,
		contextDir: contextDir,
		runLogs:    runLogs,
	})
}
