//go:build linux || windows

package host

import (
	"context"
	"os/exec"

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
	cmd, cleanup, err := s.newTTYSessionCommand(ctx, workspace, workdir, request, preparedRuntime)
	if err != nil {
		return 1, err
	}

	code, runErr := s.runTTYExecCommand(ctx, cancel, conn, cmd, request)

	return finishCommandWithCleanup(code, runErr, cleanup)
}

func (s *Server) newTTYSessionCommand(
	ctx context.Context,
	workspace string,
	workdir string,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
) (*exec.Cmd, commandCleanup, error) {
	if isOCIRuntime(request.Runtime) {
		if err := s.prepareOCIContextSetup(ctx, workspace, request, preparedRuntime); err != nil {
			return nil, noopCommandCleanup, err
		}

		return s.newOCICommand(ctx, workspace, workdir, request, preparedRuntime)
	}

	return s.newSessionCommand(ctx, workspace, workdir, request), noopCommandCleanup, nil
}
