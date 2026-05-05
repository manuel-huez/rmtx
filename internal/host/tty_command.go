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
	runLogs *hostLogSubscription,
) (int, error) {
	cmd, cleanup, err := s.newTTYSessionCommand(
		ctx,
		workspace,
		workdir,
		request,
		preparedRuntime,
		runLogs,
	)
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
	runLogs *hostLogSubscription,
) (*exec.Cmd, commandCleanup, error) {
	if isOCIRuntime(request.Runtime) {
		if err := s.prepareOCIContextSetup(
			ctx,
			workspace,
			request,
			preparedRuntime,
			runLogs,
		); err != nil {
			return nil, noopCommandCleanup, err
		}

		runLogs.Flush()

		return s.newOCICommand(ctx, workspace, workdir, request, preparedRuntime, runLogs)
	}

	return s.newSessionCommand(ctx, workspace, workdir, request), noopCommandCleanup, nil
}
