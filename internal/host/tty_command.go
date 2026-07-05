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
	runtimeDir string,
	workspace string,
	workdir string,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
	runLogs *hostLogSubscription,
) (int, error) {
	cancelRun := newRunCancelHandle(cancel)
	input := s.startTTYInputForwarding(conn, cancelRun.Cancel)

	cmd, cleanup, err := s.newTTYSessionCommand(
		ctx,
		runtimeDir,
		workspace,
		workdir,
		request,
		preparedRuntime,
		runLogs,
	)
	if err != nil {
		cancelRun.Cancel()
		_ = stopTTYInputReader(conn, input)
		return 1, err
	}

	code, runErr := s.runTTYExecCommand(ctx, cancel, conn, cmd, request, input, cancelRun)

	return finishCommandWithCleanup(code, runErr, cleanup)
}

func (s *Server) newTTYSessionCommand(
	ctx context.Context,
	runtimeDir string,
	workspace string,
	workdir string,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
	runLogs *hostLogSubscription,
) (*exec.Cmd, commandCleanup, error) {
	if isOCIRuntime(request.Runtime) {
		if err := s.prepareOCIContextSetup(
			ctx,
			runtimeDir,
			workspace,
			request,
			preparedRuntime,
			runLogs,
		); err != nil {
			return nil, noopCommandCleanup, err
		}

		runLogs.Flush()

		return s.newOCICommand(ctx, runtimeDir, workspace, workdir, request, preparedRuntime, runLogs)
	}

	return s.newSessionCommand(ctx, workspace, workdir, request), noopCommandCleanup, nil
}
