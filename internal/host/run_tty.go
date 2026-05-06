//go:build linux || windows

package host

import (
	"context"
	"os/exec"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

type ttyExecRequest struct {
	conn      *protocol.Conn
	cmd       *exec.Cmd
	request   protocol.RunRequest
	input     *ttyInputForwarding
	cancelRun func()
}

func (s *Server) runTTYExecCommand(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	cmd *exec.Cmd,
	request protocol.RunRequest,
	input *ttyInputForwarding,
	cancelRunHandle *runCancelHandle,
) (int, error) {
	cancelRun := s.commandCancel(cmd, cancel)
	if cancelRunHandle != nil {
		cancelRunHandle.Set(cancelRun)
	}
	defer cancelRun()

	return s.runPlatformTTYExecCommand(ctx, ttyExecRequest{
		conn:      conn,
		cmd:       cmd,
		request:   request,
		input:     input,
		cancelRun: cancelRun,
	})
}

func watchRunContext(ctx context.Context, cancelRun func()) {
	go func() {
		<-ctx.Done()
		cancelRun()
	}()
}
