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
	cancelRun func()
}

func (s *Server) runTTYExecCommand(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	cmd *exec.Cmd,
	request protocol.RunRequest,
) (int, error) {
	cancelRun := s.commandCancel(cmd, cancel)
	defer cancelRun()

	return s.runPlatformTTYExecCommand(ctx, ttyExecRequest{
		conn:      conn,
		cmd:       cmd,
		request:   request,
		cancelRun: cancelRun,
	})
}

func watchRunContext(ctx context.Context, cancelRun func()) {
	go func() {
		<-ctx.Done()
		cancelRun()
	}()
}
