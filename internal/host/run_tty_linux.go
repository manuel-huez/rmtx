//go:build linux

package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"syscall"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func (s *Server) runTTYExecCommand(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	cmd *exec.Cmd,
	request protocol.RunRequest,
) (int, error) {
	master, slave, err := openPTY(request.TTYRows, request.TTYCols)
	if err != nil {
		return 1, err
	}

	defer func() { _ = master.Close() }()

	cmd.Stdin = slave
	cmd.Stdout = slave
	cmd.Stderr = slave

	sysProcAttr := cmd.SysProcAttr
	if sysProcAttr == nil {
		sysProcAttr = &syscall.SysProcAttr{}
	}

	sysProcAttr.Setsid = true
	sysProcAttr.Setctty = true
	sysProcAttr.Ctty = int(slave.Fd())
	cmd.SysProcAttr = sysProcAttr

	if err := cmd.Start(); err != nil {
		_ = slave.Close()
		return exitCode(err), fmt.Errorf("start TTY command: %w", err)
	}

	_ = slave.Close()

	outputDone := make(chan error, 1)

	go func() { outputDone <- streamPipe(conn, master, "stdout") }()

	go func() {
		if err := s.consumeTTYInput(conn, master); err != nil {
			cancel()

			if !errors.Is(err, io.EOF) {
				s.logger.Printf("TTY input forwarding ended: %v", err)
			}
		}
	}()

	waitErr := cmd.Wait()
	_ = master.Close()

	if err := <-outputDone; err != nil && !errors.Is(err, io.EOF) {
		cancel()
		return exitCode(waitErr), err
	}

	return exitCode(waitErr), waitErr
}
