//go:build linux

package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"
)

func (s *Server) runPlatformTTYExecCommand(
	ctx context.Context,
	run ttyExecRequest,
) (int, error) {
	master, slave, err := openPTY(run.request.TTYRows, run.request.TTYCols)
	if err != nil {
		return 1, err
	}

	defer func() { _ = master.Close() }()

	run.cmd.Stdin = slave
	run.cmd.Stdout = slave
	run.cmd.Stderr = slave

	sysProcAttr := run.cmd.SysProcAttr
	if sysProcAttr == nil {
		sysProcAttr = &syscall.SysProcAttr{}
	}

	sysProcAttr.Setsid = true
	sysProcAttr.Setctty = true
	sysProcAttr.Ctty = int(slave.Fd())
	run.cmd.SysProcAttr = sysProcAttr

	if err := run.cmd.Start(); err != nil {
		_ = slave.Close()
		return exitCode(err), fmt.Errorf("start TTY command: %w", err)
	}

	_ = slave.Close()

	if run.input != nil {
		if err := run.input.Attach(master); err != nil {
			run.cancelRun()
			return exitCode(run.cmd.Wait()), err
		}
	}

	watchRunContext(ctx, run.cancelRun)

	outputDone := make(chan error, 1)

	go func() { outputDone <- streamPipe(run.conn, master, "stdout") }()

	waitErr := run.cmd.Wait()
	_ = master.Close()

	if err := <-outputDone; err != nil && !errors.Is(err, io.EOF) {
		if !isDisconnectError(err) {
			s.logger.Printf("TTY output forwarding ended: %v", err)
		}

		run.cancelRun()

		return exitCode(waitErr), err
	}

	if err := stopTTYInputReader(run.conn, run.input); err != nil && !errors.Is(err, io.EOF) {
		s.logger.Printf("TTY input forwarding ended: %v", err)
	}

	return exitCode(waitErr), waitErr
}
