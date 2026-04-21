//go:build linux

package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func (s *Server) runTTYCommand(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	workspace string,
	workdir string,
	request protocol.RunRequest,
) (int, error) {
	master, slave, err := openPTY(request.TTYRows, request.TTYCols)
	if err != nil {
		return 1, err
	}

	defer func() { _ = master.Close() }()

	cmd := s.newSessionCommand(ctx, workspace, workdir, request)

	cmd.Stdin = slave
	cmd.Stdout = slave
	cmd.Stderr = slave
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid:  true,
		Setctty: true,
		Ctty:    int(slave.Fd()),
	}

	if err := cmd.Start(); err != nil {
		_ = slave.Close()
		return exitCode(err), fmt.Errorf("start TTY command: %w", err)
	}

	_ = slave.Close()

	outputDone := make(chan error, 1)

	go func() { outputDone <- streamPipe(conn, master, "stdout") }()

	go func() {
		if err := s.consumeTTYInput(conn, master); err != nil && !errors.Is(err, io.EOF) {
			s.logger.Printf("TTY input forwarding ended: %v", err)
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
