//go:build windows

package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"syscall"

	"github.com/UserExistsError/conpty"
)

type windowsTTY struct {
	pty *conpty.ConPty
}

func (w *windowsTTY) Write(p []byte) (int, error) {
	return w.pty.Write(p)
}

func (w *windowsTTY) ResizeTTY(rows, cols int) error {
	return w.pty.Resize(cols, rows)
}

func (s *Server) runPlatformTTYExecCommand(
	ctx context.Context,
	run ttyExecRequest,
) (int, error) {
	options := []conpty.ConPtyOption{
		conpty.ConPtyWorkDir(run.cmd.Dir),
		conpty.ConPtyEnv(run.cmd.Env),
	}
	if run.request.TTYCols > 0 && run.request.TTYRows > 0 {
		options = append(options, conpty.ConPtyDimensions(run.request.TTYCols, run.request.TTYRows))
	}

	pty, err := conpty.Start(windowsCommandLine(run.cmd.Args), options...)
	if err != nil {
		return 1, fmt.Errorf("start TTY command: %w", err)
	}

	var closeOnce sync.Once

	closePTY := func() {
		closeOnce.Do(func() {
			_ = pty.Close()
		})
	}
	defer closePTY()

	outputDone := make(chan error, 1)

	go func() { outputDone <- streamPipe(run.conn, pty, "stdout") }()

	if run.input != nil {
		if err := run.input.Attach(&windowsTTY{pty: pty}); err != nil {
			run.cancelRun()
			return 1, err
		}
	}

	watchRunContext(ctx, run.cancelRun)

	exitCode, waitErr := pty.Wait(ctx)

	closePTY()

	if err := <-outputDone; err != nil && !errors.Is(err, io.EOF) {
		if !isDisconnectError(err) {
			s.logger.Printf("TTY output forwarding ended: %v", err)
		}

		run.cancelRun()

		return int(exitCode), err
	}

	return int(exitCode), waitErr
}

func windowsCommandLine(args []string) string {
	escaped := make([]string, 0, len(args))
	for _, arg := range args {
		escaped = append(escaped, syscall.EscapeArg(arg))
	}

	return strings.Join(escaped, " ")
}
