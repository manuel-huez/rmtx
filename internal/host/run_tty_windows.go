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

	"github.com/manuel-huez/rmtx/internal/protocol"
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

func (s *Server) runTTYCommand(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	workspace string,
	workdir string,
	request protocol.RunRequest,
) (int, error) {
	cmd := s.newSessionCommand(ctx, workspace, workdir, request)

	options := []conpty.ConPtyOption{
		conpty.ConPtyWorkDir(cmd.Dir),
		conpty.ConPtyEnv(cmd.Env),
	}
	if request.TTYCols > 0 && request.TTYRows > 0 {
		options = append(options, conpty.ConPtyDimensions(request.TTYCols, request.TTYRows))
	}

	pty, err := conpty.Start(windowsCommandLine(cmd.Args), options...)
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

	go func() { outputDone <- streamPipe(conn, pty, "stdout") }()

	go func() {
		if err := s.consumeTTYInput(
			conn,
			&windowsTTY{pty: pty},
		); err != nil {
			cancel()
			if !errors.Is(err, io.EOF) {
				s.logger.Printf("TTY input forwarding ended: %v", err)
			}
		}
	}()

	go func() {
		<-ctx.Done()
		closePTY()
	}()

	exitCode, waitErr := pty.Wait(ctx)

	closePTY()

	if err := <-outputDone; err != nil && !errors.Is(err, io.EOF) {
		cancel()
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
