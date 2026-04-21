//go:build linux || windows

package client

import (
	"context"
	"io"
	"os"

	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/terminal"
)

type ttyInputSession struct {
	stdin   *os.File
	state   terminal.State
	done    chan struct{}
	conn    *protocol.Conn
	closed  chan struct{}
	closeFn func()
}

func startTTYInput(
	ctx context.Context,
	conn *protocol.Conn,
	opts ExecOptions,
) (*ttyInputSession, <-chan error, error) {
	if opts.StdinFile == nil {
		return nil, nil, io.ErrUnexpectedEOF
	}

	state, err := terminal.MakeRaw(opts.StdinFile)
	if err != nil {
		return nil, nil, err
	}

	session := &ttyInputSession{
		stdin:  opts.StdinFile,
		state:  state,
		done:   make(chan struct{}),
		conn:   conn,
		closed: make(chan struct{}),
	}

	session.startResizeWatcher(opts)

	errCh := make(chan error, 1)

	go func() {
		defer close(session.closed)

		errCh <- sendStdin(conn, opts.Stdin, true)
	}()

	go func() {
		select {
		case <-ctx.Done():
			session.Close()
		case <-session.closed:
		}
	}()

	return session, errCh, nil
}

func selectTTYSizeFile(opts ExecOptions) *os.File {
	sizeFile := opts.StdoutFile
	if !terminal.IsTerminal(sizeFile) {
		sizeFile = opts.StdinFile
	}

	if !terminal.IsTerminal(sizeFile) {
		return nil
	}

	return sizeFile
}

func (s *ttyInputSession) writeTTYSize(rows, cols int) {
	_ = s.conn.WriteJSON(
		protocol.MsgResizeTTY,
		protocol.TTYSize{Rows: rows, Cols: cols},
	)
}

func (s *ttyInputSession) Close() {
	select {
	case <-s.done:
	default:
		close(s.done)
	}

	if s.closeFn != nil {
		s.closeFn()
	}

	_ = terminal.Restore(s.stdin, s.state)
}
