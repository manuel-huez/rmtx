//go:build windows

package client

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/terminal"
)

const resizePollInterval = 250 * time.Millisecond

type ttyInputSession struct {
	stdin  *os.File
	state  terminal.State
	done   chan struct{}
	conn   *protocol.Conn
	closed chan struct{}
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

func (s *ttyInputSession) startResizeWatcher(opts ExecOptions) {
	sizeFile := opts.StdoutFile
	if !terminal.IsTerminal(sizeFile) {
		sizeFile = opts.StdinFile
	}

	if !terminal.IsTerminal(sizeFile) {
		return
	}

	rows, cols, err := terminal.Size(sizeFile)
	if err != nil {
		rows, cols = 0, 0
	}

	go func() {
		ticker := time.NewTicker(resizePollInterval)
		defer ticker.Stop()

		lastRows, lastCols := rows, cols

		for {
			select {
			case <-s.done:
				return
			case <-ticker.C:
				rows, cols, err := terminal.Size(sizeFile)
				if err != nil || (rows == lastRows && cols == lastCols) {
					continue
				}

				lastRows, lastCols = rows, cols

				_ = s.conn.WriteJSON(
					protocol.MsgResizeTTY,
					protocol.TTYSize{Rows: rows, Cols: cols},
				)
			}
		}
	}()
}

func (s *ttyInputSession) Close() {
	select {
	case <-s.done:
	default:
		close(s.done)
	}

	_ = terminal.Restore(s.stdin, s.state)
}
