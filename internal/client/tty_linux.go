//go:build linux

package client

import (
	"context"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/terminal"
)

type ttyInputSession struct {
	stdin   *os.File
	state   terminal.State
	signals chan os.Signal
	done    chan struct{}
	conn    *protocol.Conn
	closed  chan struct{}
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
		closed: make(chan struct{}),
		conn:   conn,
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

	s.signals = make(chan os.Signal, 1)
	signal.Notify(s.signals, syscall.SIGWINCH)

	go func() {
		for {
			select {
			case <-s.done:
				return
			case <-s.signals:
				rows, cols, err := terminal.Size(sizeFile)
				if err != nil {
					continue
				}

				_ = s.conn.WriteJSON(protocol.MsgResizeTTY, protocol.TTYSize{Rows: rows, Cols: cols})
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

	if s.signals != nil {
		signal.Stop(s.signals)
	}

	_ = terminal.Restore(s.stdin, s.state)
}
