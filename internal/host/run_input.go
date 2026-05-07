package host

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

type runCancelHandle struct {
	mu     sync.Mutex
	cancel func()
}

type ttyInputForwarding struct {
	mu      sync.Mutex
	writer  io.Writer
	queue   [][]byte
	resize  *protocol.TTYSize
	stopped bool
	done    chan error
}

func (s *Server) startTTYInputForwarding(
	conn *protocol.Conn,
	cancelRun func(),
) *ttyInputForwarding {
	input := &ttyInputForwarding{done: make(chan error, 1)}

	go func() {
		input.done <- s.consumeQueuedTTYInput(conn, input, cancelRun)
	}()

	return input
}

func (i *ttyInputForwarding) Attach(writer io.Writer) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.writer = writer

	for _, data := range i.queue {
		if _, err := writer.Write(data); err != nil {
			return err
		}
	}
	i.queue = nil

	if i.resize != nil {
		if err := resizeTTYWriter(writer, i.resize.Rows, i.resize.Cols); err != nil {
			return err
		}
		i.resize = nil
	}

	return nil
}

func (i *ttyInputForwarding) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	if i.writer == nil {
		i.queue = append(i.queue, append([]byte(nil), p...))
		return len(p), nil
	}

	return i.writer.Write(p)
}

func (i *ttyInputForwarding) ResizeTTY(rows, cols int) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.writer == nil {
		i.resize = &protocol.TTYSize{Rows: rows, Cols: cols}
		return nil
	}

	return resizeTTYWriter(i.writer, rows, cols)
}

func (i *ttyInputForwarding) Stop() {
	i.mu.Lock()
	i.stopped = true
	i.mu.Unlock()
}

func (i *ttyInputForwarding) Stopped() bool {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.stopped
}

func (i *ttyInputForwarding) Done() <-chan error {
	return i.done
}

func newRunCancelHandle(cancel func()) *runCancelHandle {
	return &runCancelHandle{cancel: cancel}
}

func (h *runCancelHandle) Set(cancel func()) {
	h.mu.Lock()
	h.cancel = cancel
	h.mu.Unlock()
}

func (h *runCancelHandle) Cancel() {
	h.mu.Lock()
	cancel := h.cancel
	h.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

type pipeInputForwarding struct {
	done        chan error
	once        sync.Once
	cond        *sync.Cond
	queue       [][]byte
	queuedBytes int
	closed      bool
	stopped     bool
}

const maxQueuedPipeInputBytes = 1 << 20

func (s *Server) startPipeInputForwarding(
	conn *protocol.Conn,
	cancelRun func(),
) *pipeInputForwarding {
	input := &pipeInputForwarding{
		done: make(chan error, 1),
	}
	input.cond = sync.NewCond(&sync.Mutex{})

	go func() {
		err := s.consumePipeInput(conn, input, cancelRun)
		_ = input.Close()
		if err != nil && !input.Stopped() && cancelRun != nil {
			cancelRun()
		}
		input.done <- err
	}()

	return input
}

func (i *pipeInputForwarding) Reader() io.Reader {
	return i
}

func (i *pipeInputForwarding) Done() <-chan error {
	return i.done
}

func (i *pipeInputForwarding) Stop() {
	i.once.Do(func() {
		i.cond.L.Lock()
		i.stopped = true
		i.queue = nil
		i.queuedBytes = 0
		i.cond.Broadcast()
		i.cond.L.Unlock()
	})
}

func stopPipeInputReader(conn *protocol.Conn, input *pipeInputForwarding) error {
	input.Stop()
	return stopInputReader(conn, input.Done())
}

func stopTTYInputReader(conn *protocol.Conn, input *ttyInputForwarding) error {
	if input == nil {
		return nil
	}

	input.Stop()
	return stopInputReader(conn, input.Done())
}

func stopInputReader(conn *protocol.Conn, done <-chan error) error {
	if conn != nil && conn.Raw() != nil {
		_ = conn.Raw().SetReadDeadline(time.Now())
		defer func() { _ = conn.Raw().SetReadDeadline(time.Time{}) }()
	}

	select {
	case err := <-done:
		if protocol.IsDisconnectError(err) || errors.Is(err, os.ErrDeadlineExceeded) {
			return nil
		}
		return err
	case <-time.After(time.Second):
		return nil
	}
}

func (i *pipeInputForwarding) Stopped() bool {
	i.cond.L.Lock()
	defer i.cond.L.Unlock()

	return i.stopped
}

func (i *pipeInputForwarding) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	i.cond.L.Lock()
	defer i.cond.L.Unlock()

	for !i.closed && !i.stopped && i.queuedBytes+len(p) > maxQueuedPipeInputBytes {
		i.cond.Wait()
	}

	if i.closed || i.stopped {
		return 0, io.ErrClosedPipe
	}

	buf := append([]byte(nil), p...)
	i.queue = append(i.queue, buf)
	i.queuedBytes += len(buf)
	i.cond.Signal()

	return len(p), nil
}

func (i *pipeInputForwarding) Close() error {
	i.cond.L.Lock()
	i.closed = true
	i.cond.Broadcast()
	i.cond.L.Unlock()

	return nil
}

func (i *pipeInputForwarding) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	i.cond.L.Lock()
	defer i.cond.L.Unlock()

	for len(i.queue) == 0 && !i.closed && !i.stopped {
		i.cond.Wait()
	}

	if len(i.queue) == 0 {
		return 0, io.EOF
	}

	n := copy(p, i.queue[0])
	if n == len(i.queue[0]) {
		i.queuedBytes -= len(i.queue[0])
		copy(i.queue, i.queue[1:])
		i.queue = i.queue[:len(i.queue)-1]
	} else {
		i.queue[0] = i.queue[0][n:]
		i.queuedBytes -= n
	}
	i.cond.Signal()

	return n, nil
}

func (s *Server) consumePipeInput(
	conn *protocol.Conn,
	stdin io.WriteCloser,
	cancelRun func(),
) error {
	var stdinClosed bool

	for {
		if stopped, ok := stdin.(interface{ Stopped() bool }); ok && stopped.Stopped() {
			return nil
		}

		head, err := conn.ReadHeader()
		if err != nil {
			return err
		}

		if stdinClosed {
			if head.Type == protocol.MsgRunCancel && cancelRun != nil {
				cancelRun()
			}

			if err := conn.DiscardPayload(head); err != nil {
				return err
			}
			continue
		}

		done, err := s.handleInputFrame(conn, head, stdin, false, cancelRun)
		if err != nil {
			return err
		}

		if done {
			stdinClosed = true
			_ = stdin.Close()
		}
	}
}

func (s *Server) handleInputFrame(
	conn *protocol.Conn,
	head protocol.Header,
	writer io.Writer,
	allowResize bool,
	cancelRun func(),
) (bool, error) {
	switch head.Type {
	case protocol.MsgHeartbeat:
		return false, conn.DiscardPayload(head)
	case protocol.MsgStdinData:
		_, err := io.Copy(writer, conn.PayloadReader(head))
		return false, err
	case protocol.MsgStdinClose:
		return true, nil
	case protocol.MsgRunCancel:
		if err := conn.DiscardPayload(head); err != nil {
			return false, err
		}

		if cancelRun != nil {
			cancelRun()
		}

		return true, nil
	case protocol.MsgResizeTTY:
		if !allowResize {
			if err := conn.DiscardPayload(head); err != nil {
				return false, err
			}

			return false, fmt.Errorf("unexpected frame during stdin phase: %s", head.Type)
		}

		return false, handleTTYResize(head, writer)
	default:
		if err := conn.DiscardPayload(head); err != nil {
			return false, err
		}

		phase := "stdin"
		if allowResize {
			phase = "TTY"
		}

		return false, fmt.Errorf("unexpected frame during %s phase: %s", phase, head.Type)
	}
}
