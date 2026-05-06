package host

import (
	"io"
	"sync"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

type hostLogHub struct {
	out io.Writer
}

func newHostLogHub(out io.Writer) *hostLogHub {
	if out == nil {
		out = io.Discard
	}

	return &hostLogHub{out: out}
}

func (h *hostLogHub) Write(p []byte) (int, error) {
	return h.out.Write(p)
}

func (h *hostLogHub) Subscribe(conn *protocol.Conn) *hostLogSubscription {
	if h == nil || conn == nil {
		return nil
	}

	return newHostLogSubscription(conn)
}

type hostLogSubscription struct {
	conn *protocol.Conn

	mu        sync.Mutex
	cond      *sync.Cond
	queue     [][]byte
	writing   bool
	closed    bool
	err       error
	done      chan struct{}
	closeOnce sync.Once
}

func newHostLogSubscription(conn *protocol.Conn) *hostLogSubscription {
	sub := &hostLogSubscription{
		conn: conn,
		done: make(chan struct{}),
	}
	sub.cond = sync.NewCond(&sub.mu)

	go sub.run()

	return sub
}

func (s *hostLogSubscription) Write(p []byte) (int, error) {
	if s == nil {
		return len(p), nil
	}

	s.Enqueue(p)

	return len(p), nil
}

func (s *hostLogSubscription) Enqueue(p []byte) {
	if s == nil || len(p) == 0 {
		return
	}

	buf := append([]byte(nil), p...)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	s.queue = append(s.queue, buf)
	s.cond.Signal()
}

func (s *hostLogSubscription) Flush() {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for s.err == nil && !s.closed && (len(s.queue) > 0 || s.writing) {
		s.cond.Wait()
	}
}

func (s *hostLogSubscription) Close() {
	if s == nil {
		return
	}

	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.cond.Broadcast()
		s.mu.Unlock()

		<-s.done
	})
}

func (s *hostLogSubscription) run() {
	defer close(s.done)

	for {
		item, ok := s.next()
		if !ok {
			return
		}

		err := s.conn.WriteBytes(
			protocol.MsgExecOutput,
			protocol.OutputInfo{Stream: "stderr"},
			item,
		)

		s.mu.Lock()

		s.writing = false
		if err != nil {
			s.err = err
			s.closed = true
			s.queue = nil
		}

		s.cond.Broadcast()
		s.mu.Unlock()
	}
}

func (s *hostLogSubscription) next() ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.queue) == 0 && !s.closed {
		s.cond.Wait()
	}

	if len(s.queue) == 0 {
		return nil, false
	}

	item := s.queue[0]
	copy(s.queue, s.queue[1:])
	s.queue = s.queue[:len(s.queue)-1]
	s.writing = true

	return item, true
}
