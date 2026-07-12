package host

import (
	"io"
	"sync"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

const (
	maxQueuedLogBytes = 1 << 20
	maxQueuedLogItems = 1024
)

var droppedLogMarker = []byte("rmtx: slow client; older streamed logs dropped\n")

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

	mu          sync.Mutex
	cond        *sync.Cond
	queue       [][]byte
	head        int
	queuedBytes int
	dropped     bool
	writing     bool
	closed      bool
	err         error
	started     time.Time
	lastSection time.Time
	done        chan struct{}
	closeOnce   sync.Once
}

func newHostLogSubscription(conn *protocol.Conn) *hostLogSubscription {
	now := time.Now()
	sub := &hostLogSubscription{
		conn:        conn,
		started:     now,
		lastSection: now,
		done:        make(chan struct{}),
	}
	sub.cond = sync.NewCond(&sub.mu)

	go sub.run()

	return sub
}

func (s *hostLogSubscription) markRunLogSection() (time.Duration, time.Duration, bool) {
	if s == nil {
		return 0, 0, false
	}

	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started.IsZero() {
		s.started = now
		s.lastSection = now
	}

	elapsed := now.Sub(s.lastSection)
	total := now.Sub(s.started)
	s.lastSection = now

	return elapsed, total, true
}

func (s *hostLogSubscription) Write(p []byte) (int, error) {
	if s == nil {
		return len(p), nil
	}

	s.Enqueue(p)

	return len(p), nil
}

//nolint:cyclop // Byte and item limits must be enforced before both marker and payload inserts.
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

	if len(buf) > maxQueuedLogBytes {
		buf = buf[len(buf)-maxQueuedLogBytes:]
	}

	for (s.queuedBytes+len(buf) > maxQueuedLogBytes ||
		len(s.queue)-s.head >= maxQueuedLogItems) && s.head < len(s.queue) {
		s.queuedBytes -= len(s.queue[s.head])
		s.queue[s.head] = nil
		s.head++
		s.dropped = true
	}

	if s.dropped {
		if len(buf) > maxQueuedLogBytes-len(droppedLogMarker) {
			buf = buf[len(buf)-(maxQueuedLogBytes-len(droppedLogMarker)):]
		}

		for (s.queuedBytes+len(droppedLogMarker)+len(buf) > maxQueuedLogBytes ||
			len(s.queue)-s.head >= maxQueuedLogItems-1) && s.head < len(s.queue) {
			s.queuedBytes -= len(s.queue[s.head])
			s.queue[s.head] = nil
			s.head++
		}

		marker := append([]byte(nil), droppedLogMarker...)
		s.queue = append(s.queue, marker)
		s.queuedBytes += len(marker)
		s.dropped = false
	}

	if s.head > 0 && s.head*2 >= len(s.queue) {
		s.queue = append(s.queue[:0], s.queue[s.head:]...)
		s.head = 0
	}

	s.queue = append(s.queue, buf)
	s.queuedBytes += len(buf)
	s.cond.Signal()
}

func (s *hostLogSubscription) Flush() {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for s.err == nil && !s.closed && (s.head < len(s.queue) || s.writing) {
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
			protocol.OutputInfo{Stream: outputStreamStderr},
			item,
		)

		s.mu.Lock()

		s.writing = false
		if err != nil {
			s.err = err
			s.closed = true
			s.queue = nil
			s.head = 0
			s.queuedBytes = 0
		}

		s.cond.Broadcast()
		s.mu.Unlock()
	}
}

func (s *hostLogSubscription) next() ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.head == len(s.queue) && !s.closed {
		s.cond.Wait()
	}

	if s.head == len(s.queue) {
		return nil, false
	}

	item := s.queue[s.head]
	s.queue[s.head] = nil
	s.head++

	s.queuedBytes -= len(item)
	if s.head == len(s.queue) {
		s.queue = nil
		s.head = 0
	}

	s.writing = true

	return item, true
}
