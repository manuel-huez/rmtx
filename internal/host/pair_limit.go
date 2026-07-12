package host

import (
	"net"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

const (
	pairLimitWindowDuration = time.Minute
	maxPairCodeRequests     = 10
	maxPairAttempts         = 10
	maxPairLimitWindows     = 1024
)

type pairLimitWindow struct {
	started time.Time
	count   int
}

//nolint:cyclop // Rate-window expiry, cardinality, and request limits share one lock.
func (s *Server) allowPairRequest(conn *protocol.Conn, kind string, limit int) bool {
	if conn == nil || conn.Raw() == nil || conn.Raw().RemoteAddr() == nil {
		return true
	}

	remote := conn.Raw().RemoteAddr().String()
	if host, _, err := net.SplitHostPort(remote); err == nil {
		remote = host
	}

	key := kind + "\x00" + remote
	now := time.Now()

	s.pairLimitsMu.Lock()
	defer s.pairLimitsMu.Unlock()

	if s.pairLimits == nil {
		s.pairLimits = map[string]pairLimitWindow{}
	}

	for existing, window := range s.pairLimits {
		if now.Sub(window.started) >= pairLimitWindowDuration {
			delete(s.pairLimits, existing)
		}
	}

	window, exists := s.pairLimits[key]
	if !exists && len(s.pairLimits) >= maxPairLimitWindows {
		return false
	}

	if window.started.IsZero() || now.Sub(window.started) >= pairLimitWindowDuration {
		window = pairLimitWindow{started: now}
	}

	if window.count >= limit {
		return false
	}

	window.count++
	s.pairLimits[key] = window

	return true
}
