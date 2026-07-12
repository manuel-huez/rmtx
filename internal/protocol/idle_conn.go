package protocol

import (
	"net"
	"sync"
	"time"
)

type IdleDeadlineConn struct {
	net.Conn

	mu           sync.RWMutex
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func NewIdleDeadlineConn(conn net.Conn) *IdleDeadlineConn {
	return &IdleDeadlineConn{Conn: conn}
}

func (c *IdleDeadlineConn) Underlying() net.Conn {
	return c.Conn
}

func (c *IdleDeadlineConn) SetIdleTimeout(timeout time.Duration) {
	c.SetReadIdleTimeout(timeout)
	c.SetWriteIdleTimeout(timeout)
}

func (c *IdleDeadlineConn) SetReadIdleTimeout(timeout time.Duration) {
	c.mu.Lock()
	c.readTimeout = timeout
	c.mu.Unlock()

	if timeout <= 0 {
		_ = c.SetReadDeadline(time.Time{})
	}
}

func (c *IdleDeadlineConn) SetWriteIdleTimeout(timeout time.Duration) {
	c.mu.Lock()
	c.writeTimeout = timeout
	c.mu.Unlock()

	if timeout <= 0 {
		_ = c.SetWriteDeadline(time.Time{})
	}
}

func (c *IdleDeadlineConn) Read(p []byte) (int, error) {
	if timeout := c.readIdleTimeout(); timeout > 0 {
		_ = c.SetReadDeadline(time.Now().Add(timeout))
	}

	return c.Conn.Read(p)
}

func (c *IdleDeadlineConn) Write(p []byte) (int, error) {
	if timeout := c.writeIdleTimeout(); timeout > 0 {
		_ = c.SetWriteDeadline(time.Now().Add(timeout))
	}

	return c.Conn.Write(p)
}

func (c *IdleDeadlineConn) readIdleTimeout() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.readTimeout
}

func (c *IdleDeadlineConn) writeIdleTimeout() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.writeTimeout
}
