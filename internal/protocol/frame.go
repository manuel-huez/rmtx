package protocol

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
)

type Header struct {
	Type       string          `json:"type"`
	PayloadLen int64           `json:"payload_len,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
}

type Conn struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
	mu   sync.Mutex
}

func NewConn(conn net.Conn) *Conn {
	return &Conn{conn: conn, r: bufio.NewReader(conn), w: bufio.NewWriter(conn)}
}

func (c *Conn) Raw() net.Conn { return c.conn }
func (c *Conn) Close() error  { return c.conn.Close() }

func (c *Conn) WriteJSON(msgType string, payload any) error {
	return c.write(msgType, payload, nil, 0)
}

func (c *Conn) WriteBytes(msgType string, payload any, data []byte) error {
	return c.write(msgType, payload, data, int64(len(data)))
}

func (c *Conn) WriteFrom(msgType string, payload any, src io.Reader, payloadLen int64) error {
	return c.write(msgType, payload, src, payloadLen)
}

func (c *Conn) write(msgType string, payload any, data any, payloadLen int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	h := Header{Type: msgType, PayloadLen: payloadLen}

	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal frame payload: %w", err)
		}

		h.Data = raw
	}

	encoded, err := json.Marshal(h)
	if err != nil {
		return fmt.Errorf("marshal frame header: %w", err)
	}

	if err := binary.Write(c.w, binary.BigEndian, uint32(len(encoded))); err != nil {
		return fmt.Errorf("write frame size: %w", err)
	}

	if _, err := c.w.Write(encoded); err != nil {
		return fmt.Errorf("write frame header: %w", err)
	}

	if payloadLen > 0 {
		switch v := data.(type) {
		case []byte:
			if int64(len(v)) != payloadLen {
				return fmt.Errorf("payload length mismatch: %d != %d", len(v), payloadLen)
			}

			if _, err := c.w.Write(v); err != nil {
				return fmt.Errorf("write frame payload: %w", err)
			}
		case io.Reader:
			if _, err := io.CopyN(c.w, v, payloadLen); err != nil {
				return fmt.Errorf("stream frame payload: %w", err)
			}
		default:
			return fmt.Errorf("unsupported payload type %T", data)
		}
	}

	if err := c.w.Flush(); err != nil {
		return fmt.Errorf("flush frame: %w", err)
	}

	return nil
}

func (c *Conn) ReadHeader() (Header, error) {
	var size uint32
	if err := binary.Read(c.r, binary.BigEndian, &size); err != nil {
		return Header{}, err
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(c.r, buf); err != nil {
		return Header{}, fmt.Errorf("read frame header: %w", err)
	}

	var h Header
	if err := json.Unmarshal(buf, &h); err != nil {
		return Header{}, fmt.Errorf("decode frame header: %w", err)
	}

	return h, nil
}

func (c *Conn) ReadPayload(h Header) ([]byte, error) {
	if h.PayloadLen == 0 {
		return nil, nil
	}

	buf := make([]byte, h.PayloadLen)
	if _, err := io.ReadFull(c.r, buf); err != nil {
		return nil, fmt.Errorf("read frame payload: %w", err)
	}

	return buf, nil
}

func (c *Conn) PayloadReader(h Header) io.Reader { return io.LimitReader(c.r, h.PayloadLen) }

func (c *Conn) CopyPayload(h Header, dst io.Writer) error {
	if h.PayloadLen == 0 {
		return nil
	}

	_, err := io.CopyN(dst, c.r, h.PayloadLen)
	if err != nil {
		return fmt.Errorf("copy payload: %w", err)
	}

	return nil
}

func (c *Conn) DiscardPayload(h Header) error {
	if h.PayloadLen == 0 {
		return nil
	}

	_, err := io.CopyN(io.Discard, c.r, h.PayloadLen)
	if err != nil {
		return fmt.Errorf("discard payload: %w", err)
	}

	return nil
}

func DecodeData[T any](h Header) (T, error) {
	var out T
	if len(h.Data) == 0 {
		return out, nil
	}

	if err := json.Unmarshal(h.Data, &out); err != nil {
		return out, fmt.Errorf("decode frame data: %w", err)
	}

	return out, nil
}
