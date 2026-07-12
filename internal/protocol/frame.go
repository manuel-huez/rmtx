package protocol

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

const (
	MaxFrameHeaderSize  = 8 << 20
	MaxFramePayloadSize = 64 << 20
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

func (c *Conn) WriteJSON(msgType string, payload any) error {
	return c.write(msgType, payload, nil, 0)
}

func (c *Conn) WriteBytes(msgType string, payload any, data []byte) error {
	return c.write(msgType, payload, data, int64(len(data)))
}

func (c *Conn) WriteFrom(msgType string, payload any, src io.Reader, payloadLen int64) error {
	return c.write(msgType, payload, src, payloadLen)
}

func writePayload(w *bufio.Writer, data any, payloadLen int64) error {
	switch v := data.(type) {
	case []byte:
		if int64(len(v)) != payloadLen {
			return fmt.Errorf("payload length mismatch: %d != %d", len(v), payloadLen)
		}

		if _, err := w.Write(v); err != nil {
			return fmt.Errorf("write frame payload: %w", err)
		}
	case io.Reader:
		if _, err := io.CopyN(w, v, payloadLen); err != nil {
			return fmt.Errorf("stream frame payload: %w", err)
		}
	default:
		return fmt.Errorf("unsupported payload type %T", data)
	}

	return nil
}

func (c *Conn) write(msgType string, payload any, data any, payloadLen int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.writeLocked(msgType, payload, data, payloadLen)
}

//nolint:cyclop // Frame bounds and each wire write need distinct errors.
func (c *Conn) writeLocked(msgType string, payload any, data any, payloadLen int64) error {
	if msgType == "" {
		return errors.New("frame type is required")
	}

	if payloadLen < 0 || payloadLen > MaxFramePayloadSize {
		return fmt.Errorf("invalid frame payload length %d", payloadLen)
	}

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

	if len(encoded) == 0 || len(encoded) > MaxFrameHeaderSize {
		return fmt.Errorf("invalid frame header length %d", len(encoded))
	}

	if err := binary.Write(c.w, binary.BigEndian, uint32(len(encoded))); err != nil {
		return fmt.Errorf("write frame size: %w", err)
	}

	if _, err := c.w.Write(encoded); err != nil {
		return fmt.Errorf("write frame header: %w", err)
	}

	if payloadLen > 0 {
		if err := writePayload(c.w, data, payloadLen); err != nil {
			return err
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

	if size == 0 || size > MaxFrameHeaderSize {
		return Header{}, fmt.Errorf("invalid frame header length %d", size)
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(c.r, buf); err != nil {
		return Header{}, fmt.Errorf("read frame header: %w", err)
	}

	var h Header
	if err := json.Unmarshal(buf, &h); err != nil {
		return Header{}, fmt.Errorf("decode frame header: %w", err)
	}

	if h.PayloadLen < 0 || h.PayloadLen > MaxFramePayloadSize {
		return Header{}, fmt.Errorf("invalid frame payload length %d", h.PayloadLen)
	}

	if h.Type == "" {
		return Header{}, errors.New("frame type is required")
	}

	return h, nil
}

func (c *Conn) PayloadReader(h Header) io.Reader { return io.LimitReader(c.r, h.PayloadLen) }

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
