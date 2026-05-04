package protocol

import (
	"bufio"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

const CompressionZstd = "zstd"

type CompressionInfo struct {
	Algorithm string `json:"algorithm"`
}

func (c *Conn) EnableZstdWriter() (func() error, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.w.Flush(); err != nil {
		return nil, fmt.Errorf("flush before zstd writer: %w", err)
	}

	encoder, err := zstd.NewWriter(c.conn, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, fmt.Errorf("create zstd writer: %w", err)
	}

	c.w = bufio.NewWriter(encoder)

	closed := false
	closeWriter := func() error {
		c.mu.Lock()
		defer c.mu.Unlock()

		if closed {
			return nil
		}

		closed = true

		if err := c.w.Flush(); err != nil {
			return fmt.Errorf("flush zstd writer: %w", err)
		}

		if err := encoder.Close(); err != nil {
			return fmt.Errorf("close zstd writer: %w", err)
		}

		return nil
	}

	return closeWriter, nil
}

func (c *Conn) EnableZstdReader() (func(), error) {
	decoder, err := zstd.NewReader(c.r)
	if err != nil {
		return nil, fmt.Errorf("create zstd reader: %w", err)
	}

	c.r = bufio.NewReader(decoder)

	closed := false
	closeReader := func() {
		if closed {
			return
		}

		closed = true
		decoder.Close()
	}

	return closeReader, nil
}
