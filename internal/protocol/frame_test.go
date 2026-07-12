package protocol

import (
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
)

func TestReadHeaderRejectsInvalidLengths(t *testing.T) {
	for _, length := range []uint32{0, MaxFrameHeaderSize + 1} {
		client, server := net.Pipe()
		go func() { _ = binary.Write(client, binary.BigEndian, length) }()

		if _, err := NewConn(server).ReadHeader(); err == nil {
			t.Fatalf("ReadHeader() accepted length %d", length)
		}

		_ = client.Close()
		_ = server.Close()
	}
}

func TestReadHeaderRejectsInvalidFields(t *testing.T) {
	tests := []struct {
		body string
		want string
	}{
		{`{"type":"blob_chunk","payload_len":-1}`, "invalid frame payload length -1"},
		{`{}`, "frame type is required"},
	}

	for _, tt := range tests {
		client, server := net.Pipe()
		go func() {
			_ = binary.Write(client, binary.BigEndian, uint32(len(tt.body)))
			_, _ = io.WriteString(client, tt.body)
		}()

		_, err := NewConn(server).ReadHeader()
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Fatalf("ReadHeader() error = %v, want containing %q", err, tt.want)
		}

		_ = client.Close()
		_ = server.Close()
	}
}

func TestWriteFromRejectsInvalidPayloadLength(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close(); _ = server.Close() }()

	err := NewConn(client).WriteFrom("blob", nil, strings.NewReader(""), -1)
	if err == nil || !strings.Contains(err.Error(), "invalid frame payload length -1") {
		t.Fatalf("WriteFrom() error = %v", err)
	}
}
