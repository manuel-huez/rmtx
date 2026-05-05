package host

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func TestCommandOutputCollectorLogsCompleteLines(t *testing.T) {
	var (
		logs bytes.Buffer
		live bytes.Buffer
	)

	collector := &commandOutputCollector{
		logger: log.New(&logs, "", 0),
		prefix: "runtime image setup command",
		live:   &live,
	}

	if _, err := collector.Write([]byte("Get:1 foo\nGet:2")); err != nil {
		t.Fatal(err)
	}

	if _, err := collector.Write([]byte(" bar\rGet:3 baz\r\n")); err != nil {
		t.Fatal(err)
	}

	collector.Flush()

	wantLogs := "" +
		"runtime image setup command: Get:1 foo\n" +
		"runtime image setup command: Get:2 bar\n" +
		"runtime image setup command: Get:3 baz\n"
	if logs.String() != wantLogs {
		t.Fatalf("logs=%q want %q", logs.String(), wantLogs)
	}

	wantLive := "Get:1 foo\nGet:2 bar\rGet:3 baz\r\n"
	if live.String() != wantLive {
		t.Fatalf("live output=%q want %q", live.String(), wantLive)
	}
}

func TestHostLogSubscriptionStreamsDirectWrites(t *testing.T) {
	serverRaw, clientRaw := net.Pipe()

	defer func() { _ = serverRaw.Close() }()
	defer func() { _ = clientRaw.Close() }()

	sub := newHostLogSubscription(protocol.NewConn(serverRaw))
	defer sub.Close()

	if _, err := sub.Write([]byte("Get:1 package\n")); err != nil {
		t.Fatal(err)
	}

	if err := clientRaw.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	client := protocol.NewConn(clientRaw)

	head, err := client.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}

	if head.Type != protocol.MsgExecOutput {
		t.Fatalf("frame type=%s want %s", head.Type, protocol.MsgExecOutput)
	}

	info, err := protocol.DecodeData[protocol.OutputInfo](head)
	if err != nil {
		t.Fatal(err)
	}

	if info.Stream != "stderr" {
		t.Fatalf("stream=%s want stderr", info.Stream)
	}

	payload, err := io.ReadAll(client.PayloadReader(head))
	if err != nil {
		t.Fatal(err)
	}

	want := "Get:1 package\n"
	if string(payload) != want {
		t.Fatalf("payload=%q want %q", payload, want)
	}
}

func TestHostLogHubDoesNotStreamGeneralLogs(t *testing.T) {
	serverRaw, clientRaw := net.Pipe()

	defer func() { _ = serverRaw.Close() }()
	defer func() { _ = clientRaw.Close() }()

	var hostLogs bytes.Buffer

	hub := newHostLogHub(&hostLogs)

	sub := hub.Subscribe(protocol.NewConn(serverRaw))
	defer sub.Close()

	logger := log.New(hub, "", 0)
	logger.Printf("pair request from 127.0.0.1 client=%q code=%s", "client", "123456")

	if err := clientRaw.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	client := protocol.NewConn(clientRaw)
	if head, err := client.ReadHeader(); err == nil {
		t.Fatalf("unexpected streamed frame: %s", head.Type)
	} else {
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("read header err=%v want timeout", err)
		}
	}

	want := "pair request from 127.0.0.1 client=\"client\" code=123456\n"
	if hostLogs.String() != want {
		t.Fatalf("host logs=%q want %q", hostLogs.String(), want)
	}
}
