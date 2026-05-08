package host

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/oci"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/syncfs"
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

func TestWriteRunLogLinePrefixesRmtx(t *testing.T) {
	var out bytes.Buffer

	writeRunLogLine(&out, "=== runtime context setup ===")
	writeRunLogLine(&out, "setup command: %s", "uv sync")

	want := "" +
		"rmtx: === runtime context setup ===\n" +
		"rmtx: setup command: uv sync\n"
	if out.String() != want {
		t.Fatalf("run log=%q want %q", out.String(), want)
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

func TestHostLogSubscriptionAddsSectionTimingToRunLogLines(t *testing.T) {
	serverRaw, clientRaw := net.Pipe()

	defer func() { _ = serverRaw.Close() }()
	defer func() { _ = clientRaw.Close() }()

	sub := newHostLogSubscription(protocol.NewConn(serverRaw))
	defer sub.Close()

	writeRunLogLine(sub, "=== runtime context setup ===")

	if err := clientRaw.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	client := protocol.NewConn(clientRaw)

	head, err := client.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}

	payload, err := io.ReadAll(client.PayloadReader(head))
	if err != nil {
		t.Fatal(err)
	}

	got := string(payload)
	if !strings.HasPrefix(got, "rmtx: === runtime context setup === elapsed=") ||
		!strings.Contains(got, " total=") {
		t.Fatalf("section log missing timing: %q", got)
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
	logger.Printf("request received: remote=%s type=%s", "127.0.0.1", protocol.MsgRunRequest)

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

	want := "request received: remote=127.0.0.1 type=run_request\n"
	if hostLogs.String() != want {
		t.Fatalf("host logs=%q want %q", hostLogs.String(), want)
	}
}

func TestPullOCIImageWritesRunLogs(t *testing.T) {
	var (
		hostLogs bytes.Buffer
		runLogs  bytes.Buffer
	)

	server, err := New(Options{
		StateDir: t.TempDir(),
		Logger:   log.New(&hostLogs, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	store := oci.NewStore(t.TempDir())
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	ref, err := oci.ParseReference("busybox:latest")
	if err != nil {
		t.Fatal(err)
	}

	_, err = server.pullOCIImage(
		context.Background(),
		ref,
		protocol.RuntimeSpec{PullPolicy: "never"},
		store,
		"ctx",
		&runLogs,
	)
	if err == nil {
		t.Fatal("expected missing image error")
	}

	want := "runtime image pull start: context=ctx image=docker.io/library/busybox:latest pull_policy=never"
	if !strings.Contains(hostLogs.String(), want) {
		t.Fatalf("host logs missing pull start: %q", hostLogs.String())
	}

	if !strings.Contains(runLogs.String(), "rmtx: "+want+"\n") {
		t.Fatalf("run logs missing pull start: %q", runLogs.String())
	}
}

func TestBuildProgressWritesRunLogs(t *testing.T) {
	var (
		hostLogs bytes.Buffer
		runLogs  bytes.Buffer
	)

	server, err := New(Options{
		StateDir: t.TempDir(),
		Logger:   log.New(&hostLogs, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	progress := server.logBuildProgress("ctx", "sess", "post-run", &runLogs)
	progress(syncfs.BuildProgress{
		Phase:      "hash",
		Done:       true,
		Hashed:     2,
		TotalFiles: 3,
		Bytes:      42,
	})

	want := "post-run hash done: context=ctx session=sess files=2/3 bytes=42"
	if !strings.Contains(hostLogs.String(), want) {
		t.Fatalf("host logs missing progress: %q", hostLogs.String())
	}

	if !strings.Contains(runLogs.String(), "rmtx: "+want+"\n") {
		t.Fatalf("run logs missing progress: %q", runLogs.String())
	}
}
