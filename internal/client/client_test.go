package client

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/clientstate"
	"github.com/manuel-huez/rmtx/internal/host"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/syncfs"
)

func TestPrepareUploadItemsUsesRelativeDisplayPath(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "dir", "file.txt")
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(src, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}

	items, total, err := prepareUploadItems(
		[]string{"hash"},
		map[string]string{"hash": src},
		root,
	)
	if err != nil {
		t.Fatal(err)
	}

	if total != int64(len("content")) {
		t.Fatalf("total bytes=%d want %d", total, len("content"))
	}

	if len(items) != 1 {
		t.Fatalf("items=%d want 1", len(items))
	}

	if items[0].displayPath != "dir/file.txt" {
		t.Fatalf("display path=%q want dir/file.txt", items[0].displayPath)
	}
}

func TestFormatCommandQuotesArgs(t *testing.T) {
	got := formatCommand([]string{"sh", "-c", "echo 'hello world'", ""})
	want := `sh -c 'echo '\''hello world'\''' ''`

	if got != want {
		t.Fatalf("command=%q want %q", got, want)
	}
}

func TestRunLoggerStageUsesDelimiter(t *testing.T) {
	var logs bytes.Buffer

	newRunLogger(&logs).Stage("execute remote command")

	if got, want := logs.String(), "rmtx: === execute remote command ===\n"; got != want {
		t.Fatalf("stage log=%q want %q", got, want)
	}
}

func TestBuildRunRequestRejectsInvalidSyncBack(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "src"), 0o755); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	_, _, err := buildRunRequest(
		context.Background(),
		root,
		".",
		&ExecOptions{
			Mounts:    []syncfs.MountSpec{{Path: "src"}},
			SyncBack:  []string{"generated/"},
			ContextID: "ctx",
		},
		newRunLogger(&logs),
	)
	if err == nil {
		t.Fatal("expected invalid sync_back to fail")
	}

	if !strings.Contains(err.Error(), "sync_back path") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendMissingBlobsDoesNotLogFilesWhenNothingTransfers(t *testing.T) {
	var logs bytes.Buffer

	err := sendMissingBlobs(
		context.Background(),
		nil,
		protocol.NeedBlobs{},
		nil,
		ExecOptions{},
		newRunLogger(&logs),
	)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(logs.String(), "upload file started") ||
		strings.Contains(logs.String(), "upload file done") {
		t.Fatalf("unexpected file upload log: %s", logs.String())
	}
}

func TestRequestPairCodeFallsBackToReverseConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var logs lockedBuffer

	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
		StateDir:         t.TempDir(),
		DiscoveryService: "test-rmtx",
		Logger:           log.New(&logs, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)

	go func() { errCh <- server.Serve(ctx) }()

	waitForAddr(t, server)

	resp, err := RequestPairCode(ctx, PairOptions{
		Address:          "127.0.0.1:1",
		DiscoveryService: "test-rmtx",
		Host: clientstate.HostRecord{
			Fingerprint: server.Fingerprint(),
		},
		ClientLabel: "reverse-client",
	})
	if err != nil {
		t.Fatal(err)
	}

	if strings.TrimSpace(resp.HostName) == "" {
		t.Fatal("expected host name")
	}

	assertLogDoesNotContain(t, &logs, "request failed", 400*time.Millisecond)

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server exited with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}
}

func TestRunHandshakeStreamsSetupOutput(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	defer func() { _ = clientConn.Close() }()

	serverErr := make(chan error, 1)

	go func() {
		defer func() { _ = serverConn.Close() }()

		serverErr <- serveHandshakeSetupOutput(protocol.NewConn(serverConn))
	}()

	var stderr bytes.Buffer

	ready, err := runHandshake(
		context.Background(),
		protocol.NewConn(clientConn),
		protocol.RunRequest{ContextID: "ctx", WorkDir: ".", Command: []string{"true"}},
		nil,
		ExecOptions{Stderr: &stderr},
		newRunLogger(&bytes.Buffer{}),
	)
	if err != nil {
		t.Fatal(err)
	}

	if ready.ContextID != "ctx" {
		t.Fatalf("ready context=%s want ctx", ready.ContextID)
	}

	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"image setup before sync\n",
		"image setup before ready\n",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("missing setup output %q in %q", want, stderr.String())
		}
	}
}

func TestRunHandshakeCancelAfterSyncCompleteStreamsOutputAndSendsCancel(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	defer func() { _ = clientConn.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErr := make(chan error, 1)

	go func() {
		defer func() { _ = serverConn.Close() }()

		serverErr <- serveCancellableHandshake(protocol.NewConn(serverConn), cancel)
	}()

	var stderr bytes.Buffer

	ready, stopSession, err := runHandshakeWithLiveness(
		ctx,
		protocol.NewConn(clientConn),
		protocol.RunRequest{ContextID: "ctx", WorkDir: ".", Command: []string{"true"}},
		nil,
		ExecOptions{Stderr: &stderr},
		newRunLogger(&bytes.Buffer{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stopSession()

	if ready.ContextID != "ctx" {
		t.Fatalf("ready context=%s want ctx", ready.ContextID)
	}

	if !strings.Contains(stderr.String(), "setup after cancel\n") {
		t.Fatalf("missing setup output after cancel in %q", stderr.String())
	}

	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func serveHandshakeSetupOutput(conn *protocol.Conn) error {
	if err := expectDiscardedClientFrame(conn, protocol.MsgRunRequest); err != nil {
		return err
	}

	if err := writeSetupStderr(conn, "image setup before sync\n"); err != nil {
		return err
	}

	if err := conn.WriteJSON(protocol.MsgHeartbeat, nil); err != nil {
		return err
	}

	if err := conn.WriteJSON(protocol.MsgNeedBlobs, protocol.NeedBlobs{}); err != nil {
		return err
	}

	if err := expectDiscardedClientFrame(conn, protocol.MsgSyncComplete); err != nil {
		return err
	}

	if err := writeSetupStderr(conn, "image setup before ready\n"); err != nil {
		return err
	}

	return conn.WriteJSON(
		protocol.MsgWorkspaceReady,
		protocol.WorkspaceReady{ContextID: "ctx", Workspace: "/tmp/rmtx"},
	)
}

func serveCancellableHandshake(conn *protocol.Conn, cancel context.CancelFunc) error {
	if err := expectDiscardedClientFrame(conn, protocol.MsgRunRequest); err != nil {
		return err
	}

	if err := conn.WriteJSON(protocol.MsgNeedBlobs, protocol.NeedBlobs{}); err != nil {
		return err
	}

	if err := expectClientFrameSkippingHeartbeat(conn, protocol.MsgSyncComplete); err != nil {
		return err
	}

	time.Sleep(20 * time.Millisecond)
	cancel()

	if err := writeSetupStderr(conn, "setup after cancel\n"); err != nil {
		return err
	}

	if err := conn.WriteJSON(
		protocol.MsgWorkspaceReady,
		protocol.WorkspaceReady{ContextID: "ctx", Workspace: "/tmp/rmtx"},
	); err != nil {
		return err
	}

	return expectClientFrameSkippingHeartbeat(conn, protocol.MsgRunCancel)
}

func expectDiscardedClientFrame(conn *protocol.Conn, wantType string) error {
	head, err := conn.ReadHeader()
	if err != nil {
		return err
	}

	if head.Type != wantType {
		return fmt.Errorf("expected %s, got %s", wantType, head.Type)
	}

	return conn.DiscardPayload(head)
}

func expectClientFrameSkippingHeartbeat(conn *protocol.Conn, wantType string) error {
	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return err
		}

		if err := conn.DiscardPayload(head); err != nil {
			return err
		}

		if head.Type == protocol.MsgHeartbeat {
			continue
		}

		if head.Type != wantType {
			return fmt.Errorf("expected %s, got %s", wantType, head.Type)
		}

		return nil
	}
}

func writeSetupStderr(conn *protocol.Conn, output string) error {
	return conn.WriteBytes(
		protocol.MsgExecOutput,
		protocol.OutputInfo{Stream: "stderr"},
		[]byte(output),
	)
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

func assertLogDoesNotContain(
	t *testing.T,
	logs interface{ String() string },
	needle string,
	duration time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		content := logs.String()
		if strings.Contains(content, needle) {
			t.Fatalf("log contained %q: %s", needle, content)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func waitForAddr(t *testing.T, server interface{ Addr() string }) string {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := server.Addr(); addr != "" {
			return addr
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("timed out waiting for server address")

	return ""
}
