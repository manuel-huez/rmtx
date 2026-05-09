package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
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
	"github.com/manuel-huez/rmtx/internal/security"
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

	got := logs.String()
	if !strings.Contains(got, "rmtx: === execute remote command ===") ||
		!strings.Contains(got, "elapsed=") ||
		!strings.Contains(got, "total=") {
		t.Fatalf("stage log missing timing fields: %q", got)
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

func TestFillDownloadPipelineQueuesWindowWithoutResponse(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	pending := make([]protocol.BlobChunkInfo, downloadPipelineWindow+2)
	for i := range pending {
		pending[i] = protocol.BlobChunkInfo{
			Hash:   fmt.Sprintf("hash-%d", i),
			Size:   int64(downloadPipelineWindow + 2),
			Offset: int64(i),
		}
	}

	inFlight := map[blobChunkKey]protocol.BlobChunkInfo{}
	inFlightOrder := []protocol.BlobChunkInfo{}
	done := make(chan error, 1)
	go func() {
		_, err := fillDownloadPipeline(
			context.Background(),
			&blobTransferConn{conn: protocol.NewConn(clientConn)},
			"token",
			ExecOptions{ContextID: "ctx", Session: "session"},
			&pending,
			inFlight,
			&inFlightOrder,
		)
		done <- err
	}()

	server := protocol.NewConn(serverConn)
	for i := range downloadPipelineWindow {
		head, err := server.ReadHeader()
		if err != nil {
			t.Fatal(err)
		}
		if head.Type != protocol.MsgBlobTransferRequest {
			t.Fatalf("frame %d type=%q want %q", i, head.Type, protocol.MsgBlobTransferRequest)
		}
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("fillDownloadPipeline blocked waiting for chunk responses")
	}

	if len(inFlight) != downloadPipelineWindow {
		t.Fatalf("inFlight=%d want %d", len(inFlight), downloadPipelineWindow)
	}
	if len(pending) != 2 {
		t.Fatalf("pending=%d want 2", len(pending))
	}
}

func TestRetryDownloadPipelineCountsOnlyFailedChunks(t *testing.T) {
	failed := []protocol.BlobChunkInfo{
		{Hash: "failed-1", Size: 10, Offset: 0},
		{Hash: "failed-2", Size: 10, Offset: 1},
	}
	queued := []protocol.BlobChunkInfo{
		{Hash: "queued-1", Size: 10, Offset: 2},
		{Hash: "queued-2", Size: 10, Offset: 3},
	}
	attempts := map[blobChunkKey]int{}
	for _, chunk := range failed {
		attempts[keyBlobChunk(chunk)] = blobTransferMaxAttempts - 1
	}

	err := retryDownloadPipeline(
		context.Background(),
		nil,
		attempts,
		failed,
		nil,
		&syncfs.ChunkReadError{Hash: "failed-1", Err: io.ErrUnexpectedEOF},
	)
	if err == nil {
		t.Fatal("retryDownloadPipeline succeeded after max attempts")
	}

	for _, chunk := range failed {
		if got := attempts[keyBlobChunk(chunk)]; got != blobTransferMaxAttempts {
			t.Fatalf("failed chunk attempts=%d want %d", got, blobTransferMaxAttempts)
		}
	}
	for _, chunk := range queued {
		if got := attempts[keyBlobChunk(chunk)]; got != 0 {
			t.Fatalf("queued chunk attempts=%d want 0", got)
		}
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

func TestRequestPairCodeRacesReverseWhenDirectTLSStalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stalled := startStalledTCPListener(t)

	var logs lockedBuffer
	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
		StateDir:         t.TempDir(),
		DiscoveryService: "test-rmtx-race",
		Logger:           log.New(&logs, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(ctx) }()
	waitForAddr(t, server)

	start := time.Now()
	_, err = RequestPairCode(ctx, PairOptions{
		Address:          stalled.Addr().String(),
		DiscoveryService: "test-rmtx-race",
		Host: clientstate.HostRecord{
			Fingerprint: server.Fingerprint(),
		},
		ClientLabel: "reverse-client",
	})
	if err != nil {
		t.Fatal(err)
	}

	if elapsed := time.Since(start); elapsed >= directDialTimeout {
		t.Fatalf("reverse fallback elapsed=%s want less than direct timeout %s", elapsed, directDialTimeout)
	}

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

func TestUpdatedRemoteConnKeepsConnectionForNextRequest(t *testing.T) {
	oldClientVersion := clientVersion
	clientVersion = func() string { return "v0.0.1" }
	defer func() { clientVersion = oldClientVersion }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stateDir := t.TempDir()
	var logs lockedBuffer
	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
		StateDir:         stateDir,
		DiscoveryService: "test-rmtx-reuse",
		Logger:           log.New(&logs, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(ctx) }()
	waitForAddr(t, server)

	pairCode, err := host.CreatePairCodeInfo(stateDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, keyPEM, csrPEM, err := GenerateClientIdentity("reuse-client")
	if err != nil {
		t.Fatal(err)
	}
	pairResp, err := PairHost(ctx, PairOptions{
		Address:          server.Addr(),
		DiscoveryService: "test-rmtx-reuse",
		Host: clientstate.HostRecord{
			Fingerprint: server.Fingerprint(),
		},
		Code:        pairCode.Code,
		ClientLabel: "reuse-client",
		CSRPEM:      csrPEM,
	})
	if err != nil {
		t.Fatal(err)
	}

	conn, err := updatedRemoteConn(ctx, RemoteOptions{
		Address:          server.Addr(),
		DiscoveryService: "test-rmtx-reuse",
		Host: clientstate.HostRecord{
			Fingerprint: server.Fingerprint(),
		},
		ClientCertPEM: []byte(pairResp.ClientCertPEM),
		ClientKeyPEM:  keyPEM,
		Stderr:        io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeQuietly(conn.Raw())

	if err := conn.WriteJSON(protocol.MsgHostStatsRequest, protocol.HostStatsRequest{}); err != nil {
		t.Fatal(err)
	}
	stats, err := expectDataFrameWithOutput[protocol.HostStatsResponse](
		conn,
		protocol.MsgHostStatsResponse,
		io.Discard,
	)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Fingerprint != server.Fingerprint() {
		t.Fatalf("stats fingerprint=%s want %s", stats.Fingerprint, server.Fingerprint())
	}

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

func TestUpdatedRemoteConnDialsFreshForUncomparableHostVersion(t *testing.T) {
	oldClientVersion := clientVersion
	clientVersion = func() string { return "v0.0.1" }
	defer func() { clientVersion = oldClientVersion }()

	pki, err := security.EnsureHostPKI(t.TempDir(), "one-shot-host")
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig, fingerprint, err := security.ServerTLSConfig(pki)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tlsLn := tls.NewListener(ln, tlsConfig)

	requestTypes := make(chan string, 4)
	serverErrCh := make(chan error, 1)
	serverDone := make(chan struct{})
	sendServerErr := func(err error) {
		select {
		case serverErrCh <- err:
		default:
		}
	}

	go func() {
		defer close(serverDone)

		for {
			raw, err := tlsLn.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				sendServerErr(err)

				return
			}

			conn := protocol.NewConn(raw)
			head, err := conn.ReadHeader()
			if err != nil {
				_ = raw.Close()
				if !protocol.IsDisconnectError(err) {
					sendServerErr(err)

					return
				}

				continue
			}

			requestTypes <- head.Type
			if err := conn.DiscardPayload(head); err != nil {
				_ = raw.Close()
				sendServerErr(err)

				return
			}

			var writeErr error
			switch head.Type {
			case protocol.MsgPingRequest:
				writeErr = conn.WriteJSON(protocol.MsgPingResponse, protocol.PingResponse{
					Online:      true,
					Version:     "dev",
					Fingerprint: fingerprint,
					Now:         time.Now().UTC(),
				})
			case protocol.MsgHostStatsRequest:
				writeErr = conn.WriteJSON(protocol.MsgHostStatsResponse, protocol.HostStatsResponse{
					Version:     "dev",
					Fingerprint: fingerprint,
					Now:         time.Now().UTC(),
				})
			default:
				writeErr = conn.WriteJSON(protocol.MsgError, protocol.ErrorMessage{
					Message: fmt.Sprintf("unexpected request %s", head.Type),
				})
			}
			_ = raw.Close()
			if writeErr != nil {
				sendServerErr(writeErr)

				return
			}
		}
	}()
	defer func() {
		_ = tlsLn.Close()

		select {
		case <-serverDone:
		case <-time.After(2 * time.Second):
			t.Error("timed out waiting for one-shot host shutdown")
		}

		select {
		case err := <-serverErrCh:
			t.Errorf("one-shot host failed: %v", err)
		default:
		}
	}()

	conn, err := updatedRemoteConn(context.Background(), RemoteOptions{
		Address: ln.Addr().String(),
		Host: clientstate.HostRecord{
			Fingerprint: fingerprint,
		},
		Stderr: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeQuietly(conn.Raw())

	if err := conn.WriteJSON(protocol.MsgHostStatsRequest, protocol.HostStatsRequest{}); err != nil {
		t.Fatal(err)
	}
	stats, err := expectDataFrameWithOutput[protocol.HostStatsResponse](
		conn,
		protocol.MsgHostStatsResponse,
		io.Discard,
	)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Fingerprint != fingerprint {
		t.Fatalf("stats fingerprint=%s want %s", stats.Fingerprint, fingerprint)
	}

	for _, want := range []string{protocol.MsgPingRequest, protocol.MsgHostStatsRequest} {
		select {
		case got := <-requestTypes:
			if got != want {
				t.Fatalf("request type=%s want %s", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %s", want)
		}
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

func startStalledTCPListener(t *testing.T) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	var connsMu sync.Mutex
	var conns []net.Conn

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			connsMu.Lock()
			conns = append(conns, conn)
			connsMu.Unlock()

			go func(conn net.Conn) {
				<-done
				_ = conn.Close()
			}(conn)
		}
	}()

	t.Cleanup(func() {
		close(done)
		_ = ln.Close()

		connsMu.Lock()
		defer connsMu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})

	return ln
}
