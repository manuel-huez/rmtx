//go:build !windows

package host

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func TestRunPipeExecCommandDisconnectCancelsAfterStdinClose(t *testing.T) {
	t.Helper()

	serverConn, clientConn := net.Pipe()
	server := &Server{logger: log.New(io.Discard, "", 0)}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "sh", "-c", "sleep 60")
	cmd.Dir = t.TempDir()

	resultCh := make(chan struct {
		code int
		err  error
	}, 1)

	go func() {
		code, err := server.runPipeExecCommand(ctx, cancel, protocol.NewConn(serverConn), cmd)
		resultCh <- struct {
			code int
			err  error
		}{code, err}
	}()

	client := protocol.NewConn(clientConn)

	time.Sleep(40 * time.Millisecond)

	if err := client.WriteJSON(protocol.MsgStdinClose, nil); err != nil {
		t.Fatalf("send stdin close: %v", err)
	}

	_ = clientConn.Close()

	start := time.Now()

	select {
	case result := <-resultCh:
		if time.Since(start) > 2*time.Second {
			t.Fatalf("command took too long after stdin close: %s", time.Since(start))
		}

		if result.err == nil {
			t.Fatalf("expected run error from disconnect, got code=%d", result.code)
		}

		if !errors.Is(result.err, context.Canceled) &&
			!strings.Contains(result.err.Error(), "killed") {
			t.Fatalf("expected cancel/kill error, got %v", result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("command did not stop after stdin close + disconnect")
	}
}

func TestRunPipeExecCommandDisconnectCancelsProcessTree(t *testing.T) {
	t.Helper()

	serverConn, clientConn := net.Pipe()
	server := &Server{logger: log.New(io.Discard, "", 0)}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "sh", "-c", "sleep 60 & wait")
	cmd.Dir = t.TempDir()

	resultCh := make(chan struct {
		code int
		err  error
	}, 1)

	go func() {
		code, err := server.runPipeExecCommand(ctx, cancel, protocol.NewConn(serverConn), cmd)
		resultCh <- struct {
			code int
			err  error
		}{code, err}
	}()

	client := protocol.NewConn(clientConn)

	time.Sleep(40 * time.Millisecond)

	if err := client.WriteJSON(protocol.MsgStdinClose, nil); err != nil {
		t.Fatalf("send stdin close: %v", err)
	}

	_ = clientConn.Close()

	select {
	case <-resultCh:
		// pass: run loop returned after disconnect and kill path.
	case <-time.After(5 * time.Second):
		t.Fatal("command with child process did not stop after disconnect")
	}
}

func TestRunPipeExecCommandIdleTimeoutCancelsSilentClient(t *testing.T) {
	t.Helper()

	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	raw := protocol.NewIdleDeadlineConn(serverConn)
	raw.SetIdleTimeout(80 * time.Millisecond)

	server := &Server{logger: log.New(io.Discard, "", 0)}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "sh", "-c", "sleep 60")
	cmd.Dir = t.TempDir()

	resultCh := make(chan struct {
		code int
		err  error
	}, 1)

	go func() {
		code, err := server.runPipeExecCommand(ctx, cancel, protocol.NewConn(raw), cmd)
		resultCh <- struct {
			code int
			err  error
		}{code, err}
	}()

	client := protocol.NewConn(clientConn)
	time.Sleep(40 * time.Millisecond)

	if err := client.WriteJSON(protocol.MsgStdinClose, nil); err != nil {
		t.Fatalf("send stdin close: %v", err)
	}

	select {
	case result := <-resultCh:
		if result.err == nil {
			t.Fatalf("expected idle timeout to cancel run, got code=%d", result.code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("command did not stop after silent client idle timeout")
	}
}

func TestRunPipeExecCommandHeartbeatsKeepIdleClientAlive(t *testing.T) {
	t.Helper()

	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	raw := protocol.NewIdleDeadlineConn(serverConn)
	raw.SetIdleTimeout(120 * time.Millisecond)

	server := &Server{logger: log.New(io.Discard, "", 0)}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "sh", "-c", "sleep 0.25")
	cmd.Dir = t.TempDir()

	resultCh := make(chan struct {
		code int
		err  error
	}, 1)

	go func() {
		code, err := server.runPipeExecCommand(ctx, cancel, protocol.NewConn(raw), cmd)
		resultCh <- struct {
			code int
			err  error
		}{code, err}
	}()

	client := protocol.NewConn(clientConn)
	if err := client.WriteJSON(protocol.MsgStdinClose, nil); err != nil {
		t.Fatalf("send stdin close: %v", err)
	}

	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	defer stopHeartbeat()

	go func() {
		ticker := time.NewTicker(30 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				_ = client.WriteJSON(protocol.MsgHeartbeat, nil)
			}
		}
	}()

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("run failed while client heartbeats were flowing: %v", result.err)
		}
		if result.code != 0 {
			t.Fatalf("exit code=%d want 0", result.code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("command did not finish while client heartbeats were flowing")
	}
}

func TestPipeInputForwardingReadsCancelAfterQueuedStdin(t *testing.T) {
	t.Helper()

	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	cancelled := make(chan struct{})
	server := &Server{logger: log.New(io.Discard, "", 0)}
	input := server.startPipeInputForwarding(protocol.NewConn(serverConn), func() {
		select {
		case <-cancelled:
		default:
			close(cancelled)
		}
	})
	defer input.Stop()

	client := protocol.NewConn(clientConn)
	if err := client.WriteBytes(protocol.MsgStdinData, nil, []byte("queued before command")); err != nil {
		t.Fatalf("send stdin data: %v", err)
	}
	if err := client.WriteJSON(protocol.MsgRunCancel, nil); err != nil {
		t.Fatalf("send run cancel: %v", err)
	}

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel frame blocked behind queued stdin")
	}
}

func TestPipeInputForwardingBackpressuresQueuedStdin(t *testing.T) {
	t.Helper()

	input := &pipeInputForwarding{done: make(chan error, 1)}
	input.cond = sync.NewCond(&sync.Mutex{})
	defer input.Stop()

	if _, err := input.Write(make([]byte, maxQueuedPipeInputBytes)); err != nil {
		t.Fatalf("fill input queue: %v", err)
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := input.Write([]byte("blocked"))
		writeDone <- err
	}()

	select {
	case err := <-writeDone:
		t.Fatalf("write finished before reader drained queue: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	buf := make([]byte, len("blocked"))
	if _, err := input.Read(buf); err != nil {
		t.Fatalf("drain input queue: %v", err)
	}

	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("write after drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write stayed blocked after reader drained queue")
	}
}

func TestHandleRunRequestDisconnectClearsContextActiveState(t *testing.T) {
	t.Helper()

	stateDir := t.TempDir()

	server, err := New(Options{
		StateDir: stateDir,
		Logger:   log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("new host: %v", err)
	}

	serverConn, clientConn := net.Pipe()

	done := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	request := protocol.RunRequest{
		ContextID: "ctx-recover",
		Session:   "session-recover",
		Command:   []string{"sh", "-c", "sleep 60"},
		WorkDir:   "",
		Env:       map[string]string{},
		Manifest:  nil,
	}

	go func() {
		err := server.handleRunRequest(ctx, protocol.NewConn(serverConn), request, nil)
		done <- err
	}()

	client := protocol.NewConn(clientConn)

	needBlobs, err := client.ReadHeader()
	if err != nil {
		t.Fatalf("read NeedBlobs: %v", err)
	}

	if needBlobs.Type != protocol.MsgNeedBlobs {
		t.Fatalf("need blobs request type %q", needBlobs.Type)
	}

	if err := client.WriteJSON(protocol.MsgSyncComplete, nil); err != nil {
		t.Fatalf("send sync complete: %v", err)
	}

	ws, err := client.ReadHeader()
	if err != nil {
		t.Fatalf("read workspace ready: %v", err)
	}

	if ws.Type != protocol.MsgWorkspaceReady {
		t.Fatalf("workspace ready type %q", ws.Type)
	}

	_ = client.WriteJSON(protocol.MsgStdinClose, nil)
	_ = clientConn.Close()

	cancel()

	select {
	case err := <-done:
		if err != nil && !protocol.IsDisconnectError(err) {
			t.Fatalf("run request finished with unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run request did not return after disconnect")
	}

	if server.contextIsActive(request.ContextID) {
		t.Fatalf("context lock still active after disconnect")
	}
}

func TestHandleRunRequestDisconnectBeforeSyncBackKeepsWorkspace(t *testing.T) {
	t.Helper()

	stateDir := t.TempDir()

	server, err := New(Options{
		StateDir: stateDir,
		Logger:   log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("new host: %v", err)
	}

	serverConn, clientConn := net.Pipe()

	done := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	request := protocol.RunRequest{
		ContextID: "ctx-sync-retry",
		Session:   "session-sync-retry",
		Command:   []string{"sh", "-c", "printf host > host.txt"},
		Env:       map[string]string{},
		Manifest:  nil,
	}

	go func() {
		done <- server.handleRunRequest(ctx, protocol.NewConn(serverConn), request, nil)
	}()

	client := protocol.NewConn(clientConn)

	needBlobs, err := client.ReadHeader()
	if err != nil {
		t.Fatalf("read NeedBlobs: %v", err)
	}
	if needBlobs.Type != protocol.MsgNeedBlobs {
		t.Fatalf("need blobs request type %q", needBlobs.Type)
	}
	if err := client.DiscardPayload(needBlobs); err != nil {
		t.Fatal(err)
	}
	if err := client.WriteJSON(protocol.MsgSyncComplete, nil); err != nil {
		t.Fatalf("send sync complete: %v", err)
	}

	ws, err := client.ReadHeader()
	if err != nil {
		t.Fatalf("read workspace ready: %v", err)
	}
	if ws.Type != protocol.MsgWorkspaceReady {
		t.Fatalf("workspace ready type %q", ws.Type)
	}
	if err := client.DiscardPayload(ws); err != nil {
		t.Fatal(err)
	}

	if err := client.WriteJSON(protocol.MsgStdinClose, nil); err != nil {
		t.Fatalf("send stdin close: %v", err)
	}

	for {
		head, err := client.ReadHeader()
		if err != nil {
			t.Fatalf("read run frame: %v", err)
		}
		if head.Type == protocol.MsgExecExit {
			if err := client.DiscardPayload(head); err != nil {
				t.Fatal(err)
			}

			break
		}
		if err := client.DiscardPayload(head); err != nil {
			t.Fatal(err)
		}
	}

	_ = clientConn.Close()

	select {
	case err := <-done:
		if err != nil && !protocol.IsDisconnectError(err) {
			t.Fatalf("run request finished with unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run request did not return after sync-back disconnect")
	}

	contextDir := filepath.Join(stateDir, contextDirName, request.ContextID)
	workspaceFile := filepath.Join(contextDir, contextWorkspaceDir, "host.txt")
	content, err := os.ReadFile(workspaceFile)
	if err != nil {
		t.Fatalf("read retained workspace file: %v", err)
	}
	if string(content) != "host" {
		t.Fatalf("workspace file=%q want host", string(content))
	}
	assertPathMissing(t, filepath.Join(contextDir, contextCleanFile))
}
