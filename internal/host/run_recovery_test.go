//go:build !windows

package host

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os/exec"
	"strings"
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
		if err != nil && !isDisconnectError(err) {
			t.Fatalf("run request finished with unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run request did not return after disconnect")
	}

	if server.contextIsActive(request.ContextID) {
		t.Fatalf("context lock still active after disconnect")
	}
}
