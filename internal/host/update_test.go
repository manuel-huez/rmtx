//nolint:cyclop,gocognit,goconst // Update scenarios keep streamed events and lifecycle assertions together.
package host

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func TestPruneOldUpdateDirsRemovesStaleInstalls(t *testing.T) {
	stateDir := t.TempDir()

	oldDir := filepath.Join(stateDir, "updates", "v0.0.1")

	if err := os.MkdirAll(oldDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(oldDir, "rmtx"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	removed, err := pruneOldUpdateDirs(stateDir)
	if err != nil {
		t.Fatal(err)
	}

	if len(removed) != 1 || removed[0] != oldDir {
		t.Fatalf("removed=%#v want %s", removed, oldDir)
	}

	assertPathMissing(t, oldDir)
}

func TestPruneOldUpdateArtifactsKeepsPendingRestartInstall(t *testing.T) {
	stateDir := t.TempDir()
	oldDir := filepath.Join(stateDir, "updates", "v0.0.1")
	pendingDir := filepath.Join(stateDir, "updates", "v9.8.7")

	for _, dir := range []string{oldDir, pendingDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}

		if err := os.WriteFile(filepath.Join(dir, "rmtx"), []byte("bin"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	s := &Server{opts: Options{StateDir: stateDir}}
	if !s.beginRestart(
		filepath.Join(pendingDir, "rmtx"),
		"v9.8.7",
		"github.com/manuel-huez/rmtx/cmd/rmtx@v9.8.7",
	) {
		t.Fatal("expected restart setup to succeed")
	}

	deleted, _, err := s.pruneOldUpdateArtifacts()
	if err != nil {
		t.Fatal(err)
	}

	if len(deleted) != 1 || deleted[0].Path != oldDir {
		t.Fatalf("deleted=%#v want only %s", deleted, oldDir)
	}

	assertPathMissing(t, oldDir)
	assertPathExists(t, pendingDir)
}

func TestHandleHostUpdateRequestRunsFixedInstallTarget(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	defer func() { _ = clientConn.Close() }()

	client := protocol.NewConn(clientConn)

	gotVersionCh := make(chan string, 1)

	s := &Server{
		logger: log.New(io.Discard, "", 0),
		updateRunner: func(
			_ context.Context,
			_ *log.Logger,
			targetVersion string,
			_ string,
			_ io.Writer,
		) (updateResult, error) {
			gotVersionCh <- targetVersion

			return updateResult{
				InstallTarget: updateInstallTarget(targetVersion),
				Executable:    "/tmp/rmtx-updated",
			}, nil
		},
	}

	errCh := make(chan error, 2)

	go func() {
		defer func() { _ = serverConn.Close() }()

		errCh <- s.handleHostUpdateRequest(
			context.Background(),
			protocol.NewConn(serverConn),
			protocol.HostUpdateRequest{Version: "v9.8.7"},
			nil,
		)
	}()

	head, err := client.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}

	if head.Type != protocol.MsgHostUpdateResponse {
		t.Fatalf("response type=%s want %s", head.Type, protocol.MsgHostUpdateResponse)
	}

	resp, err := protocol.DecodeData[protocol.HostUpdateResponse](head)
	if err != nil {
		t.Fatal(err)
	}

	if !resp.Updated || !resp.Restarting {
		t.Fatalf("unexpected update response: %#v", resp)
	}

	if resp.Version != "v9.8.7" {
		t.Fatalf("restart version=%s want v9.8.7", resp.Version)
	}

	wantTarget := "github.com/manuel-huez/rmtx/cmd/rmtx@v9.8.7"
	if resp.InstallTarget != wantTarget {
		t.Fatalf("install target=%s want %s", resp.InstallTarget, wantTarget)
	}

	if err := <-errCh; err != nil {
		t.Fatal(err)
	}

	gotVersion := <-gotVersionCh
	if gotVersion != "v9.8.7" {
		t.Fatalf("runner version=%s want v9.8.7", gotVersion)
	}

	if !s.restartWasRequested() {
		t.Fatal("restart should be requested after successful update")
	}
}

func TestStreamsHostLogsIncludesHostUpdateRequests(t *testing.T) {
	if !streamsHostLogs(protocol.MsgHostUpdateRequest) {
		t.Fatal("host update requests should stream host logs")
	}
}

func TestHandleHostUpdateRequestStreamsLogs(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	defer func() { _ = clientConn.Close() }()

	client := protocol.NewConn(clientConn)

	var hostLogs strings.Builder

	serverProtocol := protocol.NewConn(serverConn)

	requestLogs := newHostLogSubscription(serverProtocol)
	defer requestLogs.Close()

	s := &Server{
		logger: log.New(&hostLogs, "", 0),
		updateRunner: func(
			_ context.Context,
			_ *log.Logger,
			targetVersion string,
			_ string,
			live io.Writer,
		) (updateResult, error) {
			if _, err := io.WriteString(live, "downloaded module\n"); err != nil {
				return updateResult{}, err
			}

			return updateResult{
				InstallTarget: updateInstallTarget(targetVersion),
				Executable:    "/tmp/rmtx-updated",
			}, nil
		},
	}

	errCh := make(chan error, 1)

	go func() {
		defer func() { _ = serverConn.Close() }()

		errCh <- s.handleHostUpdateRequest(
			context.Background(),
			serverProtocol,
			protocol.HostUpdateRequest{Version: "v9.8.7"},
			requestLogs,
		)
	}()

	if err := clientConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}

	var updateLogs strings.Builder

	for {
		head, err := client.ReadHeader()
		if err != nil {
			t.Fatal(err)
		}

		switch head.Type {
		case protocol.MsgExecOutput:
			if _, err := protocol.DecodeData[protocol.OutputInfo](head); err != nil {
				t.Fatal(err)
			}

			payload, err := io.ReadAll(client.PayloadReader(head))
			if err != nil {
				t.Fatal(err)
			}

			updateLogs.Write(payload)
		case protocol.MsgHostUpdateResponse:
			resp, err := protocol.DecodeData[protocol.HostUpdateResponse](head)
			if err != nil {
				t.Fatal(err)
			}

			if !resp.Updated || !resp.Restarting {
				t.Fatalf("unexpected update response: %#v", resp)
			}

			if !strings.Contains(updateLogs.String(), "downloaded module") {
				t.Fatalf("update logs missing installer output: %q", updateLogs.String())
			}

			if err := <-errCh; err != nil {
				t.Fatal(err)
			}

			if !strings.Contains(hostLogs.String(), "host update installed") {
				t.Fatalf("host logs missing update progress: %q", hostLogs.String())
			}

			return
		case protocol.MsgError:
			errMsg, err := protocol.DecodeData[protocol.ErrorMessage](head)
			if err != nil {
				t.Fatal(err)
			}

			t.Fatalf("server error: %s", errMsg.Message)
		default:
			t.Fatalf("unexpected frame type: %s", head.Type)
		}
	}
}

func TestHandleHostUpdateRequestReturnsRestartingWhenUpdateAlreadyPending(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	defer func() { _ = clientConn.Close() }()

	client := protocol.NewConn(clientConn)
	runnerCalled := make(chan struct{}, 1)

	s := &Server{
		logger: log.New(io.Discard, "", 0),
		updateRunner: func(
			_ context.Context,
			_ *log.Logger,
			_ string,
			_ string,
			_ io.Writer,
		) (updateResult, error) {
			runnerCalled <- struct{}{}

			return updateResult{}, nil
		},
	}

	if !s.beginRestart(
		"/tmp/rmtx-updated",
		"v9.8.6",
		"github.com/manuel-huez/rmtx/cmd/rmtx@v9.8.6",
	) {
		t.Fatal("expected restart setup to succeed")
	}

	errCh := make(chan error, 2)

	go func() {
		defer func() { _ = serverConn.Close() }()

		errCh <- s.handleHostUpdateRequest(
			context.Background(),
			protocol.NewConn(serverConn),
			protocol.HostUpdateRequest{Version: "v9.8.7"},
			nil,
		)
	}()

	head, err := client.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}

	if head.Type != protocol.MsgHostUpdateResponse {
		t.Fatalf("response type=%s want %s", head.Type, protocol.MsgHostUpdateResponse)
	}

	resp, err := protocol.DecodeData[protocol.HostUpdateResponse](head)
	if err != nil {
		t.Fatal(err)
	}

	if !resp.Updated || !resp.Restarting {
		t.Fatalf("unexpected update response: %#v", resp)
	}

	if resp.Version != "v9.8.6" {
		t.Fatalf("restart version=%s want v9.8.6", resp.Version)
	}

	wantTarget := "github.com/manuel-huez/rmtx/cmd/rmtx@v9.8.6"
	if resp.InstallTarget != wantTarget {
		t.Fatalf("install target=%s want %s", resp.InstallTarget, wantTarget)
	}

	if err := <-errCh; err != nil {
		t.Fatal(err)
	}

	select {
	case <-runnerCalled:
		t.Fatal("runner should not be called while restart is already pending")
	default:
	}
}

func TestHandleHostUpdateRequestWaitsForActiveRunBeforeRestart(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	defer func() { _ = clientConn.Close() }()

	client := protocol.NewConn(clientConn)

	s := &Server{
		logger: log.New(io.Discard, "", 0),
		updateRunner: func(
			_ context.Context,
			_ *log.Logger,
			targetVersion string,
			_ string,
			_ io.Writer,
		) (updateResult, error) {
			return updateResult{
				InstallTarget: updateInstallTarget(targetVersion),
				Executable:    "/tmp/rmtx-updated",
			}, nil
		},
	}

	releaseRun, err := s.acquireRun()
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 2)
	headCh := make(chan protocol.Header, 1)

	go func() {
		defer func() { _ = serverConn.Close() }()

		errCh <- s.handleHostUpdateRequest(
			context.Background(),
			protocol.NewConn(serverConn),
			protocol.HostUpdateRequest{Version: "v9.8.7"},
			nil,
		)
	}()

	go func() {
		head, err := client.ReadHeader()
		if err != nil {
			errCh <- err
			return
		}

		headCh <- head
	}()

	select {
	case head := <-headCh:
		t.Fatalf("update responded before active run finished: %s", head.Type)
	case err := <-errCh:
		t.Fatalf("update failed before active run finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	releaseRun()

	deadline := time.After(2 * time.Second)
	waitHeadCh := headCh
	waitErrCh := errCh

	for range 2 {
		select {
		case head := <-waitHeadCh:
			if head.Type != protocol.MsgHostUpdateResponse {
				t.Fatalf("response type=%s want %s", head.Type, protocol.MsgHostUpdateResponse)
			}

			waitHeadCh = nil
		case err := <-waitErrCh:
			if err != nil {
				t.Fatal(err)
			}

			waitErrCh = nil
		case <-deadline:
			t.Fatal("timed out waiting for update response")
		}
	}

	if !s.restartWasRequested() {
		t.Fatal("restart should be requested after active run finishes")
	}
}

func TestHandleHostUpdateRequestRejectsUnsafeVersion(t *testing.T) {
	s := &Server{logger: log.New(io.Discard, "", 0)}

	err := s.handleHostUpdateRequest(
		context.Background(),
		nil,
		protocol.HostUpdateRequest{Version: "latest;rm -rf /"},
		nil,
	)
	if err == nil {
		t.Fatal("expected invalid version error")
	}

	if !strings.Contains(err.Error(), "invalid host update version") {
		t.Fatalf("unexpected error: %v", err)
	}
}
