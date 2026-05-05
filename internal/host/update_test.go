package host

import (
	"context"
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func TestHandleHostUpdateRequestRunsFixedInstallTarget(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	defer func() { _ = clientConn.Close() }()

	client := protocol.NewConn(clientConn)

	var gotVersion string

	s := &Server{
		logger: log.New(io.Discard, "", 0),
		updateRunner: func(
			_ context.Context,
			_ *log.Logger,
			targetVersion string,
			_ string,
			_ io.Writer,
		) (updateResult, error) {
			gotVersion = targetVersion

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

	if gotVersion != "v9.8.7" {
		t.Fatalf("runner version=%s want v9.8.7", gotVersion)
	}

	wantTarget := "github.com/manuel-huez/rmtx/cmd/rmtx@v9.8.7"
	if resp.InstallTarget != wantTarget {
		t.Fatalf("install target=%s want %s", resp.InstallTarget, wantTarget)
	}

	if err := <-errCh; err != nil {
		t.Fatal(err)
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

	gotInstallerLog := false
	gotResponse := false

	for !gotInstallerLog || !gotResponse {
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

			if strings.Contains(string(payload), "downloaded module") {
				gotInstallerLog = true
			}
		case protocol.MsgHostUpdateResponse:
			resp, err := protocol.DecodeData[protocol.HostUpdateResponse](head)
			if err != nil {
				t.Fatal(err)
			}

			if !resp.Updated || !resp.Restarting {
				t.Fatalf("unexpected update response: %#v", resp)
			}

			gotResponse = true
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

	if err := <-errCh; err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(hostLogs.String(), "host update installed") {
		t.Fatalf("host logs missing update progress: %q", hostLogs.String())
	}
}

func TestHandleHostUpdateRequestReturnsRestartingWhenUpdateAlreadyPending(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	defer func() { _ = clientConn.Close() }()

	client := protocol.NewConn(clientConn)
	runnerCalled := false

	s := &Server{
		logger: log.New(io.Discard, "", 0),
		updateRunner: func(
			_ context.Context,
			_ *log.Logger,
			_ string,
			_ string,
			_ io.Writer,
		) (updateResult, error) {
			runnerCalled = true

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

	if runnerCalled {
		t.Fatal("runner should not be called while restart is already pending")
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

	gotResponse := false
	gotDone := false
	deadline := time.After(2 * time.Second)

	for !gotResponse || !gotDone {
		select {
		case head := <-headCh:
			if head.Type != protocol.MsgHostUpdateResponse {
				t.Fatalf("response type=%s want %s", head.Type, protocol.MsgHostUpdateResponse)
			}

			gotResponse = true
		case err := <-errCh:
			if err != nil {
				t.Fatal(err)
			}

			gotDone = true
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
