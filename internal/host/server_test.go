package host

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"syscall"
	"testing"

	"github.com/manuel-huez/rmtx/internal/syncfs"
)

func TestIsDisconnectErrorRecognizesTypedNetworkCloseErrors(t *testing.T) {
	for _, err := range []error{
		syscall.ECONNRESET,
		syscall.ECONNABORTED,
		syscall.EPIPE,
		windowsConnectionReset,
	} {
		if !isDisconnectError(err) {
			t.Fatalf("expected disconnect error for %v", err)
		}
	}

	if isDisconnectError(errors.New("apply non-file entries failed")) {
		t.Fatal("non-disconnect error should not match")
	}
}

func TestHandleConnIgnoresDisconnectBeforeRequestHeader(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	var logs bytes.Buffer
	s := &Server{logger: log.New(&logs, "", 0)}

	if err := clientConn.Close(); err != nil {
		t.Fatal(err)
	}

	s.handleConn(context.Background(), serverConn)

	if logs.Len() != 0 {
		t.Fatalf("expected no request failure log for disconnect, got %q", logs.String())
	}
}

func TestDiffWorkspaceChangesCanIgnoreModeOnlyChanges(t *testing.T) {
	before := []syncfs.Entry{{
		Path: "file.txt",
		Kind: syncfs.KindFile,
		Hash: "hash",
		Size: 4,
		Mode: 0o644,
	}}
	after := []syncfs.Entry{{
		Path: "file.txt",
		Kind: syncfs.KindFile,
		Hash: "hash",
		Size: 4,
		Mode: 0o666,
	}}

	changed, deleted := diffWorkspaceChanges(before, after, nil, true)
	if len(changed) != 0 || len(deleted) != 0 {
		t.Fatalf("mode-only change should be ignored: changed=%#v deleted=%#v", changed, deleted)
	}

	changed, deleted = diffWorkspaceChanges(before, after, nil, false)
	if len(changed) != 1 || len(deleted) != 0 {
		t.Fatalf("mode-only change should be reported: changed=%#v deleted=%#v", changed, deleted)
	}
}
