package host

import (
	"errors"
	"syscall"
	"testing"

	"github.com/manuel-huez/rmtx/internal/protocol"
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

func TestIsExpectedSessionDisconnectOnlyMatchesControlRequests(t *testing.T) {
	if !isExpectedSessionDisconnect(protocol.MsgListContextsRequest, syscall.ECONNRESET) {
		t.Fatal("context list disconnect should be expected")
	}

	if isExpectedSessionDisconnect(protocol.MsgRunRequest, syscall.ECONNRESET) {
		t.Fatal("run disconnect should still be reported")
	}

	if isExpectedSessionDisconnect(protocol.MsgListContextsRequest, errors.New("handler failed")) {
		t.Fatal("non-disconnect control error should still be reported")
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

	changed, deleted := diffWorkspaceChanges(before, after, true)
	if len(changed) != 0 || len(deleted) != 0 {
		t.Fatalf("mode-only change should be ignored: changed=%#v deleted=%#v", changed, deleted)
	}

	changed, deleted = diffWorkspaceChanges(before, after, false)
	if len(changed) != 1 || len(deleted) != 0 {
		t.Fatalf("mode-only change should be reported: changed=%#v deleted=%#v", changed, deleted)
	}
}
