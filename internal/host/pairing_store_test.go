package host

import (
	"bytes"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/security"
)

func TestConsumePairCodeAllowsSingleWinner(t *testing.T) {
	stateDir := t.TempDir()

	record, err := CreatePairCode(stateDir, 0)
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)
	start := make(chan struct{})

	var wg sync.WaitGroup

	for range 2 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start

			results <- ConsumePairCode(stateDir, record.Code)
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	successes := 0
	failures := 0

	for err := range results {
		if err == nil {
			successes++
			continue
		}

		failures++
	}

	if successes != 1 || failures != 1 {
		t.Fatalf(
			"expected exactly one successful consume, got %d success and %d failure",
			successes,
			failures,
		)
	}
}

func TestTrustClientPreservesConcurrentPairings(t *testing.T) {
	server := &Server{opts: Options{StateDir: t.TempDir()}}

	start := make(chan struct{})

	var wg sync.WaitGroup
	for _, tc := range []struct {
		fingerprint string
		label       string
	}{
		{fingerprint: "sha256:111", label: "client-a"},
		{fingerprint: "sha256:222", label: "client-b"},
	} {
		wg.Add(1)

		go func(fingerprint, label string) {
			defer wg.Done()

			<-start

			if err := server.trustClient(fingerprint, "", label); err != nil {
				t.Errorf("trustClient(%s): %v", fingerprint, err)
			}
		}(tc.fingerprint, tc.label)
	}

	close(start)
	wg.Wait()

	store, err := server.loadTrustStore()
	if err != nil {
		t.Fatal(err)
	}

	if len(store.Clients) != 2 {
		t.Fatalf("expected 2 trusted clients, got %#v", store.Clients)
	}
}

func TestTrustClientReplacesPreviousFingerprint(t *testing.T) {
	server := &Server{opts: Options{StateDir: t.TempDir()}}

	if err := server.trustClient("sha256:old", "", "client-a"); err != nil {
		t.Fatal(err)
	}

	if err := server.trustClient("sha256:new", "sha256:old", "client-a"); err != nil {
		t.Fatal(err)
	}

	store, err := server.loadTrustStore()
	if err != nil {
		t.Fatal(err)
	}

	if len(store.Clients) != 1 {
		t.Fatalf("expected one trusted client after rotation, got %#v", store.Clients)
	}

	if store.Clients[0].Fingerprint != "sha256:new" {
		t.Fatalf("unexpected trusted fingerprint: got %s", store.Clients[0].Fingerprint)
	}
}

func TestHandlePairRequestKeepsCodeOnValidationFailure(t *testing.T) {
	stateDir := t.TempDir()
	server := &Server{opts: Options{StateDir: stateDir}}

	record, err := CreatePairCode(stateDir, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = server.handlePairRequest(nil, protocol.PairRequest{Code: record.Code})
	if err == nil {
		t.Fatal("expected pair request to fail without csr")
	}

	if err := ConsumePairCode(stateDir, record.Code); err != nil {
		t.Fatalf("expected code to remain usable after validation failure: %v", err)
	}
}

func TestHandlePairRequestKeepsCodeOnResponseWriteFailure(t *testing.T) {
	stateDir := t.TempDir()

	server, err := New(Options{StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}

	record, err := CreatePairCode(stateDir, 0)
	if err != nil {
		t.Fatal(err)
	}

	_, _, csrPEM, err := security.GenerateClientIdentity("retry-client")
	if err != nil {
		t.Fatal(err)
	}

	serverConn, clientConn := net.Pipe()
	_ = clientConn.Close()

	defer func() { _ = serverConn.Close() }()

	err = server.handlePairRequest(protocol.NewConn(serverConn), protocol.PairRequest{
		Code:        record.Code,
		ClientLabel: "retry-client",
		CSRPEM:      string(csrPEM),
	})
	if err == nil {
		t.Fatal("expected pair response write to fail")
	}

	if err := ConsumePairCode(stateDir, record.Code); err != nil {
		t.Fatalf("expected code to remain usable after response failure: %v", err)
	}
}

func TestCreatePairCodeStoresCodesPrivately(t *testing.T) {
	if hostIsWindows() {
		t.Skip("permission bits are not portable on windows")
	}

	stateDir := t.TempDir()
	if _, err := CreatePairCode(stateDir, 0); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(filepath.Join(stateDir, "pair-codes.json"))
	if err != nil {
		t.Fatal(err)
	}

	if got := info.Mode().Perm(); got != pairCodeFileMode {
		t.Fatalf("unexpected permissions: got %#o want %#o", got, pairCodeFileMode)
	}
}

func TestPairCodeStoreLockBlocksOtherProcesses(t *testing.T) {
	stateDir := t.TempDir()
	signalPath := filepath.Join(stateDir, "child-acquired")

	release, err := acquirePairCodeStoreLock(stateDir)
	if err != nil {
		t.Fatal(err)
	}

	released := false

	defer func() {
		if !released {
			release()
		}
	}()

	cmd := exec.Command(os.Args[0], "-test.run=TestPairCodeStoreLockHelperProcess")

	var output bytes.Buffer

	cmd.Stdout = &output
	cmd.Stderr = &output

	cmd.Env = append(
		os.Environ(),
		"RMTX_PAIRCODE_HELPER=1",
		"RMTX_PAIRCODE_STATE_DIR="+stateDir,
		"RMTX_PAIRCODE_SIGNAL="+signalPath,
	)

	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(signalPath); err == nil {
			t.Fatalf("child acquired lock while parent held it: %s", output.String())
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat signal file: %v", err)
		}

		time.Sleep(10 * time.Millisecond)
	}

	release()

	released = true

	if err := cmd.Wait(); err != nil {
		t.Fatalf("helper process failed: %v (%s)", err, output.String())
	}

	if _, err := os.Stat(signalPath); err != nil {
		t.Fatalf("expected child to acquire lock after release: %v", err)
	}
}

func TestPairCodeStoreLockHelperProcess(t *testing.T) {
	if os.Getenv("RMTX_PAIRCODE_HELPER") != "1" {
		return
	}

	release, err := acquirePairCodeStoreLock(os.Getenv("RMTX_PAIRCODE_STATE_DIR"))
	if err != nil {
		t.Fatalf("acquire pair code lock: %v", err)
	}
	defer release()

	if err := os.WriteFile(
		os.Getenv("RMTX_PAIRCODE_SIGNAL"),
		[]byte("acquired\n"),
		0o644,
	); err != nil {
		t.Fatalf("write signal: %v", err)
	}
}
