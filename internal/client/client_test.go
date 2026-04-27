package client

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/clientstate"
	"github.com/manuel-huez/rmtx/internal/host"
)

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
