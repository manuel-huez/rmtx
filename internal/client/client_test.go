package client

import (
	"context"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/clientstate"
	"github.com/manuel-huez/rmtx/internal/host"
)

func TestRequestPairCodeFallsBackToReverseConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
		StateDir:         t.TempDir(),
		DiscoveryService: "test-rmtx",
		Logger:           log.New(io.Discard, "", 0),
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
