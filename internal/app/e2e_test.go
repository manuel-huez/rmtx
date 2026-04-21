package app

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/host"
)

func TestRunExecEndToEndSyncsBackChanges(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-based integration test")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stateDir := t.TempDir()

	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
		Token:            "secret-token",
		StateDir:         stateDir,
		DisableDiscovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)

	go func() { errCh <- server.Serve(ctx) }()

	addr := waitForAddr(t, server)

	project := t.TempDir()

	configContent := `{
  "version": 1,
  "mounts": [{"path": ".", "exclude": [".git/**"]}],
  "env": {"forward": ["FORWARD_ME"]},
  "discovery": {"enabled": false}
}`
	if err := os.WriteFile(
		filepath.Join(project, ".remotex.json"),
		[]byte(configContent),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		filepath.Join(project, "hello.txt"),
		[]byte("initial\n"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	if err := os.Setenv("FORWARD_ME", "visible-value"); err != nil {
		t.Fatal(err)
	}

	defer os.Unsetenv("FORWARD_ME")

	var stdout, stderr bytes.Buffer

	code, err := RunExec(ctx, project, ExecParams{
		AddressOverride: addr,
		TokenOverride:   "secret-token",
		Command: []string{
			"sh",
			"-c",
			`printf "%s\n" "$FORWARD_ME"; cat hello.txt; echo changed > hello.txt`,
		},
		Stdout:       &stdout,
		Stderr:       &stderr,
		ForwardStdin: false,
	})
	if err != nil {
		t.Fatal(err)
	}

	if code != 0 {
		t.Fatalf("unexpected exit code: %d (stderr=%s)", code, stderr.String())
	}

	out := stdout.String()
	if !strings.Contains(out, "visible-value") {
		t.Fatalf("expected forwarded env in output, got %q", out)
	}

	if !strings.Contains(out, "initial") {
		t.Fatalf("expected original file content in output, got %q", out)
	}

	updated, err := os.ReadFile(filepath.Join(project, "hello.txt"))
	if err != nil {
		t.Fatal(err)
	}

	if strings.TrimSpace(string(updated)) != "changed" {
		t.Fatalf("expected synced-back file change, got %q", string(updated))
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

func waitForAddr(t *testing.T, server *host.Server) string {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := server.Addr(); addr != "" {
			return addr
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("server did not expose a listening address in time")

	return ""
}
