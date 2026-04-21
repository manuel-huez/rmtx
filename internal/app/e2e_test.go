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

	"github.com/manuel-huez/rmtx/internal/config"
	"github.com/manuel-huez/rmtx/internal/host"
)

func TestRunExecRequiresLocalConfig(t *testing.T) {
	project := t.TempDir()

	_, err := RunExec(context.Background(), project, ExecParams{
		AddressOverride: "127.0.0.1:33221",
		TokenOverride:   "secret-token",
		Command:         []string{"true"},
	})
	if err == nil {
		t.Fatal("expected missing config error")
	}

	if !strings.Contains(err.Error(), "local config file is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

//nolint:cyclop,gocognit,maintidx // integration setup/verification naturally has many branch checks.
func TestRunExecEndToEndSyncsBackChangesAndPersistsContexts(t *testing.T) {
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
  "context": {"name": "integration-context"},
  "mounts": [{"path": ".", "exclude": [".git/**", "cache/**"]}],
  "env": {"forward": ["FORWARD_ME"]},
  "discovery": {"enabled": false}
}`

	configPath := filepath.Join(project, ".rmtx.json")
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
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

	defer func() { _ = os.Unsetenv("FORWARD_ME") }()

	loaded, err := config.ResolveRequired(project, "")
	if err != nil {
		t.Fatal(err)
	}

	contextID := loaded.ContextID()

	var stdout1, stderr1 bytes.Buffer

	code, err := RunExec(ctx, project, ExecParams{
		AddressOverride: addr,
		TokenOverride:   "secret-token",
		Command: []string{
			"sh",
			"-c",
			`printf "%s\n" "$FORWARD_ME"; cat hello.txt; mkdir -p cache; echo persisted > cache/marker; echo changed > hello.txt`,
		},
		Stdout: &stdout1,
		Stderr: &stderr1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if code != 0 {
		t.Fatalf("unexpected exit code: %d (stderr=%s)", code, stderr1.String())
	}

	out := stdout1.String()
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

	if _, err := os.Stat(filepath.Join(project, "cache", "marker")); !os.IsNotExist(err) {
		t.Fatalf("expected excluded cache file to stay remote-only, got err=%v", err)
	}

	var stdout2, stderr2 bytes.Buffer

	code, err = RunExec(ctx, project, ExecParams{
		AddressOverride: addr,
		TokenOverride:   "secret-token",
		Command:         []string{"sh", "-c", `cat cache/marker`},
		Stdout:          &stdout2,
		Stderr:          &stderr2,
	})
	if err != nil {
		t.Fatal(err)
	}

	if code != 0 {
		t.Fatalf("unexpected second exit code: %d (stderr=%s)", code, stderr2.String())
	}

	if strings.TrimSpace(stdout2.String()) != "persisted" {
		t.Fatalf("expected remote cache file to persist across runs, got %q", stdout2.String())
	}

	contexts, err := RunListContexts(ctx, project, RemoteParams{
		AddressOverride: addr,
		TokenOverride:   "secret-token",
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(contexts) != 1 {
		t.Fatalf("expected 1 context, got %d: %#v", len(contexts), contexts)
	}

	if contexts[0].ID != contextID {
		t.Fatalf("unexpected context id: got %s want %s", contexts[0].ID, contextID)
	}

	if contexts[0].Name != "integration-context" {
		t.Fatalf("unexpected context name: %s", contexts[0].Name)
	}

	if !strings.Contains(contexts[0].Workspace, filepath.Join("contexts", contextID, "workspace")) {
		t.Fatalf("unexpected workspace path: %s", contexts[0].Workspace)
	}

	ping, err := RunPing(ctx, project, RemoteParams{
		AddressOverride: addr,
		TokenOverride:   "secret-token",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !ping.Online {
		t.Fatal("expected ping to report host online")
	}

	if ping.ContextCount != 1 {
		t.Fatalf("expected ping context count 1, got %d", ping.ContextCount)
	}

	deleteResult, err := RunDeleteContexts(ctx, project, ContextDeleteParams{
		AddressOverride: addr,
		TokenOverride:   "secret-token",
		Current:         true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(deleteResult.Deleted) != 1 || deleteResult.Deleted[0].ID != contextID {
		t.Fatalf("unexpected delete result: %#v", deleteResult)
	}

	contextDir := filepath.Join(stateDir, "contexts", contextID)
	if _, err := os.Stat(contextDir); !os.IsNotExist(err) {
		t.Fatalf("expected context dir to be deleted, got err=%v", err)
	}

	contexts, err = RunListContexts(ctx, project, RemoteParams{
		AddressOverride: addr,
		TokenOverride:   "secret-token",
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(contexts) != 0 {
		t.Fatalf("expected all contexts deleted, got %#v", contexts)
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
