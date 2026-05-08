package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/client"
	"github.com/manuel-huez/rmtx/internal/clientstate"
	"github.com/manuel-huez/rmtx/internal/config"
	"github.com/manuel-huez/rmtx/internal/discovery"
	"github.com/manuel-huez/rmtx/internal/host"
)

func TestRunExecRequiresLocalConfig(t *testing.T) {
	project := t.TempDir()

	_, err := RunExec(context.Background(), project, ExecParams{
		AddressOverride: "127.0.0.1:33221",
		Command:         []string{"true"},
	})
	if err == nil {
		t.Fatal("expected missing config error")
	}

	if !strings.Contains(err.Error(), "local config file is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildMountSpecsUsesGitignoreWhenEnabled(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(root, ".gitignore"),
		[]byte("node_modules/\n*.log\n# comment\n!important.log\n"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	mounts, err := buildMountSpecs(root, config.Config{
		Mounts:          []config.Mount{{Path: ".", Exclude: []string{"tmp/**"}}},
		Ignore:          []string{"dist/**"},
		IgnoreGitignore: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(mounts) != 1 {
		t.Fatalf("unexpected mounts: %#v", mounts)
	}

	patterns := map[string]bool{}
	for _, pattern := range mounts[0].Exclude {
		patterns[pattern] = true
	}

	for _, want := range []string{"dist/**", "**/node_modules/**", "**/*.log", "tmp/**"} {
		if !patterns[want] {
			t.Fatalf("missing pattern %q in %#v", want, mounts[0].Exclude)
		}
	}

	if patterns["**/important.log"] {
		t.Fatalf("negated gitignore pattern should not become exclude: %#v", mounts[0].Exclude)
	}
}

//nolint:cyclop,gocognit,maintidx // integration setup/verification naturally has many branch checks.
func TestRunExecEndToEndSyncsBackChangesAndCleansWorkspaces(t *testing.T) {
	if testIsWindows() {
		t.Skip("shell-based integration test")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	home := t.TempDir()
	t.Setenv("HOME", home)

	stateDir := t.TempDir()

	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
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
  "tls": {"host_fingerprint": "` + server.Fingerprint() + `"},
  "mounts": [{"path": ".", "exclude": [".git/**"]}],
  "ignore": ["cache/**"],
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

	pairCode, err := RunHostPairCode(HostPairCodeParams{StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := RunPair(ctx, project, PairParams{
		AddressOverride: addr,
		ConfigPath:      configPath,
		Code:            pairCode.Code,
		ClientLabel:     "integration-client",
	}); err != nil {
		t.Fatal(err)
	}

	var stdout1, stderr1 bytes.Buffer

	code, err := RunExec(ctx, project, ExecParams{
		AddressOverride: addr,
		Command: []string{
			"sh",
			"-c",
			`printf "%s\n" "$FORWARD_ME"; cat hello.txt; mkdir -p cache; echo persisted > cache/marker; cp -p hello.txt .hello-stamp; printf "changed\n" > hello.txt; touch -r .hello-stamp hello.txt; rm .hello-stamp`,
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
		Command:         []string{"sh", "-c", `test ! -e cache/marker && cat hello.txt`},
		Stdout:          &stdout2,
		Stderr:          &stderr2,
	})
	if err != nil {
		t.Fatal(err)
	}

	if code != 0 {
		t.Fatalf("unexpected second exit code: %d (stderr=%s)", code, stderr2.String())
	}

	if strings.TrimSpace(stdout2.String()) != "changed" {
		t.Fatalf("expected workspace to rehydrate without ignored cache file, got %q", stdout2.String())
	}

	var stdout3, stderr3 bytes.Buffer

	code, err = RunExec(ctx, project, ExecParams{
		AddressOverride: addr,
		Command:         []string{"sh", "-c", `echo failed > fail.txt; exit 7`},
		Stdout:          &stdout3,
		Stderr:          &stderr3,
	})
	if err != nil {
		t.Fatal(err)
	}

	if code != 7 {
		t.Fatalf("unexpected failing exit code: %d (stderr=%s)", code, stderr3.String())
	}
	if err := os.Remove(filepath.Join(project, "fail.txt")); err != nil {
		t.Fatal(err)
	}

	var stdout4, stderr4 bytes.Buffer

	code, err = RunExec(ctx, project, ExecParams{
		AddressOverride: addr,
		Command:         []string{"sh", "-c", `test ! -e fail.txt && cat hello.txt`},
		Stdout:          &stdout4,
		Stderr:          &stderr4,
	})
	if err != nil {
		t.Fatal(err)
	}

	if code != 0 {
		t.Fatalf("unexpected post-failure exit code: %d (stderr=%s)", code, stderr4.String())
	}

	if strings.TrimSpace(stdout4.String()) != "changed" {
		t.Fatalf("expected failed-run workspace to be cleaned, got %q", stdout4.String())
	}

	contexts, err := RunListContexts(ctx, project, RemoteParams{
		AddressOverride: addr,
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

	stats, err := RunHostStats(ctx, project, RemoteParams{
		AddressOverride: addr,
	})
	if err != nil {
		t.Fatal(err)
	}

	if stats.CPU.LogicalCores < 1 {
		t.Fatalf("expected logical cores, got %#v", stats.CPU)
	}

	if len(stats.CPU.PerCoreUsedPercent) != stats.CPU.LogicalCores {
		t.Fatalf("expected per-core stats for each logical core, got %#v", stats.CPU)
	}

	if stats.Memory.TotalBytes == 0 || stats.Memory.AvailableBytes == 0 {
		t.Fatalf("expected memory stats, got %#v", stats.Memory)
	}

	if stats.ContextCount != 1 {
		t.Fatalf("expected stats context count 1, got %d", stats.ContextCount)
	}

	if stats.ContextID != contextID {
		t.Fatalf("expected stats context id %s, got %s", contextID, stats.ContextID)
	}

	if stats.ContextDiskBytes <= 0 {
		t.Fatalf("expected context disk usage, got %d", stats.ContextDiskBytes)
	}

	deleteResult, err := RunDeleteContexts(ctx, project, ContextDeleteParams{
		AddressOverride: addr,
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

func TestRunExecInterruptSyncsBackHostChanges(t *testing.T) {
	if testIsWindows() {
		t.Skip("shell-based integration test")
	}

	hostCtx, stopHost := context.WithCancel(context.Background())
	defer stopHost()

	home := t.TempDir()
	t.Setenv("HOME", home)

	stateDir := t.TempDir()

	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
		StateDir:         stateDir,
		DisableDiscovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(hostCtx) }()

	addr := waitForAddr(t, server)
	project := t.TempDir()
	configPath := filepath.Join(project, ".rmtx.json")

	writeTestFile(t, configPath, `{
  "version": 1,
  "context": {"name": "interrupt-sync"},
  "host": "`+addr+`",
  "tls": {"host_fingerprint": "`+server.Fingerprint()+`"},
  "mounts": [{"path": "."}],
  "discovery": {"enabled": false}
}`)

	pairCode, err := RunHostPairCode(HostPairCodeParams{StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := RunPair(hostCtx, project, PairParams{
		ConfigPath:  configPath,
		Code:        pairCode.Code,
		ClientLabel: "interrupt-client",
	}); err != nil {
		t.Fatal(err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	stdin := newCloseBlockReader()
	stdoutReader, stdoutWriter := io.Pipe()
	runDone := make(chan struct {
		code int
		err  error
	}, 1)

	go func() {
		code, err := RunExec(runCtx, project, ExecParams{
			ConfigPath: configPath,
			Command: []string{
				"sh",
				"-c",
				`printf "host edit\n" > interrupted.txt; printf "ready\n"; sleep 60`,
			},
			Stdout:       stdoutWriter,
			Stderr:       io.Discard,
			Stdin:        stdin,
			ForwardStdin: true,
		})
		_ = stdoutWriter.Close()
		runDone <- struct {
			code int
			err  error
		}{code, err}
	}()

	ready := make(chan error, 1)
	go func() {
		buf := make([]byte, len("ready\n"))
		_, err := io.ReadFull(stdoutReader, buf)
		if err == nil && string(buf) != "ready\n" {
			err = errors.New("unexpected stdout before interrupt")
		}
		ready <- err
	}()

	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remote command did not become ready")
	}

	cancelRun()

	select {
	case result := <-runDone:
		if result.err != nil {
			t.Fatalf("interrupted run returned error before sync-back: code=%d err=%v", result.code, result.err)
		}
		if result.code == 0 {
			t.Fatal("interrupted run should return non-zero exit code")
		}
	case <-time.After(10 * time.Second):
		stdin.Close()
		t.Fatal("interrupted run did not finish")
	}
	stdin.Close()

	got, err := os.ReadFile(filepath.Join(project, "interrupted.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "host edit\n" {
		t.Fatalf("interrupted.txt=%q want host edit", string(got))
	}

	var rerunStdout, rerunStderr bytes.Buffer
	code, err := RunExec(context.Background(), project, ExecParams{
		ConfigPath: configPath,
		Command:    []string{"cat", "interrupted.txt"},
		Stdout:     &rerunStdout,
		Stderr:     &rerunStderr,
	})
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("rerun exit code=%d stderr=%s", code, rerunStderr.String())
	}
	if rerunStdout.String() != "host edit\n" {
		t.Fatalf("rerun stdout=%q want host edit", rerunStdout.String())
	}
	if !strings.Contains(rerunStderr.String(), "host already has all file blobs") {
		t.Fatalf("rerun reuploaded interrupted file: stderr=%s", rerunStderr.String())
	}

	stopHost()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server exited with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}
}

type closeBlockReader struct {
	once sync.Once
	done chan struct{}
}

func newCloseBlockReader() *closeBlockReader {
	return &closeBlockReader{done: make(chan struct{})}
}

func (r *closeBlockReader) Read([]byte) (int, error) {
	<-r.done

	return 0, io.EOF
}

func (r *closeBlockReader) Close() {
	r.once.Do(func() { close(r.done) })
}

func TestRunExecEndToEndNoopRunDoesNotDownloadFiles(t *testing.T) {
	if testIsWindows() {
		t.Skip("shell-based integration test")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	home := t.TempDir()
	t.Setenv("HOME", home)

	stateDir := t.TempDir()

	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
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
	configPath := filepath.Join(project, ".rmtx.json")

	configContent := `{
  "version": 1,
  "tls": {"host_fingerprint": "` + server.Fingerprint() + `"},
  "mounts": [{"path": "."}],
  "discovery": {"enabled": false}
}`
	writeTestFile(t, configPath, configContent)
	writeTestFile(t, filepath.Join(project, "hello.txt"), "stable\n")

	pairCode, err := RunHostPairCode(HostPairCodeParams{StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := RunPair(ctx, project, PairParams{
		AddressOverride: addr,
		ConfigPath:      configPath,
		Code:            pairCode.Code,
		ClientLabel:     "noop-client",
	}); err != nil {
		t.Fatal(err)
	}

	for run := range 2 {
		var stdout, stderr bytes.Buffer

		code, err := RunExec(ctx, project, ExecParams{
			AddressOverride: addr,
			Command:         []string{"sh", "-c", "echo test"},
			Stdout:          &stdout,
			Stderr:          &stderr,
		})
		if err != nil {
			t.Fatalf("run %d failed: %v", run+1, err)
		}

		if code != 0 {
			t.Fatalf("run %d exit code: %d stderr=%s", run+1, code, stderr.String())
		}

		if strings.TrimSpace(stdout.String()) != "test" {
			t.Fatalf("run %d unexpected stdout: %q", run+1, stdout.String())
		}

		noChanges := "applying host changes locally: changed=0 deleted=0"
		if run == 1 && !strings.Contains(stderr.String(), noChanges) {
			t.Fatalf("second no-op run downloaded changes: stderr=%s", stderr.String())
		}
	}

	cancel()
	waitForServerShutdown(t, "noop-host", errCh)
}

func TestRunExecEndToEndReportsCommandStartFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	home := t.TempDir()
	t.Setenv("HOME", home)

	stateDir := t.TempDir()

	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
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
	configPath := filepath.Join(project, ".rmtx.json")

	configContent := `{
  "version": 1,
  "tls": {"host_fingerprint": "` + server.Fingerprint() + `"},
  "mounts": [{"path": "."}],
  "discovery": {"enabled": false}
}`
	writeTestFile(t, configPath, configContent)

	pairCode, err := RunHostPairCode(HostPairCodeParams{StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := RunPair(ctx, project, PairParams{
		AddressOverride: addr,
		ConfigPath:      configPath,
		Code:            pairCode.Code,
		ClientLabel:     "start-failure-client",
	}); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer

	missingCommand := "rmtx-command-that-should-not-exist"

	code, err := RunExec(ctx, project, ExecParams{
		AddressOverride: addr,
		Command:         []string{missingCommand},
		Stdout:          &stdout,
		Stderr:          &stderr,
	})
	if err != nil {
		t.Fatal(err)
	}

	if code != 127 {
		t.Fatalf("unexpected exit code: got %d want 127 stderr=%s", code, stderr.String())
	}

	if stdout.String() != "" {
		t.Fatalf("expected empty stdout, got %q", stdout.String())
	}

	stderrOutput := stderr.String()
	if !strings.Contains(stderrOutput, "start command:") {
		t.Fatalf("expected start error in stderr, got %q", stderrOutput)
	}

	if !strings.Contains(stderrOutput, missingCommand) {
		t.Fatalf("expected missing command name in stderr, got %q", stderrOutput)
	}

	cancel()
	waitForServerShutdown(t, "start-failure-host", errCh)
}

//nolint:cyclop // End-to-end sync-back coverage has several verification branches.
func TestRunExecEndToEndRespectsContextSyncBack(t *testing.T) {
	if testIsWindows() {
		t.Skip("shell-based integration test")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	home := t.TempDir()
	t.Setenv("HOME", home)

	stateDir := t.TempDir()

	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
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
	configPath := filepath.Join(project, ".rmtx.json")

	configContent := `{
  "version": 1,
  "context": {"name": "sync-back-context"},
  "sync_back": ["allowed/", "allowed-file.txt"],
  "tls": {"host_fingerprint": "` + server.Fingerprint() + `"},
  "mounts": [{"path": "."}],
  "discovery": {"enabled": false}
}`
	writeTestFile(t, configPath, configContent)
	writeTestFile(t, filepath.Join(project, "allowed-file.txt"), "before\n")
	writeTestFile(t, filepath.Join(project, "blocked-file.txt"), "before\n")

	pairCode, err := RunHostPairCode(HostPairCodeParams{StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := RunPair(ctx, project, PairParams{
		AddressOverride: addr,
		ConfigPath:      configPath,
		Code:            pairCode.Code,
		ClientLabel:     "sync-back-client",
	}); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer

	code, err := RunExec(ctx, project, ExecParams{
		AddressOverride: addr,
		Command: []string{
			"sh",
			"-c",
			`mkdir -p allowed blocked; printf "after\n" > allowed-file.txt; printf "after\n" > blocked-file.txt; printf "ok\n" > allowed/report.txt; printf "no\n" > blocked/report.txt`,
		},
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		t.Fatal(err)
	}

	if code != 0 {
		t.Fatalf("unexpected exit code: %d stderr=%s", code, stderr.String())
	}

	allowed, err := os.ReadFile(filepath.Join(project, "allowed-file.txt"))
	if err != nil {
		t.Fatal(err)
	}

	if strings.TrimSpace(string(allowed)) != "after" {
		t.Fatalf("expected allowed file to sync back, got %q", string(allowed))
	}

	blocked, err := os.ReadFile(filepath.Join(project, "blocked-file.txt"))
	if err != nil {
		t.Fatal(err)
	}

	if strings.TrimSpace(string(blocked)) != "before" {
		t.Fatalf("expected blocked file to remain local, got %q", string(blocked))
	}

	if _, err := os.Stat(filepath.Join(project, "allowed", "report.txt")); err != nil {
		t.Fatalf("expected allowed dir output to sync back: %v", err)
	}

	if _, err := os.Stat(filepath.Join(project, "blocked", "report.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected blocked dir output to stay remote-only, got err=%v", err)
	}

	cancel()
	waitForServerShutdown(t, "sync-back-host", errCh)
}

//nolint:cyclop // Integration test exercises multi-host setup, pairing, verification, and shutdown.
func TestRunPairSupportsMultipleHosts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	home := t.TempDir()
	t.Setenv("HOME", home)

	type hostFixture struct {
		server     *host.Server
		stateDir   string
		addr       string
		configPath string
		errCh      chan error
	}

	startHost := func(t *testing.T, label string) hostFixture {
		t.Helper()

		stateDir := t.TempDir()

		server, err := host.New(host.Options{
			ListenAddr:       "127.0.0.1:0",
			StateDir:         stateDir,
			AdvertiseName:    label,
			DisableDiscovery: true,
		})
		if err != nil {
			t.Fatal(err)
		}

		errCh := make(chan error, 1)

		go func() { errCh <- server.Serve(ctx) }()

		project := t.TempDir()
		configPath := filepath.Join(project, ".rmtx.json")

		configContent := `{
  "version": 1,
  "host": "` + waitForAddr(t, server) + `",
  "tls": {"host_fingerprint": "` + server.Fingerprint() + `"},
  "discovery": {"enabled": false}
}`
		if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
			t.Fatal(err)
		}

		return hostFixture{
			server:     server,
			stateDir:   stateDir,
			addr:       server.Addr(),
			configPath: configPath,
			errCh:      errCh,
		}
	}

	hostA := startHost(t, "host-a")
	hostB := startHost(t, "host-b")

	for _, tc := range []struct {
		name string
		host hostFixture
	}{
		{name: "host-a", host: hostA},
		{name: "host-b", host: hostB},
	} {
		pairCode, err := RunHostPairCode(HostPairCodeParams{StateDir: tc.host.stateDir})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := RunPair(ctx, filepath.Dir(tc.host.configPath), PairParams{
			AddressOverride: tc.host.addr,
			ConfigPath:      tc.host.configPath,
			Code:            pairCode.Code,
			ClientLabel:     "multi-host-client",
		}); err != nil {
			t.Fatalf("%s pair failed: %v", tc.name, err)
		}
	}

	for _, tc := range []struct {
		name string
		host hostFixture
	}{
		{name: "host-a", host: hostA},
		{name: "host-b", host: hostB},
	} {
		ping, err := RunPing(ctx, filepath.Dir(tc.host.configPath), RemoteParams{
			AddressOverride: tc.host.addr,
			ConfigPath:      tc.host.configPath,
		})
		if err != nil {
			t.Fatalf("%s ping failed after pairing both hosts: %v", tc.name, err)
		}

		if !ping.Online {
			t.Fatalf("%s expected online ping", tc.name)
		}
	}

	cancel()

	for _, tc := range []struct {
		name  string
		errCh chan error
	}{
		{name: "host-a", errCh: hostA.errCh},
		{name: "host-b", errCh: hostB.errCh},
	} {
		select {
		case err := <-tc.errCh:
			if err != nil {
				t.Fatalf("%s server exited with error: %v", tc.name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %s shutdown", tc.name)
		}
	}
}

func TestRunPairUsesConfiguredHostWhenDiscoveryDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	home := t.TempDir()
	t.Setenv("HOME", home)

	stateDir := t.TempDir()

	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
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
	configPath := filepath.Join(project, ".rmtx.json")

	configContent := `{
  "version": 1,
  "host": "` + addr + `",
  "tls": {"host_fingerprint": "` + server.Fingerprint() + `"},
  "discovery": {"enabled": false}
}`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatal(err)
	}

	pairCode, err := RunHostPairCode(HostPairCodeParams{StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}

	record, err := RunPair(ctx, project, PairParams{
		ConfigPath:     configPath,
		Code:           pairCode.Code,
		ClientLabel:    "cfg-host-client",
		SelectionIndex: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if record.Address != addr {
		t.Fatalf("unexpected paired address: got %s want %s", record.Address, addr)
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

func TestRunPairManualHostAcceptsFingerprintOverride(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	home := t.TempDir()
	t.Setenv("HOME", home)

	stateDir := t.TempDir()

	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
		StateDir:         stateDir,
		DisableDiscovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)

	go func() { errCh <- server.Serve(ctx) }()

	addr := waitForAddr(t, server)

	pairCode, err := RunHostPairCode(HostPairCodeParams{StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}

	record, err := RunPair(ctx, t.TempDir(), PairParams{
		AddressOverride: addr,
		Fingerprint:     server.Fingerprint(),
		Code:            pairCode.Code,
		ClientLabel:     "manual-host-client",
	})
	if err != nil {
		t.Fatal(err)
	}

	if record.Fingerprint != server.Fingerprint() {
		t.Fatalf(
			"unexpected paired fingerprint: got %s want %s",
			record.Fingerprint,
			server.Fingerprint(),
		)
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

func TestRunPairRequestsCodeWhenCodeOmitted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	home := t.TempDir()
	t.Setenv("HOME", home)

	stateDir := t.TempDir()
	codeCh := make(chan string, 1)

	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(&pairCodeLogCapture{codes: codeCh}, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)

	go func() { errCh <- server.Serve(ctx) }()

	addr := waitForAddr(t, server)
	project := t.TempDir()
	configPath := filepath.Join(project, ".rmtx.json")

	configContent := `{
  "version": 1,
  "host": "` + addr + `",
  "tls": {"host_fingerprint": "` + server.Fingerprint() + `"},
  "discovery": {"enabled": false}
}`
	writeTestFile(t, configPath, configContent)

	reader, writer := io.Pipe()
	inputErrCh := startPairCodeInput(codeCh, writer)

	var stdout bytes.Buffer

	record, err := RunPair(ctx, project, PairParams{
		ConfigPath:     configPath,
		ClientLabel:    "interactive-client",
		SelectionIndex: 1,
		Stdin:          reader,
		Stdout:         &stdout,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := <-inputErrCh; err != nil {
		t.Fatal(err)
	}

	if record.Address != addr {
		t.Fatalf("unexpected paired address: got %s want %s", record.Address, addr)
	}

	output := stdout.String()
	if !strings.Contains(output, "pair code requested from") {
		t.Fatalf("expected pair prompt in stdout, got %q", output)
	}

	if !strings.Contains(output, "Enter code: ") {
		t.Fatalf("expected code entry prompt, got %q", output)
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

//nolint:cyclop // End-to-end init test covers discovery, trust, config write, pairing, and shutdown.
func TestRunInitCreatesConfigAndRequestsCodeFromSelectedHost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	home := t.TempDir()
	t.Setenv("HOME", home)

	type hostFixture struct {
		server *host.Server
		addr   string
		codeCh chan string
		errCh  chan error
	}

	startHost := func(t *testing.T, label string) hostFixture {
		t.Helper()

		stateDir := t.TempDir()
		codeCh := make(chan string, 1)

		server, err := host.New(host.Options{
			ListenAddr:       "127.0.0.1:0",
			StateDir:         stateDir,
			AdvertiseName:    label,
			DisableDiscovery: true,
			Logger:           log.New(&pairCodeLogCapture{codes: codeCh}, "", 0),
		})
		if err != nil {
			t.Fatal(err)
		}

		errCh := make(chan error, 1)

		go func() { errCh <- server.Serve(ctx) }()

		return hostFixture{
			server: server,
			addr:   waitForAddr(t, server),
			codeCh: codeCh,
			errCh:  errCh,
		}
	}

	hostA := startHost(t, "host-a")
	hostB := startHost(t, "host-b")

	setDiscoverAllHostsForTest(
		t,
		func(context.Context, string, time.Duration) ([]discovery.Result, error) {
			return []discovery.Result{
				{
					Instance:        "host-a",
					OS:              runtime.GOOS,
					Address:         hostA.addr,
					HostFingerprint: hostA.server.Fingerprint(),
					PairingEnabled:  true,
				},
				{
					Instance:        "host-b",
					OS:              runtime.GOOS,
					Address:         hostB.addr,
					HostFingerprint: hostB.server.Fingerprint(),
					PairingEnabled:  true,
				},
			}, nil
		},
	)

	project := t.TempDir()
	reader, writer := io.Pipe()
	inputErrCh := startScriptedPairInput("2\ny\n", hostB.codeCh, writer)

	var stdout bytes.Buffer

	result, err := RunInit(ctx, project, InitParams{
		ClientLabel: "init-client",
		Stdin:       reader,
		Stdout:      &stdout,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := <-inputErrCh; err != nil {
		t.Fatal(err)
	}

	expectedConfigPath := filepath.Join(project, ".rmtx.json")
	if result.ConfigPath != expectedConfigPath {
		t.Fatalf("unexpected config path: got %s want %s", result.ConfigPath, expectedConfigPath)
	}

	if result.Host.Address != hostB.addr {
		t.Fatalf("unexpected paired address: got %s want %s", result.Host.Address, hostB.addr)
	}

	if result.Host.Fingerprint != hostB.server.Fingerprint() {
		t.Fatalf(
			"unexpected paired fingerprint: got %s want %s",
			result.Host.Fingerprint,
			hostB.server.Fingerprint(),
		)
	}

	loaded, err := config.ResolveRequired(project, "")
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Config.TLS.HostFingerprint != hostB.server.Fingerprint() {
		t.Fatalf(
			"unexpected config fingerprint: got %s want %s",
			loaded.Config.TLS.HostFingerprint,
			hostB.server.Fingerprint(),
		)
	}

	if len(loaded.Config.Mounts) != 1 || loaded.Config.Mounts[0].Path != "." {
		t.Fatalf("unexpected mounts: %#v", loaded.Config.Mounts)
	}

	select {
	case code := <-hostA.codeCh:
		t.Fatalf("expected unselected host to stay idle, got code %q", code)
	default:
	}

	ping, err := RunPing(ctx, project, RemoteParams{AddressOverride: hostB.addr})
	if err != nil {
		t.Fatal(err)
	}

	if !ping.Online {
		t.Fatal("expected ping to report selected host online")
	}

	output := stdout.String()
	if !strings.Contains(output, "Select host: ") {
		t.Fatalf("expected host selection prompt, got %q", output)
	}

	if !strings.Contains(output, "Trust host") {
		t.Fatalf("expected trust prompt, got %q", output)
	}

	if !strings.Contains(output, "Enter code: ") {
		t.Fatalf("expected code prompt, got %q", output)
	}

	cancel()

	for _, tc := range []struct {
		name  string
		errCh chan error
	}{
		{name: "host-a", errCh: hostA.errCh},
		{name: "host-b", errCh: hostB.errCh},
	} {
		select {
		case err := <-tc.errCh:
			if err != nil {
				t.Fatalf("%s server exited with error: %v", tc.name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %s shutdown", tc.name)
		}
	}
}

func TestRunInitManualHostBypassesDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	home := t.TempDir()
	t.Setenv("HOME", home)

	stateDir := t.TempDir()
	codeCh := make(chan string, 1)

	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
		StateDir:         stateDir,
		AdvertiseName:    "manual-host",
		DisableDiscovery: true,
		Logger:           log.New(&pairCodeLogCapture{codes: codeCh}, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)

	go func() { errCh <- server.Serve(ctx) }()

	addr := waitForAddr(t, server)
	project := t.TempDir()

	reader, writer := io.Pipe()
	inputErrCh := startScriptedPairInput("y\n", codeCh, writer)

	var stdout bytes.Buffer

	result, err := RunInit(ctx, project, InitParams{
		AddressOverride: addr,
		Fingerprint:     server.Fingerprint(),
		ClientLabel:     "manual-init-client",
		Stdin:           reader,
		Stdout:          &stdout,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := <-inputErrCh; err != nil {
		t.Fatal(err)
	}

	assertHostRecord(t, result.Host, addr, server.Fingerprint())

	loaded, err := config.ResolveRequired(project, "")
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Config.Host != addr {
		t.Fatalf("unexpected configured host: got %s want %s", loaded.Config.Host, addr)
	}

	assertConfigFingerprint(t, loaded.Config.TLS.HostFingerprint, server.Fingerprint())
	assertManualInitOutput(t, stdout.String())

	cancel()
	waitForServerShutdown(t, "manual-host", errCh)
}

func TestRunInitFailsWhenConfigAlreadyExists(t *testing.T) {
	project := t.TempDir()
	writeTestFile(t, filepath.Join(project, ".rmtx.json"), `{"version":1}`)

	_, err := RunInit(context.Background(), project, InitParams{})
	if err == nil {
		t.Fatal("expected existing config error")
	}

	if !strings.Contains(err.Error(), "config already exists") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPairPromptsReuseBufferedInput(t *testing.T) {
	var stdout bytes.Buffer

	params := PairParams{
		Stdin:  strings.NewReader("2\n123456\n"),
		Stdout: &stdout,
	}

	index, err := promptForPairSelection(&params, []discovery.Result{
		{Instance: "host-a", Address: "10.0.0.1:33221", HostFingerprint: "sha256:host-a"},
		{Instance: "host-b", Address: "10.0.0.2:33221", HostFingerprint: "sha256:host-b"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if index != 2 {
		t.Fatalf("unexpected selected host index: got %d want 2", index)
	}

	code, err := promptForPairCode(&params, client.PairCodeResult{
		HostName:  "host-b",
		ExpiresAt: time.Unix(123, 0).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if code != "123456" {
		t.Fatalf("unexpected pairing code: got %q want %q", code, "123456")
	}

	output := stdout.String()
	if !strings.Contains(output, "Select host: ") {
		t.Fatalf("expected host selection prompt, got %q", output)
	}

	if !strings.Contains(output, "Enter code: ") {
		t.Fatalf("expected pairing code prompt, got %q", output)
	}
}

func TestRequestPairCodeQuotesClientLabelInLogs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stateDir := t.TempDir()

	var logs bytes.Buffer

	server, err := host.New(host.Options{
		ListenAddr:       "127.0.0.1:0",
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(&logs, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)

	go func() { errCh <- server.Serve(ctx) }()

	addr := waitForAddr(t, server)

	_, err = client.RequestPairCode(ctx, client.PairOptions{
		Address: addr,
		Host: clientstate.HostRecord{
			Address:     addr,
			Fingerprint: server.Fingerprint(),
		},
		ClientLabel: "evil\nforged\x1b[31m",
	})
	if err != nil {
		t.Fatal(err)
	}

	logOutput := logs.String()
	if !strings.Contains(logOutput, "client=\"evil\\nforged\\x1b[31m\"") {
		t.Fatalf("expected quoted client label in logs, got %q", logOutput)
	}

	if strings.Contains(logOutput, "evil\nforged") {
		t.Fatalf("expected log to escape embedded newline, got %q", logOutput)
	}

	if strings.Contains(logOutput, "\x1b[31m") {
		t.Fatalf("expected log to escape terminal control sequence, got %q", logOutput)
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

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertHostRecord(
	t *testing.T,
	record clientstate.HostRecord,
	wantAddress string,
	wantFingerprint string,
) {
	t.Helper()

	if record.Address != wantAddress {
		t.Fatalf("unexpected paired address: got %s want %s", record.Address, wantAddress)
	}

	if record.Fingerprint != wantFingerprint {
		t.Fatalf(
			"unexpected paired fingerprint: got %s want %s",
			record.Fingerprint,
			wantFingerprint,
		)
	}
}

func assertConfigFingerprint(t *testing.T, got string, want string) {
	t.Helper()

	if got != want {
		t.Fatalf("unexpected config fingerprint: got %s want %s", got, want)
	}
}

func assertManualInitOutput(t *testing.T, output string) {
	t.Helper()

	if strings.Contains(output, "Select host: ") {
		t.Fatalf("did not expect discovery selection prompt, got %q", output)
	}

	if !strings.Contains(output, "Trust host") {
		t.Fatalf("expected trust prompt, got %q", output)
	}

	if !strings.Contains(output, "Enter code: ") {
		t.Fatalf("expected code prompt, got %q", output)
	}
}

func startPairCodeInput(codeCh <-chan string, writer *io.PipeWriter) <-chan error {
	return startScriptedPairInput("", codeCh, writer)
}

func startScriptedPairInput(
	prefix string,
	codeCh <-chan string,
	writer *io.PipeWriter,
) <-chan error {
	errCh := make(chan error, 1)

	go func() {
		defer func() { _ = writer.Close() }()

		if prefix != "" {
			if _, err := io.WriteString(writer, prefix); err != nil {
				errCh <- err
				return
			}
		}

		if codeCh == nil {
			errCh <- nil
			return
		}

		select {
		case code := <-codeCh:
			_, err := io.WriteString(writer, code+"\n")
			errCh <- err
		case <-time.After(2 * time.Second):
			errCh <- errors.New("timed out waiting for host pair code")
		}
	}()

	return errCh
}

func setDiscoverAllHostsForTest(
	t *testing.T,
	fn func(context.Context, string, time.Duration) ([]discovery.Result, error),
) {
	t.Helper()

	prev := discoverAllHosts
	discoverAllHosts = fn

	t.Cleanup(func() { discoverAllHosts = prev })
}

func waitForServerShutdown(t *testing.T, name string, errCh <-chan error) {
	t.Helper()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("%s server exited with error: %v", name, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s shutdown", name)
	}
}

func TestResolvePairTargetManualHostKeepsExplicitFingerprint(t *testing.T) {
	result, err := resolvePairTarget(
		context.Background(),
		config.WithDefaults(config.Default()),
		&PairParams{
			AddressOverride: "192.168.1.42",
			Fingerprint:     "sha256:expected",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if result.Address != "192.168.1.42:33221" {
		t.Fatalf("unexpected address: got %s", result.Address)
	}

	if result.HostFingerprint != "sha256:expected" {
		t.Fatalf("unexpected fingerprint: got %s", result.HostFingerprint)
	}
}

type pairCodeLogCapture struct {
	codes chan<- string
}

func (w *pairCodeLogCapture) Write(p []byte) (int, error) {
	for _, field := range strings.Fields(string(p)) {
		if !strings.HasPrefix(field, "code=") {
			continue
		}

		select {
		case w.codes <- strings.TrimPrefix(field, "code="):
		default:
		}

		break
	}

	return len(p), nil
}

func TestResolvePairTargetRequiresPinnedFingerprint(t *testing.T) {
	_, err := resolvePairTarget(
		context.Background(),
		config.WithDefaults(config.Default()),
		&PairParams{
			AddressOverride: "192.168.1.42",
		},
	)
	if err == nil {
		t.Fatal("expected missing fingerprint error")
	}

	if !strings.Contains(err.Error(), "host fingerprint is required for pairing") {
		t.Fatalf("unexpected error: %v", err)
	}
}

const hostBAddress = "10.0.0.2:33221"

func TestSelectDiscoveredHostFiltersPinnedFingerprintBeforeMultipleHostError(t *testing.T) {
	result, err := selectDiscoveredHost([]discovery.Result{
		{Address: "10.0.0.1:33221", HostFingerprint: "sha256:host-a"},
		{Address: hostBAddress, HostFingerprint: "sha256:host-b"},
	}, "sha256:host-b", "")
	if err != nil {
		t.Fatal(err)
	}

	if result.Address != hostBAddress {
		t.Fatalf("unexpected selected host: got %s", result.Address)
	}
}

func TestSelectDiscoveredHostPrefersKnownAddressForPinnedFingerprint(t *testing.T) {
	result, err := selectDiscoveredHost([]discovery.Result{
		{Address: "10.0.0.1:33221", HostFingerprint: "sha256:host"},
		{Address: hostBAddress, HostFingerprint: "sha256:host"},
	}, "sha256:host", hostBAddress)
	if err != nil {
		t.Fatal(err)
	}

	if result.Address != hostBAddress {
		t.Fatalf("unexpected preferred host: got %s", result.Address)
	}
}

func TestResolveClientHostUsesFingerprintWhenAddressChanges(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	state, err := clientstate.Load()
	if err != nil {
		t.Fatal(err)
	}

	state.UpsertHost(clientstate.HostRecord{
		Address:       "10.0.0.1:33221",
		Name:          "test-host",
		Fingerprint:   "sha256:host",
		Paired:        true,
		ClientCertPEM: "cert",
		ClientKeyPEM:  "key",
	})

	if err := state.Save(); err != nil {
		t.Fatal(err)
	}

	loaded, record, err := resolveClientHost("10.0.0.2:33221", "sha256:host")
	if err != nil {
		t.Fatal(err)
	}

	if loaded == nil || record == nil {
		t.Fatal("expected paired host to resolve by fingerprint")
	}

	if record.Address != "10.0.0.2:33221" {
		t.Fatalf("unexpected updated address: got %s", record.Address)
	}

	reloaded, err := clientstate.Load()
	if err != nil {
		t.Fatal(err)
	}

	if reloaded.FindHostByAddress("10.0.0.2:33221") == nil {
		t.Fatal("expected state to persist updated address")
	}
}

func TestResolveClientHostSkipsOccupiedAddressToAvoidDuplicates(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	state, err := clientstate.Load()
	if err != nil {
		t.Fatal(err)
	}

	state.UpsertHost(clientstate.HostRecord{
		Address:       "10.0.0.1:33221",
		Name:          "moving-host",
		Fingerprint:   "sha256:host-a",
		Paired:        true,
		ClientCertPEM: "cert-a",
		ClientKeyPEM:  "key-a",
	})
	state.UpsertHost(clientstate.HostRecord{
		Address:       "10.0.0.2:33221",
		Name:          "occupied-host",
		Fingerprint:   "sha256:host-b",
		Paired:        true,
		ClientCertPEM: "cert-b",
		ClientKeyPEM:  "key-b",
	})

	if err := state.Save(); err != nil {
		t.Fatal(err)
	}

	_, record, err := resolveClientHost("10.0.0.2:33221", "sha256:host-a")
	if err != nil {
		t.Fatal(err)
	}

	if record == nil {
		t.Fatal("expected host record to resolve by fingerprint")
	}

	if record.Address != "10.0.0.1:33221" {
		t.Fatalf(
			"expected existing address to remain when target is occupied, got %s",
			record.Address,
		)
	}

	reloaded, err := clientstate.Load()
	if err != nil {
		t.Fatal(err)
	}

	occupiedCount := 0

	for _, host := range reloaded.Data.Hosts {
		if host.Address == "10.0.0.2:33221" {
			occupiedCount++
		}
	}

	if occupiedCount != 1 {
		t.Fatalf("expected one host to keep occupied address, got %d", occupiedCount)
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
