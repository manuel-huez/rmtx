//nolint:goconst // Repeated fixture literals keep each test case self-contained.
package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSearchFindsParentConfig(t *testing.T) {
	root := t.TempDir()

	nested := filepath.Join(root, "a", "b", "c")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}

	content := []byte(
		`{"version":1,"host":"10.0.0.42:33221","mounts":[{"path":"."}],"ignore":["node_modules/**"],"ignore_gitignore":true,"env":{"forward":["GOENV"]}}`,
	)
	if err := os.WriteFile(filepath.Join(root, ".rmtx.json"), content, 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, err := Search(nested)
	if err != nil {
		t.Fatal(err)
	}

	if loaded == nil {
		t.Fatal("expected config to be found")
	}

	if loaded.Root != root {
		t.Fatalf("root mismatch: got %s want %s", loaded.Root, root)
	}

	if got := loaded.Config.Host; got != "10.0.0.42:33221" {
		t.Fatalf("host mismatch: %s", got)
	}

	if len(loaded.Config.Ignore) != 1 || loaded.Config.Ignore[0] != "node_modules/**" {
		t.Fatalf("unexpected ignore patterns: %#v", loaded.Config.Ignore)
	}

	if !loaded.Config.IgnoreGitignore {
		t.Fatal("expected ignore_gitignore to be enabled")
	}
}

func TestResolveReturnsDefaultsWithoutConfig(t *testing.T) {
	root := t.TempDir()

	loaded, err := Resolve(root, "")
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Path != "" {
		t.Fatalf("expected no config path, got %s", loaded.Path)
	}

	if len(loaded.Config.Mounts) != 1 || loaded.Config.Mounts[0].Path != "." {
		t.Fatalf("unexpected default mounts: %#v", loaded.Config.Mounts)
	}

	if !loaded.Config.DiscoveryEnabled() {
		t.Fatal("expected discovery to be enabled by default")
	}
}

func TestResolveRequiredFailsWithoutConfig(t *testing.T) {
	root := t.TempDir()

	_, err := ResolveRequired(root, "")
	if err == nil {
		t.Fatal("expected missing config error")
	}

	if !strings.Contains(err.Error(), "local config file is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadedContextIDUsesStableExplicitName(t *testing.T) {
	root := t.TempDir()

	if err := os.WriteFile(
		filepath.Join(root, ".rmtx.json"),
		[]byte(`{"version":1,"context":{"name":"shared context"}}`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	loaded, err := ResolveRequired(root, "")
	if err != nil {
		t.Fatal(err)
	}

	if got := loaded.ContextName(); got != "shared context" {
		t.Fatalf("unexpected context name: %s", got)
	}

	id1 := loaded.ContextID()

	id2 := loaded.ContextID()
	if id1 == "" || id1 != id2 {
		t.Fatalf("expected stable context id, got %q and %q", id1, id2)
	}
}

func TestLoadRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		content string
		want    string
	}{
		{`{"version":1,"token_env":"RMTX_TOKEN"}`, `unknown field "token_env"`},
		{`{"version":2}`, "unsupported config version 2"},
		{`{"version":1,"discovery":{"timeout":"0s"}}`, `invalid discovery.timeout "0s"`},
	}

	for _, tt := range tests {
		path := filepath.Join(t.TempDir(), ".rmtx.json")
		if err := os.WriteFile(path, []byte(tt.content), 0o644); err != nil {
			t.Fatal(err)
		}

		_, err := Load(path)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Fatalf("Load() error = %v, want containing %q", err, tt.want)
		}
	}
}

func TestLoadRuntimeDefaults(t *testing.T) {
	root := t.TempDir()

	path := filepath.Join(root, ".rmtx.json")
	if err := os.WriteFile(
		path,
		[]byte(`{
  "version": 1,
  "runtime": {
    "type": "oci",
    "image": "node:22",
    "setup": {"context_inputs": ["package-lock.json"]},
    "volumes": [{"name": "npm-cache", "target": "/root/.npm"}]
  }
}`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	runtime := loaded.Config.Runtime
	if runtime.Type != "oci" || runtime.Image != "node:22" {
		t.Fatalf("unexpected runtime: %#v", runtime)
	}

	if runtime.PullPolicy != "if_missing" {
		t.Fatalf("unexpected pull policy: %s", runtime.PullPolicy)
	}

	if runtime.WorkDir != "/workspace" {
		t.Fatalf("unexpected runtime workdir: %s", runtime.WorkDir)
	}

	if runtime.Network != "host" || runtime.GPU != "none" || runtime.User != "root" {
		t.Fatalf("unexpected runtime defaults: %#v", runtime)
	}
}

func TestValidateRuntimeAppliesDefaults(t *testing.T) {
	err := ValidateRuntime(RuntimeConfig{
		Type:  "oci",
		Image: "node:22",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateRuntimeRejectsPartialConfigWithoutType(t *testing.T) {
	err := ValidateRuntime(RuntimeConfig{
		Image: "node:22",
	})
	if err == nil {
		t.Fatal("expected missing runtime type error")
	}

	if !strings.Contains(err.Error(), "runtime.type is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsInvalidRuntimeVolume(t *testing.T) {
	root := t.TempDir()

	path := filepath.Join(root, ".rmtx.json")
	if err := os.WriteFile(
		path,
		[]byte(`{
  "version": 1,
  "runtime": {
    "type": "oci",
    "image": "node:22",
    "volumes": [{"name": "cache", "target": "relative"}]
  }
}`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected invalid runtime volume error")
	}

	if !strings.Contains(err.Error(), "invalid runtime volume target") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsRelativeRuntimeWorkDir(t *testing.T) {
	root := t.TempDir()

	path := filepath.Join(root, ".rmtx.json")
	if err := os.WriteFile(
		path,
		[]byte(`{
  "version": 1,
  "runtime": {
    "type": "oci",
    "image": "node:22",
    "workdir": "workspace"
  }
}`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected invalid runtime workdir error")
	}

	if !strings.Contains(err.Error(), "invalid runtime.workdir") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsEscapingRuntimeWorkDir(t *testing.T) {
	root := t.TempDir()

	path := filepath.Join(root, ".rmtx.json")
	if err := os.WriteFile(
		path,
		[]byte(`{
  "version": 1,
  "runtime": {
    "type": "oci",
    "image": "node:22",
    "workdir": "/../../tmp/rmtx"
  }
}`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected invalid runtime workdir error")
	}

	if !strings.Contains(err.Error(), "invalid runtime.workdir") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsEscapingRuntimeVolumeTarget(t *testing.T) {
	root := t.TempDir()

	path := filepath.Join(root, ".rmtx.json")
	if err := os.WriteFile(
		path,
		[]byte(`{
  "version": 1,
  "runtime": {
    "type": "oci",
    "image": "node:22",
    "volumes": [{"name": "cache", "target": "/../../tmp/rmtx"}]
  }
}`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected invalid runtime volume target error")
	}

	if !strings.Contains(err.Error(), "invalid runtime volume target") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsUnsupportedRuntimeUser(t *testing.T) {
	root := t.TempDir()

	path := filepath.Join(root, ".rmtx.json")
	if err := os.WriteFile(
		path,
		[]byte(`{
  "version": 1,
  "runtime": {
    "type": "oci",
    "image": "node:22",
    "user": "1000"
  }
}`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected unsupported runtime user error")
	}

	if !strings.Contains(err.Error(), "unsupported runtime.user") {
		t.Fatalf("unexpected error: %v", err)
	}
}
