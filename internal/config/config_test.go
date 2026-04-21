package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSearchFindsParentConfig(t *testing.T) {
	root := t.TempDir()

	nested := filepath.Join(root, "a", "b", "c")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}

	content := []byte(
		`{"version":1,"host":"10.0.0.42:33221","mounts":[{"path":"."}],"env":{"forward":["GOENV"]}}`,
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
