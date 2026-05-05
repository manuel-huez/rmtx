//go:build linux

package host

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveOCICommandPathUsesSpecEnvPATH(t *testing.T) {
	dir := t.TempDir()

	command := filepath.Join(dir, "tool")
	if err := os.WriteFile(command, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	got, err := resolveOCICommandPath("tool", []string{"PATH=" + dir})
	if err != nil {
		t.Fatal(err)
	}

	if got != command {
		t.Fatalf("resolved command=%s want %s", got, command)
	}
}

func TestResolveOCICommandPathKeepsSlashCommand(t *testing.T) {
	got, err := resolveOCICommandPath("/bin/sh", []string{"PATH=/missing"})
	if err != nil {
		t.Fatal(err)
	}

	if got != "/bin/sh" {
		t.Fatalf("resolved command=%s want /bin/sh", got)
	}
}

func TestOCIChildCommandCleanupRemovesSpecFile(t *testing.T) {
	s := &Server{}

	cmd, cleanup, err := s.ociChildCommand(
		context.Background(),
		ociChildSpec{
			RootFS:  t.TempDir(),
			WorkDir: "/",
			Command: []string{"/bin/true"},
			Env:     []string{"SECRET_TOKEN=token"},
		},
		t.TempDir(),
	)
	if err != nil {
		t.Fatal(err)
	}

	specPath := cmd.Args[len(cmd.Args)-1]
	if _, err := os.Stat(specPath); err != nil {
		t.Fatalf("expected spec before cleanup: %v", err)
	}

	if err := cleanup(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(specPath); !os.IsNotExist(err) {
		t.Fatalf("expected spec removed, err=%v", err)
	}
}
