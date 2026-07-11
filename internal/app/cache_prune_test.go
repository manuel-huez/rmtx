package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunCachePruneCleansClientCacheWhenRemoteResolutionFails(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	hash := strings.Repeat("a", 64)
	blob := filepath.Join(home, ".rmtx", "blobs", hash[:2], hash)
	if err := os.MkdirAll(filepath.Dir(blob), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blob, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(blob, old, old); err != nil {
		t.Fatal(err)
	}

	result, err := RunCachePrune(context.Background(), home, RemoteParams{
		ConfigPath: filepath.Join(home, "missing.json"),
	})
	if err == nil {
		t.Fatal("expected remote config error")
	}
	if len(result.Deleted) != 1 || result.Deleted[0].Path != blob {
		t.Fatalf("deleted=%#v", result.Deleted)
	}
	if _, err := os.Stat(blob); !os.IsNotExist(err) {
		t.Fatalf("client blob remains after remote failure: %v", err)
	}
}
