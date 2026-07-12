package host

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureOverlayRootFSBundleCreatesPrivateDirs(t *testing.T) {
	rootfs := filepath.Join(t.TempDir(), "rootfs")
	base := filepath.Join(t.TempDir(), "base")

	if err := ensureOverlayRootFSBundle(rootfs, "key", base); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"upper", "work", "merged", rootFSOverlayMarker, rootFSInstanceMarker} {
		assertPathExists(t, filepath.Join(rootfs, name))
	}
}

func TestEnsureOverlayRootFSBundlePreservesMatchingUpper(t *testing.T) {
	rootfs := filepath.Join(t.TempDir(), "rootfs")
	base := filepath.Join(t.TempDir(), "base")
	upperFile := filepath.Join(rootfs, "upper", "file")

	if err := ensureOverlayRootFSBundle(rootfs, "key", base); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(upperFile, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := ensureOverlayRootFSBundle(rootfs, "key", base); err != nil {
		t.Fatal(err)
	}

	assertPathExists(t, upperFile)
}

func TestEnsureOverlayRootFSBundleReplacesStaleBundle(t *testing.T) {
	rootfs := filepath.Join(t.TempDir(), "rootfs")
	firstBase := filepath.Join(t.TempDir(), "first")
	secondBase := filepath.Join(t.TempDir(), "second")
	upperFile := filepath.Join(rootfs, "upper", "file")

	if err := ensureOverlayRootFSBundle(rootfs, "key", firstBase); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(upperFile, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := ensureOverlayRootFSBundle(rootfs, "key", secondBase); err != nil {
		t.Fatal(err)
	}

	assertPathMissing(t, upperFile)
}
