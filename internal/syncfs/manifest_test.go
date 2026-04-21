package syncfs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildManifestRespectsExcludePatterns(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "main.go"), "package main\n")
	mustWrite(t, filepath.Join(root, ".git", "config"), "hidden")
	mustWrite(t, filepath.Join(root, "tmp", "output.txt"), "ignore")
	mustWrite(t, filepath.Join(root, "sub", "keep.txt"), "keep")

	result, err := BuildManifest(
		root,
		[]MountSpec{{Path: ".", Exclude: []string{".git/**", "tmp/**"}}},
	)
	if err != nil {
		t.Fatal(err)
	}

	paths := map[string]bool{}
	for _, entry := range result.Entries {
		paths[entry.Path] = true
	}

	if !paths["main.go"] || !paths["sub/keep.txt"] {
		t.Fatalf("expected tracked files in manifest: %#v", paths)
	}

	if paths[".git/config"] || paths["tmp/output.txt"] {
		t.Fatalf("excluded files leaked into manifest: %#v", paths)
	}
}

func TestDiffAndBlobStoreMissingHashes(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "file.txt"), "one")

	before, err := BuildManifest(root, []MountSpec{{Path: "."}})
	if err != nil {
		t.Fatal(err)
	}

	storeDir := t.TempDir()

	store := NewBlobStore(storeDir)
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	missing := store.MissingHashes(before.Entries)
	if len(missing) != 1 {
		t.Fatalf("expected 1 missing hash, got %d", len(missing))
	}

	file, err := os.Open(before.BlobSources[missing[0]])
	if err != nil {
		t.Fatal(err)
	}

	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Store(missing[0], info.Size(), file); err != nil {
		t.Fatal(err)
	}

	_ = file.Close()

	if got := store.MissingHashes(before.Entries); len(got) != 0 {
		t.Fatalf("expected cache hit on second pass, got %v", got)
	}

	mustWrite(t, filepath.Join(root, "file.txt"), "two")

	after, err := BuildManifest(root, []MountSpec{{Path: "."}})
	if err != nil {
		t.Fatal(err)
	}

	changed, deleted := Diff(before.Entries, after.Entries)
	if len(deleted) != 0 {
		t.Fatalf("unexpected deletions: %v", deleted)
	}

	if len(changed) != 1 || changed[0].Path != "file.txt" {
		t.Fatalf("unexpected changes: %#v", changed)
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
