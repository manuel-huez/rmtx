package syncfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	testFilePath = "file.txt"
	cachedHash   = "cached-hash"
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

func TestBuildManifestTreatsTrailingSlashExcludeAsDirectory(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "cache", "out.txt"), "ignore")
	mustWrite(t, filepath.Join(root, "src", "main.go"), "keep")

	result, err := BuildManifest(root, []MountSpec{{Path: ".", Exclude: []string{"cache/"}}})
	if err != nil {
		t.Fatal(err)
	}

	paths := map[string]bool{}
	for _, entry := range result.Entries {
		paths[entry.Path] = true
	}

	if !paths["src/main.go"] {
		t.Fatalf("expected kept file in manifest: %#v", paths)
	}

	if paths["cache/out.txt"] {
		t.Fatalf("trailing-slash exclude leaked into manifest: %#v", paths)
	}
}

func TestBuildManifestHandlesMoreFilesThanWorkers(t *testing.T) {
	root := t.TempDir()

	for i := range 256 {
		mustWrite(t, filepath.Join(root, "files", fmt.Sprintf("file-%03d.txt", i)), "content")
	}

	done := make(chan error, 1)

	go func() {
		_, err := BuildManifest(root, []MountSpec{{Path: "."}})
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BuildManifest timed out")
	}
}

func TestBuildManifestSkipsSymlinkEscapingRoot(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.txt")
	mustWrite(t, outside, "outside")
	mustSymlinkOrSkip(t, outside, filepath.Join(root, "outside-link"))

	result, err := BuildManifest(root, []MountSpec{{Path: "."}})
	if err != nil {
		t.Fatal(err)
	}

	for _, entry := range result.Entries {
		if entry.Path == "outside-link" {
			t.Fatalf("escaping symlink leaked into manifest: %#v", entry)
		}
	}
}

func TestBuildManifestMakesAbsoluteInRootSymlinkPortable(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.txt")
	mustWrite(t, target, "target")
	mustSymlinkOrSkip(t, target, filepath.Join(root, "target-link"))

	result, err := BuildManifest(root, []MountSpec{{Path: "."}})
	if err != nil {
		t.Fatal(err)
	}

	for _, entry := range result.Entries {
		if entry.Path == "target-link" {
			if entry.Kind != KindSymlink || entry.Linkname != "target.txt" {
				t.Fatalf("unexpected portable symlink entry: %#v", entry)
			}

			return
		}
	}

	t.Fatalf("expected symlink entry in manifest: %#v", result.Entries)
}

func TestBuildManifestContextStopsCanceledWalk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := BuildManifestContext(ctx, t.TempDir(), []MountSpec{{Path: "."}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestPreserveMissingEntriesKeepsMissingKindOnly(t *testing.T) {
	entries := []Entry{{Path: "kept.txt", Kind: KindFile, Hash: "hash"}}
	preserve := []Entry{
		{Path: "link", Kind: KindSymlink, Linkname: "target"},
		{Path: "missing.txt", Kind: KindFile, Hash: "other"},
	}

	got := PreserveMissingEntries(entries, preserve, KindSymlink)

	paths := map[string]Entry{}
	for _, entry := range got {
		paths[entry.Path] = entry
	}

	if _, ok := paths["link"]; !ok {
		t.Fatalf("missing symlink was not preserved: %#v", got)
	}

	if _, ok := paths["missing.txt"]; ok {
		t.Fatalf("wrong kind was preserved: %#v", got)
	}
}

func TestDiffCanIgnoreModeOnlyChanges(t *testing.T) {
	before := []Entry{{Path: "file.txt", Kind: KindFile, Hash: "hash", Size: 4, Mode: 0o644}}
	after := []Entry{{Path: "file.txt", Kind: KindFile, Hash: "hash", Size: 4, Mode: 0o666}}

	changed, deleted := Diff(before, after, DiffOptions{IgnoreMode: true})
	if len(changed) != 0 || len(deleted) != 0 {
		t.Fatalf("mode-only change should be ignored: changed=%#v deleted=%#v", changed, deleted)
	}

	changed, deleted = Diff(before, after, DiffOptions{})
	if len(changed) != 1 || len(deleted) != 0 {
		t.Fatalf(
			"regular diff should report mode change: changed=%#v deleted=%#v",
			changed,
			deleted,
		)
	}
}

func TestNormalizeModesUsesReferenceModeForEquivalentEntries(t *testing.T) {
	reference := []Entry{{Path: "file.txt", Kind: KindFile, Hash: "hash", Size: 4, Mode: 0o644}}
	entries := []Entry{{Path: "file.txt", Kind: KindFile, Hash: "hash", Size: 4, Mode: 0o666}}

	got := NormalizeModes(entries, reference)
	if len(got) != 1 || got[0].Mode != 0o644 {
		t.Fatalf("mode was not normalized from reference: %#v", got)
	}
}

func TestWriteFileLeavesExistingTargetWhenReadFails(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, testFilePath), "original")

	err := WriteFile(
		root,
		Entry{Path: testFilePath, Kind: KindFile, Mode: 0o644},
		errReader{},
	)
	if err == nil {
		t.Fatal("expected WriteFile read error")
	}

	got, readErr := os.ReadFile(filepath.Join(root, testFilePath))
	if readErr != nil {
		t.Fatal(readErr)
	}

	if string(got) != "original" {
		t.Fatalf("existing target was changed after failed write: %q", got)
	}
}

func TestDiffAndBlobStoreMissingHashes(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, testFilePath), "one")

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

	mustWrite(t, filepath.Join(root, testFilePath), "two")

	after, err := BuildManifest(root, []MountSpec{{Path: "."}})
	if err != nil {
		t.Fatal(err)
	}

	changed, deleted := Diff(before.Entries, after.Entries, DiffOptions{})
	if len(deleted) != 0 {
		t.Fatalf("unexpected deletions: %v", deleted)
	}

	if len(changed) != 1 || changed[0].Path != testFilePath {
		t.Fatalf("unexpected changes: %#v", changed)
	}
}

func TestBuildManifestReusesPreviousFileHashWhenMetadataMatches(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, testFilePath)
	mustWrite(t, path, "unchanged")

	initial, err := BuildManifest(root, []MountSpec{{Path: "."}})
	if err != nil {
		t.Fatal(err)
	}

	previous := initial.Entries
	for i := range previous {
		if previous[i].Path == testFilePath {
			previous[i].Hash = cachedHash
		}
	}

	result, err := BuildManifestContextOptions(
		context.Background(),
		root,
		[]MountSpec{{Path: "."}},
		BuildOptions{PreviousEntries: previous},
	)
	if err != nil {
		t.Fatal(err)
	}

	var file Entry

	for _, entry := range result.Entries {
		if entry.Path == testFilePath {
			file = entry
		}
	}

	if file.Hash != cachedHash {
		t.Fatalf("expected cached hash reuse, got %q", file.Hash)
	}

	if result.BlobSources[cachedHash] != path {
		t.Fatalf("expected reused hash to keep blob source, got %#v", result.BlobSources)
	}
}

func TestBuildManifestHashesFileWhenMetadataChanges(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, testFilePath), "content")

	previous := []Entry{{
		Path:    testFilePath,
		Kind:    KindFile,
		Hash:    cachedHash,
		Size:    int64(len("content")),
		Mode:    0o644,
		ModTime: 1,
	}}

	result, err := BuildManifestContextOptions(
		context.Background(),
		root,
		[]MountSpec{{Path: "."}},
		BuildOptions{PreviousEntries: previous},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, entry := range result.Entries {
		if entry.Path == testFilePath && entry.Hash == cachedHash {
			t.Fatal("expected metadata mismatch to force hashing")
		}
	}
}

func TestBlobStoreMaterializePreservesDuplicateContentModTimes(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	const hash = "abcdef"

	content := "same content"
	if err := store.Store(hash, int64(len(content)), strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	first := filepath.Join(root, "first.txt")
	second := filepath.Join(root, "second.txt")
	firstMod := time.Unix(100, 0).UnixNano()
	secondMod := time.Unix(200, 0).UnixNano()

	if err := store.Materialize(hash, first, 0o644, firstMod); err != nil {
		t.Fatal(err)
	}

	if err := store.Materialize(hash, second, 0o644, secondMod); err != nil {
		t.Fatal(err)
	}

	firstInfo, err := os.Stat(first)
	if err != nil {
		t.Fatal(err)
	}

	secondInfo, err := os.Stat(second)
	if err != nil {
		t.Fatal(err)
	}

	if firstInfo.ModTime().UnixNano() != firstMod {
		t.Fatalf(
			"first mtime changed through duplicate-content materialize: got %d want %d",
			firstInfo.ModTime().UnixNano(),
			firstMod,
		)
	}

	if secondInfo.ModTime().UnixNano() != secondMod {
		t.Fatalf(
			"second mtime mismatch: got %d want %d",
			secondInfo.ModTime().UnixNano(),
			secondMod,
		)
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

func mustSymlinkOrSkip(t *testing.T, oldname, newname string) {
	t.Helper()

	if err := os.Symlink(oldname, newname); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
}

type errReader struct{}

func (errReader) Read(_ []byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}
