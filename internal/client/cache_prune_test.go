package client

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/syncfs"
)

func TestPruneClientBlobCacheKeepsHashesFromAllManifests(t *testing.T) {
	dir := t.TempDir()
	scanStarted := time.Now()
	old := scanStarted.Add(-time.Hour)
	firstHash := strings.Repeat("a", sha256HexLength)
	secondHash := strings.Repeat("b", sha256HexLength)
	staleHash := strings.Repeat("c", sha256HexLength)
	recentHash := strings.Repeat("d", sha256HexLength)

	writeCachedManifest(
		t,
		dir,
		"first.json",
		[]syncfs.Entry{{Kind: syncfs.KindFile, Hash: firstHash}},
	)
	writeCachedManifest(
		t,
		dir,
		"second.json",
		[]syncfs.Entry{{Kind: syncfs.KindFile, Hash: secondHash}},
	)
	writeCachedBlob(t, dir, firstHash, "first", old)
	writeCachedBlob(t, dir, secondHash, "second", old)
	stalePath := writeCachedBlob(t, dir, staleHash, "stale", old)
	writeCachedBlob(t, dir, recentHash, "recent", scanStarted)

	deleted, bytes, err := pruneClientBlobCacheInDir(dir, scanStarted)
	if err != nil {
		t.Fatal(err)
	}

	if len(deleted) != 1 || deleted[0].Name != staleHash || deleted[0].Path != stalePath {
		t.Fatalf("deleted=%#v want stale blob", deleted)
	}

	if bytes != int64(len("stale")) {
		t.Fatalf("bytes=%d want %d", bytes, len("stale"))
	}

	for _, hash := range []string{firstHash, secondHash, recentHash} {
		if _, err := os.Stat(blobPath(dir, hash)); err != nil {
			t.Fatalf("kept blob %s: %v", hash, err)
		}
	}

	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale blob still exists: %v", err)
	}
}

func TestPruneClientBlobCacheRejectsInvalidManifestBeforeDeleting(t *testing.T) {
	dir := t.TempDir()

	manifestDir := filepath.Join(dir, "manifests")
	if err := os.MkdirAll(manifestDir, 0o700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(
		filepath.Join(manifestDir, "broken.json"),
		[]byte("{"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	staleHash := strings.Repeat("a", sha256HexLength)
	stalePath := writeCachedBlob(t, dir, staleHash, "content", time.Now().Add(-time.Hour))

	if _, _, err := pruneClientBlobCacheInDir(dir, time.Now()); err == nil {
		t.Fatal("expected invalid manifest error")
	}

	if _, err := os.Stat(stalePath); err != nil {
		t.Fatalf("blob deleted despite invalid manifest: %v", err)
	}
}

func TestPruneClientBlobCacheRemovesOldInvalidManifest(t *testing.T) {
	dir := t.TempDir()

	manifestPath := filepath.Join(dir, "manifests", "broken.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(manifestPath, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}

	old := time.Now().Add(-2 * staleManifestAge)
	if err := os.Chtimes(manifestPath, old, old); err != nil {
		t.Fatal(err)
	}

	orphanHash := strings.Repeat("e", sha256HexLength)
	orphanPath := writeCachedBlob(t, dir, orphanHash, "orphan", old)

	deleted, _, err := pruneClientBlobCacheInDir(dir, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if len(deleted) != 2 {
		t.Fatalf("deleted=%#v", deleted)
	}

	for _, path := range []string{manifestPath, orphanPath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("stale path remains %s: %v", path, err)
		}
	}
}

func TestPruneClientBlobCacheRemovesStaleManifestTemp(t *testing.T) {
	dir := t.TempDir()

	manifestDir := filepath.Join(dir, "manifests")
	if err := os.MkdirAll(manifestDir, 0o700); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(manifestDir, "manifest.json.tmp")
	if err := os.WriteFile(path, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	old := time.Now().Add(-2 * staleManifestAge)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}

	deleted, bytes, err := pruneClientBlobCacheInDir(dir, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if len(deleted) != 1 || deleted[0].Kind != "client_manifest" || deleted[0].Path != path {
		t.Fatalf("deleted=%#v", deleted)
	}

	if bytes != int64(len("partial")) {
		t.Fatalf("bytes=%d", bytes)
	}
}

func TestPruneClientBlobCacheWithoutCacheDirs(t *testing.T) {
	deleted, bytes, err := pruneClientBlobCacheInDir(t.TempDir(), time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if len(deleted) != 0 || bytes != 0 {
		t.Fatalf("deleted=%#v bytes=%d", deleted, bytes)
	}
}

func TestPruneClientBlobCacheRejectsInvalidHashWithoutEscapingRoot(t *testing.T) {
	dir := t.TempDir()

	outside := filepath.Join(dir, "outside")
	if err := os.WriteFile(outside, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	writeCachedManifest(t, dir, "invalid.json", []syncfs.Entry{{
		Kind: syncfs.KindFile,
		Hash: "../../outside",
	}})

	if _, _, err := pruneClientBlobCacheInDir(dir, time.Now()); err == nil {
		t.Fatal("expected invalid hash error")
	}

	if content, err := os.ReadFile(outside); err != nil || string(content) != "keep" {
		t.Fatalf("outside file changed: content=%q err=%v", content, err)
	}
}

func writeCachedManifest(t *testing.T, dir, name string, entries []syncfs.Entry) {
	t.Helper()

	manifestDir := filepath.Join(dir, "manifests")
	if err := os.MkdirAll(manifestDir, 0o700); err != nil {
		t.Fatal(err)
	}

	content, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(manifestDir, name), content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeCachedBlob(t *testing.T, dir, hash, content string, modTime time.Time) string {
	t.Helper()

	path := blobPath(dir, hash)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatal(err)
	}

	return path
}

func blobPath(dir, hash string) string {
	return filepath.Join(dir, "blobs", hash[:2], hash)
}
