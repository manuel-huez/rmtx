package syncfs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBlobStoreRejectsDigestMismatch(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	wantHash := sha256Hex([]byte("expected"))

	if err := store.Store(wantHash, 6, strings.NewReader("actual")); err == nil {
		t.Fatal("Store() accepted content under wrong digest")
	}

	if _, err := os.Stat(mustBlobPath(t, store, wantHash)); !os.IsNotExist(err) {
		t.Fatalf("mismatched blob persisted: %v", err)
	}
}

func TestBlobStoreReplacesCorruptCachedBlob(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	content := "correct"
	hash := sha256Hex([]byte(content))

	path := mustBlobPath(t, store, hash)
	if err := os.MkdirAll(filepath.Dir(path), blobDefaultDirPerm); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, []byte("corrupt"), blobDefaultFileMode); err != nil {
		t.Fatal(err)
	}

	if err := store.Store(hash, int64(len(content)), strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if string(got) != content {
		t.Fatalf("stored blob = %q, want %q", got, content)
	}
}
