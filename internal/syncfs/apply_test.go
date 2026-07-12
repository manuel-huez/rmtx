package syncfs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestApplyChangesRollsBackCommitFailure(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "first.txt"), "old")
	mustWrite(t, filepath.Join(root, "blocked"), "not a directory")

	store := NewBlobStore(t.TempDir())
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	firstHash := sha256Hex([]byte("new"))
	childHash := sha256Hex([]byte("child"))

	if err := store.Store(firstHash, 3, strings.NewReader("new")); err != nil {
		t.Fatal(err)
	}

	if err := store.Store(childHash, 5, strings.NewReader("child")); err != nil {
		t.Fatal(err)
	}

	err := ApplyChanges(
		context.Background(),
		root,
		store,
		[]Entry{
			{Path: "first.txt", Kind: KindFile, Hash: firstHash, Size: 3},
			{Path: "blocked/child.txt", Kind: KindFile, Hash: childHash, Size: 5},
		},
		nil,
		ApplyOptions{},
	)
	if err == nil {
		t.Fatal("ApplyChanges() succeeded")
	}

	content, err := os.ReadFile(filepath.Join(root, "first.txt"))
	if err != nil {
		t.Fatal(err)
	}

	if string(content) != "old" {
		t.Fatalf("first.txt = %q, want rollback content", content)
	}

	blocked, err := os.ReadFile(filepath.Join(root, "blocked"))
	if err != nil {
		t.Fatal(err)
	}

	if string(blocked) != "not a directory" {
		t.Fatalf("blocked = %q, want original", blocked)
	}
}

func TestApplyChangesRejectsSymlinkParent(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	victim := filepath.Join(outside, "victim.txt")
	mustWrite(t, victim, "outside")

	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	store := NewBlobStore(t.TempDir())
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	hash := sha256Hex([]byte("changed"))
	if err := store.Store(hash, 7, strings.NewReader("changed")); err != nil {
		t.Fatal(err)
	}

	err := ApplyChanges(
		context.Background(),
		root,
		store,
		[]Entry{{Path: "link/victim.txt", Kind: KindFile, Hash: hash, Size: 7}},
		nil,
		ApplyOptions{},
	)
	if err == nil {
		t.Fatal("ApplyChanges() followed symlink parent")
	}

	content, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}

	if string(content) != "outside" {
		t.Fatalf("outside victim = %q", content)
	}
}
