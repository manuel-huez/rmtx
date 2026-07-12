package pathutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSecureJoinExistingRejectsEscapingSymlink(t *testing.T) {
	root := t.TempDir()

	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}

	if _, err := SecureJoinExisting(root, "escape"); err == nil {
		t.Fatal("expected escaping symlink to fail")
	}
}
