//go:build !windows

package pathutil

import (
	"io/fs"
	"os"
)

// Symlink creates a symbolic link.
func Symlink(oldname, newname string) error {
	return os.Symlink(oldname, newname)
}

// Link creates a hard link.
func Link(oldname, newname string) error {
	return os.Link(oldname, newname)
}

// Chmod changes path mode.
func Chmod(path string, mode fs.FileMode) error {
	return os.Chmod(path, mode)
}
