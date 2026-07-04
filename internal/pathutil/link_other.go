//go:build !windows

package pathutil

import "os"

// Symlink creates a symbolic link.
func Symlink(oldname, newname string) error {
	return os.Symlink(oldname, newname)
}

// Link creates a hard link.
func Link(oldname, newname string) error {
	return os.Link(oldname, newname)
}
