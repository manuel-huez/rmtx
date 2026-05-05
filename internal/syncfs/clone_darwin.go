//go:build darwin

package syncfs

import (
	"io/fs"

	"golang.org/x/sys/unix"
)

func cloneFile(src, dest string, _ fs.FileMode) (bool, error) {
	if err := unix.Clonefile(src, dest, 0); err != nil {
		return false, nil
	}

	return true, nil
}
