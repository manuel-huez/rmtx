//go:build darwin

package syncfs

import (
	"io/fs"

	"golang.org/x/sys/unix"
)

func cloneFile(src, dest string, _ fs.FileMode) (bool, error) {
	// Clone is an optimization. Caller performs the portable copy fallback.
	return unix.Clonefile(src, dest, 0) == nil, nil
}
