//go:build linux

package syncfs

import (
	"io/fs"
	"os"

	"golang.org/x/sys/unix"
)

func cloneFile(src, dest string, mode fs.FileMode) (bool, error) {
	in, err := os.Open(src)
	if err != nil {
		return false, err
	}
	defer func() { _ = in.Close() }()

	out, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_EXCL, mode)
	if err != nil {
		return false, err
	}

	if err := unix.IoctlFileClone(int(out.Fd()), int(in.Fd())); err != nil {
		_ = out.Close()

		return false, nil
	}

	if err := out.Close(); err != nil {
		return true, err
	}

	return true, nil
}
