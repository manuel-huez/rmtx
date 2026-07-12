//go:build !windows

package pathutil

import "os"

// ReplaceFile atomically replaces newpath with oldpath.
func ReplaceFile(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}

	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}

	return dir.Close()
}
