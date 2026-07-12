//go:build windows

package pathutil

import "golang.org/x/sys/windows"

// ReplaceFile atomically replaces newpath with oldpath.
func ReplaceFile(oldpath, newpath string) error {
	oldptr, err := windows.UTF16PtrFromString(oldpath)
	if err != nil {
		return err
	}
	newptr, err := windows.UTF16PtrFromString(newpath)
	if err != nil {
		return err
	}

	return windows.MoveFileEx(
		oldptr,
		newptr,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
	)
}

func syncDir(string) error {
	// MOVEFILE_WRITE_THROUGH flushes the replacement before returning.
	return nil
}
