//go:build !windows

package pathutil

import "os"

// RemoveAll removes path recursively.
func RemoveAll(path string) error {
	return os.RemoveAll(path)
}
