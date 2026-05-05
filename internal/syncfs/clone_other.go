//go:build !darwin && !linux && !windows

package syncfs

import "io/fs"

func cloneFile(_, _ string, _ fs.FileMode) (bool, error) {
	return false, nil
}
