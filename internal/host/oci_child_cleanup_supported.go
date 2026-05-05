//go:build linux || windows

package host

import (
	"errors"
	"os"
	"strings"
)

func cleanupTempFile(path string) commandCleanup {
	return func() error {
		if strings.TrimSpace(path) == "" {
			return nil
		}

		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}

		return nil
	}
}
