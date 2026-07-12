//go:build !windows

package client

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func lockClientCache(path string) (func() error, error) {
	file, err := openClientCacheLockFile(path)
	if err != nil {
		return nil, err
	}

	if err = unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock client cache: %w", err)
	}

	return func() error {
		unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
		closeErr := file.Close()

		if unlockErr != nil {
			return fmt.Errorf("unlock client cache: %w", unlockErr)
		}

		if closeErr != nil {
			return fmt.Errorf("close client cache lock: %w", closeErr)
		}

		return nil
	}, nil
}
