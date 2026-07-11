package client

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func lockClientCache(path string) (func() error, error) {
	file, err := openClientCacheLockFile(path)
	if err != nil {
		return nil, err
	}

	overlapped := &windows.Overlapped{}
	if err := windows.LockFileEx(
		windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK,
		0,
		1,
		0,
		overlapped,
	); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock client cache: %w", err)
	}

	return func() error {
		unlockErr := windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, overlapped)
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
