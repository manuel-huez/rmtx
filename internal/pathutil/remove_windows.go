//go:build windows

package pathutil

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// RemoveAll removes path recursively, using WSL when path is inside WSL UNC storage.
func RemoveAll(path string) error {
	parsed, ok, err := ParseWSLUNCPath(path)
	if err != nil {
		return err
	}
	if !ok {
		return os.RemoveAll(path)
	}
	// OCI unpack removes before most writes; avoid WSL process startup for no-op deletes.
	if _, err := os.Lstat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}
	if parsed.LinuxPath == "/" {
		return errors.New("refuse to remove WSL distro root")
	}

	out, err := exec.Command(
		"wsl.exe",
		"--distribution",
		parsed.Distro,
		"--exec",
		"rm",
		"-rf",
		"--",
		parsed.LinuxPath,
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf(
			"remove WSL path in %s: %s: %w",
			parsed.Distro,
			strings.TrimSpace(string(out)),
			err,
		)
	}

	return nil
}
