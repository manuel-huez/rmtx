//go:build !linux && !windows

package terminal

import (
	"errors"
	"os"
)

type State struct{}

func IsTerminal(f *os.File) bool { return false }

func Size(f *os.File) (int, int, error) {
	return 0, 0, errors.New("interactive TTY is not supported on this platform")
}
