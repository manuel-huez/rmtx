//go:build !linux

package host

import (
	"errors"
	"os"
)

func resizePTY(f *os.File, rows, cols int) error {
	return errors.New("interactive TTY is not supported on this platform")
}
