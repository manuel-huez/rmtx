//go:build !linux

package host

import (
	"errors"
	"os"
)

func openPTY(rows, cols int) (*os.File, *os.File, error) {
	return nil, nil, errors.New("interactive TTY is not supported on this platform")
}

func resizePTY(f *os.File, rows, cols int) error {
	return errors.New("interactive TTY is not supported on this platform")
}

var (
	_ = openPTY
	_ = resizePTY
)
