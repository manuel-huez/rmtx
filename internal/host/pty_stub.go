//go:build !linux

package host

import (
	"fmt"
	"os"
)

func openPTY(rows, cols int) (*os.File, *os.File, error) {
	return nil, nil, fmt.Errorf("interactive TTY is not supported on this platform")
}

func resizePTY(f *os.File, rows, cols int) error {
	return fmt.Errorf("interactive TTY is not supported on this platform")
}
