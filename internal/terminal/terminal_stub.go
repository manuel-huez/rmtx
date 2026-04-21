//go:build !linux

package terminal

import (
	"fmt"
	"os"
)

type State struct{}

func IsTerminal(f *os.File) bool { return false }

func MakeRaw(f *os.File) (State, error) {
	return State{}, fmt.Errorf("interactive TTY is not supported on this platform")
}

func Restore(f *os.File, state State) error { return nil }

func Size(f *os.File) (int, int, error) {
	return 0, 0, fmt.Errorf("interactive TTY is not supported on this platform")
}
