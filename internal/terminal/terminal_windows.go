//go:build windows

package terminal

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

type State struct {
	mode uint32
}

func IsTerminal(f *os.File) bool {
	if f == nil {
		return false
	}

	var mode uint32

	return windows.GetConsoleMode(windows.Handle(f.Fd()), &mode) == nil
}

func MakeRaw(f *os.File) (State, error) {
	if f == nil {
		return State{}, errors.New("terminal file is required")
	}

	handle := windows.Handle(f.Fd())

	var mode uint32
	if err := windows.GetConsoleMode(handle, &mode); err != nil {
		return State{}, err
	}

	raw := mode
	raw &^= windows.ENABLE_ECHO_INPUT | windows.ENABLE_LINE_INPUT | windows.ENABLE_PROCESSED_INPUT
	raw |= windows.ENABLE_EXTENDED_FLAGS

	if err := windows.SetConsoleMode(handle, raw); err != nil {
		return State{}, err
	}

	return State{mode: mode}, nil
}

func Restore(f *os.File, state State) error {
	if f == nil {
		return nil
	}

	return windows.SetConsoleMode(windows.Handle(f.Fd()), state.mode)
}

func Size(f *os.File) (int, int, error) {
	if f == nil {
		return 0, 0, errors.New("terminal file is required")
	}

	var info windows.ConsoleScreenBufferInfo
	if err := windows.GetConsoleScreenBufferInfo(windows.Handle(f.Fd()), &info); err != nil {
		return 0, 0, err
	}

	rows := int(info.Window.Bottom-info.Window.Top) + 1
	cols := int(info.Window.Right-info.Window.Left) + 1

	return rows, cols, nil
}
