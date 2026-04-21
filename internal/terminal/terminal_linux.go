//go:build linux

package terminal

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

type State struct {
	termios syscall.Termios
}

type winsize struct {
	row    uint16
	col    uint16
	xpixel uint16
	ypixel uint16
}

func IsTerminal(f *os.File) bool {
	if f == nil {
		return false
	}

	_, err := getTermios(f.Fd())

	return err == nil
}

func MakeRaw(f *os.File) (State, error) {
	if f == nil {
		return State{}, fmt.Errorf("terminal file is required")
	}

	current, err := getTermios(f.Fd())
	if err != nil {
		return State{}, err
	}

	raw := *current
	raw.Iflag &^= syscall.IGNBRK | syscall.BRKINT | syscall.PARMRK |
		syscall.ISTRIP | syscall.INLCR | syscall.IGNCR | syscall.ICRNL | syscall.IXON
	raw.Oflag &^= syscall.OPOST
	raw.Lflag &^= syscall.ECHO | syscall.ECHONL | syscall.ICANON | syscall.ISIG | syscall.IEXTEN
	raw.Cflag &^= syscall.CSIZE | syscall.PARENB
	raw.Cflag |= syscall.CS8
	raw.Cc[syscall.VMIN] = 1
	raw.Cc[syscall.VTIME] = 0

	if err := setTermios(f.Fd(), &raw); err != nil {
		return State{}, err
	}

	return State{termios: *current}, nil
}

func Restore(f *os.File, state State) error {
	if f == nil {
		return nil
	}

	return setTermios(f.Fd(), &state.termios)
}

func Size(f *os.File) (int, int, error) {
	if f == nil {
		return 0, 0, fmt.Errorf("terminal file is required")
	}

	ws, err := getWinsize(f.Fd())
	if err != nil {
		return 0, 0, err
	}

	return int(ws.row), int(ws.col), nil
}

func getTermios(fd uintptr) (*syscall.Termios, error) {
	var termios syscall.Termios
	if _, _, errno := syscall.Syscall6(
		syscall.SYS_IOCTL,
		fd,
		uintptr(syscall.TCGETS),
		uintptr(unsafe.Pointer(&termios)),
		0,
		0,
		0,
	); errno != 0 {
		return nil, errno
	}

	return &termios, nil
}

func setTermios(fd uintptr, termios *syscall.Termios) error {
	if _, _, errno := syscall.Syscall6(
		syscall.SYS_IOCTL,
		fd,
		uintptr(syscall.TCSETS),
		uintptr(unsafe.Pointer(termios)),
		0,
		0,
		0,
	); errno != 0 {
		return errno
	}

	return nil
}

func getWinsize(fd uintptr) (*winsize, error) {
	var ws winsize
	if _, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		fd,
		uintptr(syscall.TIOCGWINSZ),
		uintptr(unsafe.Pointer(&ws)),
	); errno != 0 {
		return nil, errno
	}

	return &ws, nil
}
