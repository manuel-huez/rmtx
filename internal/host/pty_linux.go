//go:build linux

package host

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

type ptyWinsize struct {
	row    uint16
	col    uint16
	xpixel uint16
	ypixel uint16
}

func openPTY(rows, cols int) (*os.File, *os.File, error) {
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("open ptmx: %w", err)
	}

	if err := unlockPTY(master); err != nil {
		_ = master.Close()
		return nil, nil, err
	}

	name, err := ptyName(master)
	if err != nil {
		_ = master.Close()
		return nil, nil, err
	}

	slave, err := os.OpenFile(name, os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		_ = master.Close()
		return nil, nil, fmt.Errorf("open pty slave %s: %w", name, err)
	}

	if err := resizePTY(master, rows, cols); err != nil {
		_ = slave.Close()
		_ = master.Close()
		return nil, nil, err
	}

	return master, slave, nil
}

func resizePTY(f *os.File, rows, cols int) error {
	if f == nil || rows <= 0 || cols <= 0 {
		return nil
	}

	ws := ptyWinsize{row: uint16(rows), col: uint16(cols)}
	if _, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		f.Fd(),
		uintptr(syscall.TIOCSWINSZ),
		uintptr(unsafe.Pointer(&ws)),
	); errno != 0 {
		return errno
	}

	return nil
}

func unlockPTY(f *os.File) error {
	var unlocked int32
	if _, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		f.Fd(),
		uintptr(syscall.TIOCSPTLCK),
		uintptr(unsafe.Pointer(&unlocked)),
	); errno != 0 {
		return fmt.Errorf("unlock ptmx: %w", errno)
	}

	return nil
}

func ptyName(f *os.File) (string, error) {
	var num uint32
	if _, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		f.Fd(),
		uintptr(syscall.TIOCGPTN),
		uintptr(unsafe.Pointer(&num)),
	); errno != 0 {
		return "", fmt.Errorf("resolve ptmx slave: %w", errno)
	}

	return fmt.Sprintf("/dev/pts/%d", num), nil
}
