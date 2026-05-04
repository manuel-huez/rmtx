//go:build !windows

package host

import (
	"errors"
	"os/exec"
	"syscall"
)

func configureCommandProcessGroup(cmd *exec.Cmd) {
	sysProcAttr := cmd.SysProcAttr
	if sysProcAttr == nil {
		sysProcAttr = &syscall.SysProcAttr{}
	}

	sysProcAttr.Setpgid = true
	cmd.SysProcAttr = sysProcAttr
}

func killCommandProcessTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}

	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err == nil && pgid > 0 && pgid != syscall.Getpgrp() {
		if killErr := syscall.Kill(-pgid, syscall.SIGKILL); killErr != nil {
			if !errors.Is(killErr, syscall.ESRCH) {
				_ = cmd.Process.Kill()
			}
		}

		return
	}

	_ = cmd.Process.Kill()
}
