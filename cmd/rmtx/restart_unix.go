//go:build !windows

package main

import (
	"os"
	"syscall"
)

func restartHostProcess(executable string, args []string) error {
	if executable == "" {
		exe, err := os.Executable()
		if err != nil {
			return err
		}

		executable = exe
	}

	argv := append([]string{executable, "host"}, args...)

	return syscall.Exec(executable, argv, os.Environ())
}
