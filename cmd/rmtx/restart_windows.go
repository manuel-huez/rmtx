//go:build windows

package main

import (
	"os"
	"os/exec"
)

func restartHostProcess(executable string, args []string) error {
	if executable == "" {
		exe, err := os.Executable()
		if err != nil {
			return err
		}

		executable = exe
	}

	cmd := exec.Command(executable, append([]string{"host"}, args...)...)
	cmd.Env = os.Environ()
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Start()
}
