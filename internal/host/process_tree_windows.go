//go:build windows

package host

import "os/exec"

func configureCommandProcessGroup(cmd *exec.Cmd) {}

func killCommandProcessTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}

	_ = cmd.Process.Kill()
}
