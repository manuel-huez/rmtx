//go:build linux

package host

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

func (s *Server) ociChildCommand(
	ctx context.Context,
	spec ociChildSpec,
	contextDir string,
) (*exec.Cmd, commandCleanup, error) {
	if len(spec.Command) == 0 {
		return nil, noopCommandCleanup, errors.New("OCI command is required")
	}

	specDir := filepath.Join(contextDir, runtimeDirName, runtimeSpecDirName)
	if err := os.MkdirAll(specDir, defaultDirMode); err != nil {
		return nil, noopCommandCleanup, err
	}

	specFile, err := os.CreateTemp(specDir, "run-*.json")
	if err != nil {
		return nil, noopCommandCleanup, err
	}

	encErr := json.NewEncoder(specFile).Encode(spec)

	closeErr := specFile.Close()
	if encErr != nil {
		_ = os.Remove(specFile.Name())
		return nil, noopCommandCleanup, encErr
	}

	if closeErr != nil {
		_ = os.Remove(specFile.Name())
		return nil, noopCommandCleanup, closeErr
	}

	exe, err := os.Executable()
	if err != nil {
		_ = os.Remove(specFile.Name())
		return nil, noopCommandCleanup, err
	}

	cmd := exec.CommandContext(ctx, exe, "__rmtx-oci-child", specFile.Name())
	cmd.Env = os.Environ()

	clone := uintptr(syscall.CLONE_NEWUSER |
		syscall.CLONE_NEWNS |
		syscall.CLONE_NEWPID |
		syscall.CLONE_NEWIPC |
		syscall.CLONE_NEWUTS)
	if strings.EqualFold(spec.Network, "none") {
		clone |= syscall.CLONE_NEWNET
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: clone,
		UidMappings: []syscall.SysProcIDMap{
			{ContainerID: 0, HostID: os.Geteuid(), Size: 1},
		},
		GidMappings: []syscall.SysProcIDMap{
			{ContainerID: 0, HostID: os.Getegid(), Size: 1},
		},
		GidMappingsEnableSetgroups: false,
	}

	return cmd, cleanupTempFile(specFile.Name()), nil
}
