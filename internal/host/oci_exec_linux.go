//go:build linux

package host

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func (s *Server) platformOCIChildCommand(
	ctx context.Context,
	run ociChildCommandRequest,
) (*exec.Cmd, commandCleanup, error) {
	specDir := filepath.Join(run.runtimeDir, runtimeDirName, runtimeSpecDirName)
	if err := os.MkdirAll(specDir, defaultDirMode); err != nil {
		return nil, noopCommandCleanup, err
	}

	specFile, err := os.CreateTemp(specDir, "run-*.json")
	if err != nil {
		return nil, noopCommandCleanup, err
	}

	encErr := json.NewEncoder(specFile).Encode(run.spec)

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
	if strings.EqualFold(run.spec.Network, noneValue) {
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

func pruneWSLStagedRootFS(context.Context, []string) ([]protocol.ContextArtifact, int64, error) {
	return nil, 0, nil
}
