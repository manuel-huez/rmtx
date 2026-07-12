//go:build windows

package host

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"strings"

	"github.com/manuel-huez/rmtx/internal/config"
	"github.com/manuel-huez/rmtx/internal/pathutil"
)

func hostIsWindows() bool {
	return true
}

func (s *Server) platformRuntimeStateDir(
	ctx context.Context,
	runtime config.RuntimeConfig,
	runLogs *hostLogSubscription,
) (string, error) {
	if !isOCIRuntime(runtime) {
		return s.opts.StateDir, nil
	}

	distro := strings.TrimSpace(runtime.WSLDistro)
	if err := checkWSLAvailable(ctx, s.hostOnlyLogger(), distro, runLogs); err != nil {
		return "", err
	}

	linuxPath, err := wslUserStateDir(ctx, distro)
	if err != nil {
		return "", err
	}

	return pathutil.WSLUNCPath(distro, linuxPath)
}

func wslUserStateDir(ctx context.Context, distro string) (string, error) {
	// Use the distro user's state dir so the Windows host can write through UNC
	// without requiring root-owned /var/lib permissions.
	script := `dir="${XDG_STATE_HOME:-$HOME/.local/state}/rmtx"; mkdir -p "$dir"; printf "%s" "$dir"`
	args := wslCommandArgs(distro, "--exec", "sh", "-lc", script)
	out, err := exec.CommandContext(ctx, "wsl.exe", args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf(
			"resolve WSL runtime state dir in %s: %s: %w",
			distro,
			strings.TrimSpace(string(out)),
			err,
		)
	}

	linuxPath := strings.TrimSpace(string(out))
	if linuxPath == "" {
		return "", errors.New("resolve WSL runtime state dir: empty path")
	}
	if !strings.HasPrefix(linuxPath, "/") {
		return "", fmt.Errorf("resolve WSL runtime state dir: non-absolute path %q", linuxPath)
	}

	return path.Clean(linuxPath), nil
}
