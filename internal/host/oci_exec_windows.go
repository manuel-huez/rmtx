//go:build windows

package host

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"unicode"
)

const wslDistroEnv = "RMTX_WSL_DISTRO"

func (s *Server) ociChildCommand(
	ctx context.Context,
	spec ociChildSpec,
	contextDir string,
) (*exec.Cmd, commandCleanup, error) {
	if len(spec.Command) == 0 {
		return nil, noopCommandCleanup, errors.New("OCI command is required")
	}

	if err := checkWSLAvailable(ctx); err != nil {
		return nil, noopCommandCleanup, err
	}

	spec, err := wslChildSpec(ctx, spec)
	if err != nil {
		return nil, noopCommandCleanup, err
	}

	script, err := writeWSLChildScript(contextDir, spec)
	if err != nil {
		return nil, noopCommandCleanup, err
	}

	wslScript, err := wslPath(ctx, script)
	if err != nil {
		_ = os.Remove(script)
		return nil, noopCommandCleanup, err
	}

	args := wslCommandArgs("--user", "root", "--exec", "sh", wslScript)
	cmd := exec.CommandContext(ctx, "wsl.exe", args...)
	cmd.Env = os.Environ()

	return cmd, cleanupTempFile(script), nil
}

func nvidiaRuntime(mode string) (nvidiaRuntimeSpec, error) {
	if strings.EqualFold(strings.TrimSpace(mode), "nvidia") {
		return nvidiaRuntimeSpec{
			Env: []string{
				"NVIDIA_VISIBLE_DEVICES=all",
				"NVIDIA_DRIVER_CAPABILITIES=compute,utility",
				"LD_LIBRARY_PATH=/usr/lib/wsl/lib",
			},
			Binds: []ociBind{
				{Source: "/dev/dxg", Target: "/dev/dxg"},
				{Source: "/usr/lib/wsl/lib", Target: "/usr/lib/wsl/lib", ReadOnly: true},
			},
		}, nil
	}

	return nvidiaRuntimeSpec{}, nil
}

type nvidiaRuntimeSpec struct {
	Binds []ociBind
	Env   []string
}

func nvidiaUnavailableError(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf(
		"%w; install NVIDIA Windows driver with WSL CUDA support or set runtime.gpu to \"none\"",
		err,
	)
}

func checkWSLAvailable(ctx context.Context) error {
	if err := exec.CommandContext(ctx, "wsl.exe", "--status").Run(); err != nil {
		return fmt.Errorf("WSL2 is required for OCI runtime on Windows: %w", err)
	}

	return nil
}

func wslChildSpec(ctx context.Context, spec ociChildSpec) (ociChildSpec, error) {
	rootfs, err := windowsPathToWSL(ctx, spec.RootFS)
	if err != nil {
		return ociChildSpec{}, err
	}

	spec.RootFS = rootfs

	for i, bind := range spec.Binds {
		source, err := windowsPathToWSL(ctx, bind.Source)
		if err != nil {
			return ociChildSpec{}, err
		}

		spec.Binds[i].Source = source
	}

	return spec, nil
}

func windowsPathToWSL(ctx context.Context, path string) (string, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" || strings.HasPrefix(trimmed, "/") {
		return trimmed, nil
	}

	if len(trimmed) >= 2 &&
		trimmed[1] == ':' &&
		((trimmed[0] >= 'A' && trimmed[0] <= 'Z') || (trimmed[0] >= 'a' && trimmed[0] <= 'z')) {
		drive := unicode.ToLower(rune(trimmed[0]))
		rest := filepath.ToSlash(trimmed[2:])
		return "/mnt/" + string(drive) + "/" + strings.TrimPrefix(rest, "/"), nil
	}

	if strings.HasPrefix(trimmed, `\\`) {
		return trimmed, nil
	}

	if strings.HasPrefix(trimmed, `//`) {
		return filepath.ToSlash(trimmed), nil
	}

	return wslPath(ctx, trimmed)
}

func wslPath(ctx context.Context, path string) (string, error) {
	args := wslCommandArgs("--exec", "wslpath", "-a", path)

	out, err := exec.CommandContext(ctx, "wsl.exe", args...).Output()
	if err != nil {
		return "", fmt.Errorf("convert Windows path to WSL path: %w", err)
	}

	return strings.TrimSpace(string(out)), nil
}

func writeWSLChildScript(contextDir string, spec ociChildSpec) (string, error) {
	specDir := filepath.Join(contextDir, runtimeDirName, runtimeSpecDirName)
	if err := os.MkdirAll(specDir, defaultDirMode); err != nil {
		return "", err
	}

	script, err := os.CreateTemp(specDir, "wsl-run-*.sh")
	if err != nil {
		return "", err
	}

	content, err := wslChildScript(spec)
	if err != nil {
		_ = script.Close()
		_ = os.Remove(script.Name())

		return "", err
	}

	if _, err := script.WriteString(content); err != nil {
		_ = script.Close()
		_ = os.Remove(script.Name())

		return "", err
	}

	if err := script.Close(); err != nil {
		_ = os.Remove(script.Name())

		return "", err
	}

	return script.Name(), nil
}

func wslCommandArgs(args ...string) []string {
	distro := strings.TrimSpace(os.Getenv(wslDistroEnv))
	if distro == "" {
		return args
	}

	out := []string{"--distribution", distro}

	return append(out, args...)
}

func wslChildScript(spec ociChildSpec) (string, error) {
	workdir, err := cleanWSLChildTarget(spec.WorkDir)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString("#!/bin/sh\nset -eu\n")
	b.WriteString("if [ \"${1:-}\" != inner ]; then\n")
	b.WriteString("  if command -v unshare >/dev/null 2>&1; then\n")

	if strings.EqualFold(strings.TrimSpace(spec.Network), "none") {
		b.WriteString("    exec unshare -m -n --fork \"$0\" inner\n")
		b.WriteString("  fi\n")
		b.WriteString(
			"  echo 'error: runtime.network=none requires unshare with network namespace support' >&2\n",
		)
		b.WriteString("  exit 1\n")
	} else {
		b.WriteString("    exec unshare -m --fork \"$0\" inner\n")
		b.WriteString("  fi\n")
		b.WriteString(
			"  echo 'error: OCI runtime on WSL requires unshare with mount namespace support' >&2\n",
		)
		b.WriteString("  exit 1\n")
	}

	b.WriteString("fi\n")
	b.WriteString("mount --make-rprivate / 2>/dev/null || true\n")
	b.WriteString("rootfs=" + shellQuote(spec.RootFS) + "\n")

	if strings.EqualFold(strings.TrimSpace(spec.GPU), "nvidia") {
		b.WriteString(
			"if [ ! -e /dev/dxg ]; then echo 'error: NVIDIA CUDA requested but WSL GPU device /dev/dxg was not found' >&2; exit 1; fi\n",
		)
		b.WriteString(
			"if [ ! -d /usr/lib/wsl/lib ]; then echo 'error: NVIDIA CUDA requested but WSL driver libraries were not found' >&2; exit 1; fi\n",
		)
	}

	b.WriteString("mkdir -p \"$rootfs/proc\" \"$rootfs/tmp\" \"$rootfs/dev\"\n")
	b.WriteString("mount -t proc proc \"$rootfs/proc\" 2>/dev/null || true\n")
	b.WriteString("mount -t tmpfs -o mode=1777 tmpfs \"$rootfs/tmp\" 2>/dev/null || true\n")

	for _, dev := range []string{"/dev/null", "/dev/zero", "/dev/random", "/dev/urandom"} {
		b.WriteString("touch \"$rootfs" + dev + "\" 2>/dev/null || true\n")
		b.WriteString(
			"mount --bind " + shellQuote(dev) + " \"$rootfs" + dev + "\" 2>/dev/null || true\n",
		)
	}

	for _, bind := range spec.Binds {
		snippet, err := wslBindSnippet(bind)
		if err != nil {
			return "", err
		}

		b.WriteString(snippet)
	}

	b.WriteString("cd \"$rootfs\"\n")
	b.WriteString("exec chroot \"$rootfs\" ")
	b.WriteString("env")

	for _, env := range spec.Env {
		b.WriteByte(' ')
		b.WriteString(shellQuote(env))
	}

	b.WriteString(" sh -lc ")
	b.WriteString(
		shellQuote("cd " + shellQuote(workdir) + " && exec " + shellJoin(spec.Command)),
	)
	b.WriteByte('\n')

	return b.String(), nil
}

func wslBindSnippet(bind ociBind) (string, error) {
	if bind.Source == "" || bind.Target == "" {
		return "", nil
	}

	targetPath, err := cleanWSLChildTarget(bind.Target)
	if err != nil {
		return "", err
	}

	var b strings.Builder

	source := shellQuote(bind.Source)
	target := `"$rootfs"` + shellQuote(targetPath)
	b.WriteString("target=" + target + "\n")
	b.WriteString(
		"if [ -d " + source + " ]; then mkdir -p \"$target\"; else mkdir -p \"$(dirname \"$target\")\"; touch \"$target\" 2>/dev/null || true; fi\n",
	)
	b.WriteString("mount --bind " + source + " \"$target\"\n")

	if bind.ReadOnly {
		b.WriteString("mount -o remount,bind,ro \"$target\" 2>/dev/null || true\n")
	}

	return b.String(), nil
}

func cleanWSLChildTarget(target string) (string, error) {
	if strings.TrimSpace(target) == "" {
		return "", errors.New("OCI child target is required")
	}

	if !strings.HasPrefix(target, "/") || strings.Contains(target, "\x00") {
		return "", fmt.Errorf("invalid OCI child target %q", target)
	}

	if slices.Contains(strings.Split(target, "/"), "..") {
		return "", fmt.Errorf("OCI child target escapes root: %q", target)
	}

	return path.Clean(target), nil
}

func shellJoin(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, shellQuote(arg))
	}

	return strings.Join(quoted, " ")
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}

	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
