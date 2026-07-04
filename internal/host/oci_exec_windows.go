//go:build windows

package host

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"unicode"

	"github.com/manuel-huez/rmtx/internal/pathutil"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"golang.org/x/sys/windows/registry"
)

const wslDistroRegistryKey = `Software\Microsoft\Windows\CurrentVersion\Lxss`

func (s *Server) platformOCIChildCommand(
	ctx context.Context,
	run ociChildCommandRequest,
) (*exec.Cmd, commandCleanup, error) {
	if err := checkWSLAvailable(ctx, s.hostOnlyLogger(), run.spec.WSLDistro, run.runLogs); err != nil {
		return nil, noopCommandCleanup, err
	}

	spec, err := wslChildSpec(ctx, run.spec)
	if err != nil {
		return nil, noopCommandCleanup, err
	}

	script, err := writeWSLChildScript(run.contextDir, spec)
	if err != nil {
		return nil, noopCommandCleanup, err
	}

	wslScript, err := windowsPathToWSL(ctx, spec.WSLDistro, script)
	if err != nil {
		_ = os.Remove(script)
		return nil, noopCommandCleanup, err
	}

	args := wslCommandArgs(spec.WSLDistro, "--user", "root", "--exec", "sh", wslScript)
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

func checkWSLAvailable(
	ctx context.Context,
	logger *log.Logger,
	requestedDistro string,
	runLogs *hostLogSubscription,
) error {
	requestedDistro = strings.TrimSpace(requestedDistro)
	if requestedDistro == "" {
		return errors.New("runtime.wsl_distro is required for OCI runtime on Windows")
	}

	installedDistros, err := wslInstalledDistros()
	if err != nil {
		return fmt.Errorf(
			"WSL2 is required for OCI runtime on Windows: cannot list installed distros: %w",
			err,
		)
	}

	for _, name := range installedDistros {
		if strings.EqualFold(name, requestedDistro) {
			if logger != nil {
				logger.Printf("using already installed WSL distro: %s", requestedDistro)
			}
			writeRunLogLine(runLogs, "using already installed WSL distro: %s", requestedDistro)

			return nil
		}
	}

	if err := installWSLDistro(ctx, logger, requestedDistro, runLogs); err != nil {
		return fmt.Errorf(
			"requested WSL distro %q is not installed and auto-install failed: %w",
			requestedDistro,
			err,
		)
	}

	return nil
}

func wslChildSpec(ctx context.Context, spec ociChildSpec) (ociChildSpec, error) {
	rootfsID, err := readRootFSInstanceID(spec.RootFS)
	if err != nil {
		return ociChildSpec{}, err
	}

	rootfs, err := windowsPathToWSL(ctx, spec.WSLDistro, spec.RootFS)
	if err != nil {
		return ociChildSpec{}, err
	}

	spec.RootFS = rootfs
	if rootfsID != "" && wslRootFSNeedsStage(rootfs) {
		spec.RootFSID = rootfsID
		spec.StagedRootFS = wslStagedRootFSPath(rootfs)
	}

	for i, bind := range spec.Binds {
		source, err := windowsPathToWSL(ctx, spec.WSLDistro, bind.Source)
		if err != nil {
			return ociChildSpec{}, err
		}

		spec.Binds[i].Source = source
	}

	return spec, nil
}

func readRootFSInstanceID(rootfs string) (string, error) {
	content, err := os.ReadFile(filepath.Join(rootfs, rootFSInstanceMarker))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read rootfs instance marker: %w", err)
	}

	return strings.TrimSpace(string(content)), nil
}

func wslStagedRootFSPath(rootfs string) string {
	sum := sha256.Sum256([]byte(rootfs))

	return "/var/lib/rmtx/rootfs/" + hex.EncodeToString(sum[:])
}

func wslRootFSNeedsStage(rootfs string) bool {
	return strings.HasPrefix(rootfs, "/mnt/")
}

func pruneWSLStagedRootFS(
	ctx context.Context,
	contextDataDirs []string,
) ([]protocol.ContextArtifact, int64, error) {
	live, err := liveWSLStagedRootFS(ctx, contextDataDirs)
	if err != nil {
		return nil, 0, err
	}

	distros, err := wslInstalledDistros()
	if err != nil {
		return nil, 0, err
	}

	var deleted []protocol.ContextArtifact
	for _, distro := range distros {
		removed, err := pruneWSLStagedRootFSInDistro(ctx, distro, live[distro])
		if err != nil {
			return deleted, 0, err
		}

		deleted = append(deleted, removed...)
	}

	return deleted, 0, nil
}

func liveWSLStagedRootFS(
	ctx context.Context,
	contextDataDirs []string,
) (map[string]map[string]bool, error) {
	out := map[string]map[string]bool{}
	distros, err := wslInstalledDistros()
	if err != nil {
		return nil, err
	}

	for _, contextDir := range contextDataDirs {
		state, err := loadArtifactState(
			filepath.Join(contextDir, runtimeDirName, artifactStateFile),
		)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				rootfsEntries, readErr := os.ReadDir(
					filepath.Join(contextDir, runtimeDirName, runtimeRootFSDirName),
				)
				if errors.Is(readErr, os.ErrNotExist) {
					continue
				}
				if readErr != nil {
					return nil, fmt.Errorf(
						"list context rootfs for WSL prune context %s: %w",
						filepath.Base(contextDir),
						readErr,
					)
				}

				hasPrepared := false
				for _, rootfsEntry := range rootfsEntries {
					if rootfsEntry.IsDir() {
						hasPrepared = true

						break
					}
				}
				if !hasPrepared {
					continue
				}
			}

			return nil, fmt.Errorf(
				"read artifact state for WSL prune context %s: %w",
				filepath.Base(contextDir),
				err,
			)
		}

		for _, prepared := range state.Prepared {
			if err := addLiveWSLStagedRootFS(ctx, distros, prepared.Path, out); err != nil {
				return nil, err
			}
		}
	}

	return out, nil
}

func addLiveWSLStagedRootFS(
	ctx context.Context,
	distros []string,
	preparedPath string,
	out map[string]map[string]bool,
) error {
	if parsed, ok, err := pathutil.ParseWSLUNCPath(preparedPath); ok || err != nil {
		if err != nil {
			return err
		}
		if wslRootFSNeedsStage(parsed.LinuxPath) {
			if out[parsed.Distro] == nil {
				out[parsed.Distro] = map[string]bool{}
			}
			out[parsed.Distro][wslStagedRootFSPath(parsed.LinuxPath)] = true
		}

		return nil
	}

	for _, distro := range distros {
		rootfs, err := windowsPathToWSL(ctx, distro, preparedPath)
		if err != nil {
			return err
		}

		if wslRootFSNeedsStage(rootfs) {
			if out[distro] == nil {
				out[distro] = map[string]bool{}
			}
			out[distro][wslStagedRootFSPath(rootfs)] = true
		}
	}

	return nil
}

func pruneWSLStagedRootFSInDistro(
	ctx context.Context,
	distro string,
	live map[string]bool,
) ([]protocol.ContextArtifact, error) {
	args := wslCommandArgs(distro, "--user", "root", "--exec", "sh", "-c", wslPruneRootFSScript(), "sh")
	for path := range live {
		args = append(args, path)
	}

	out, err := exec.CommandContext(ctx, "wsl.exe", args...).Output()
	if err != nil {
		return nil, fmt.Errorf("prune WSL staged rootfs in %s: %w", distro, err)
	}

	var deleted []protocol.ContextArtifact
	for _, line := range strings.Split(string(out), "\n") {
		path := strings.TrimSpace(line)
		if path == "" {
			continue
		}

		deleted = append(deleted, protocol.ContextArtifact{
			Kind:   "wsl-rootfs",
			Name:   distro,
			Path:   path,
			Detail: distro,
		})
	}

	return deleted, nil
}

func wslPruneRootFSScript() string {
	return strings.Join([]string{
		"root=/var/lib/rmtx/rootfs",
		"[ -d \"$root\" ] || exit 0",
		"for path in \"$root\"/*; do",
		"  [ -e \"$path\" ] || continue",
		"  name=${path##*/}",
		"  case \"$name\" in *.tmp.*) continue ;; esac",
		"  keep=0",
		"  for live in \"$@\"; do [ \"$path\" = \"$live\" ] && keep=1; done",
		"  [ \"$keep\" = 1 ] && continue",
		"  echo \"$path\"",
		"  rm -rf \"$path\"",
		"done",
	}, "\n")
}

func windowsPathToWSL(ctx context.Context, distro string, path string) (string, error) {
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

	if strings.HasPrefix(trimmed, `\\`) || strings.HasPrefix(trimmed, `//`) {
		if parsed, ok, err := pathutil.ParseWSLUNCPath(trimmed); ok || err != nil {
			if err != nil {
				return "", err
			}
			if !strings.EqualFold(parsed.Distro, strings.TrimSpace(distro)) {
				return "", fmt.Errorf(
					"WSL UNC path %q belongs to distro %q, not %q",
					trimmed,
					parsed.Distro,
					distro,
				)
			}

			return parsed.LinuxPath, nil
		}
		if strings.HasPrefix(trimmed, `\\`) {
			return trimmed, nil
		}
		return filepath.ToSlash(trimmed), nil
	}

	return wslPath(ctx, distro, trimmed)
}

func wslPath(ctx context.Context, distro, path string) (string, error) {
	args := wslCommandArgs(distro, "--exec", "wslpath", "-a", path)

	out, err := exec.CommandContext(ctx, "wsl.exe", args...).Output()
	if err != nil {
		return "", fmt.Errorf("convert Windows path %q to WSL path: %w", path, err)
	}

	return strings.TrimSpace(string(out)), nil
}

func wslInstalledDistros() ([]string, error) {
	key, err := registry.OpenKey(
		registry.CURRENT_USER,
		wslDistroRegistryKey,
		registry.ENUMERATE_SUB_KEYS,
	)
	if err != nil {
		if errors.Is(err, registry.ErrNotExist) {
			return nil, nil
		}

		return nil, fmt.Errorf("open WSL distro registry key: %w", err)
	}

	defer func() { _ = key.Close() }()

	subkeys, err := key.ReadSubKeyNames(-1)
	if err != nil {
		return nil, fmt.Errorf("list WSL distro registry keys: %w", err)
	}

	distros := make([]string, 0, len(subkeys))
	for _, subkey := range subkeys {
		distroKey, err := registry.OpenKey(key, subkey, registry.QUERY_VALUE)
		if err != nil {
			if errors.Is(err, registry.ErrNotExist) {
				continue
			}

			return nil, fmt.Errorf("open WSL distro registry key %q: %w", subkey, err)
		}

		name, _, err := distroKey.GetStringValue("DistributionName")
		_ = distroKey.Close()

		if err != nil {
			if errors.Is(err, registry.ErrNotExist) {
				continue
			}

			return nil, fmt.Errorf("read WSL distro name from registry key %q: %w", subkey, err)
		}

		name = strings.TrimSpace(name)
		if name != "" {
			distros = append(distros, name)
		}
	}

	slices.Sort(distros)

	return distros, nil
}

func installWSLDistro(
	ctx context.Context,
	logger *log.Logger,
	distro string,
	runLogs *hostLogSubscription,
) error {
	if strings.TrimSpace(distro) == "" {
		return errors.New("requested WSL distro is empty")
	}

	if logger != nil {
		logger.Printf("installing WSL distro: %s", distro)
	}
	writeRunLogLine(runLogs, "installing WSL distro: %s", distro)

	cmd := exec.CommandContext(
		ctx,
		"wsl.exe",
		"--install",
		"-d",
		distro,
	)
	cmd.Env = os.Environ()

	out, err := runCommandWithLiveOutput(logger, cmd, "wsl install "+distro, runLogs, runLogs)
	if err != nil {
		return fmt.Errorf(
			"install WSL distro %q: %s: %w",
			distro,
			strings.TrimSpace(string(out)),
			err,
		)
	}

	return nil
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

func wslCommandArgs(distro string, args ...string) []string {
	distro = strings.TrimSpace(distro)
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

	if strings.EqualFold(strings.TrimSpace(spec.Network), noneValue) {
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
	b.WriteString("source_rootfs=" + shellQuote(spec.RootFS) + "\n")
	if strings.TrimSpace(spec.StagedRootFS) != "" && strings.TrimSpace(spec.RootFSID) != "" {
		b.WriteString("rootfs=" + shellQuote(spec.StagedRootFS) + "\n")
		b.WriteString("rootfs_id=" + shellQuote(strings.TrimSpace(spec.RootFSID)) + "\n")
		b.WriteString(wslStageRootFSSnippet())
	} else {
		b.WriteString("rootfs=\"$source_rootfs\"\n")
	}

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

	if !strings.EqualFold(strings.TrimSpace(spec.Network), noneValue) {
		b.WriteString(wslHostNetworkFilesSnippet())
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

func wslStageRootFSSnippet() string {
	return strings.Join([]string{
		"stage_marker=\"$rootfs/.rmtx-rootfs-stage-id\"",
		"source_stage_marker=\"$source_rootfs/.rmtx-wsl-stage-canonical\"",
		"current_stage_id=\"\"",
		"if [ -f \"$stage_marker\" ]; then current_stage_id=$(cat \"$stage_marker\" 2>/dev/null || true); fi",
		"if [ \"$current_stage_id\" != \"$rootfs_id\" ]; then",
		"  if [ -f \"$source_stage_marker\" ]; then echo 'error: staged WSL OCI rootfs is missing or stale; prune the context runtime artifact and rerun' >&2; exit 1; fi",
		"  if ! command -v tar >/dev/null 2>&1; then echo 'error: WSL OCI runtime staging requires tar' >&2; exit 1; fi",
		"  tmp=\"$rootfs.tmp.$$\"",
		"  rm -rf \"$tmp\"",
		"  mkdir -p \"$(dirname \"$rootfs\")\" \"$tmp\"",
		"  if ! (cd \"$source_rootfs\" && tar -cpf - .) | (cd \"$tmp\" && tar -xpf -); then rm -rf \"$tmp\"; exit 1; fi",
		"  printf '%s\\n' \"$rootfs_id\" > \"$tmp/.rmtx-rootfs-stage-id\"",
		"  rm -rf \"$rootfs\"",
		"  mv \"$tmp\" \"$rootfs\"",
		"  printf '%s\\n' \"$rootfs_id\" > \"$source_stage_marker\" 2>/dev/null || true",
		"fi",
		"",
	}, "\n")
}

func wslHostNetworkFilesSnippet() string {
	var b strings.Builder

	for _, file := range []string{"/etc/resolv.conf", "/etc/hosts", "/etc/hostname"} {
		source := shellQuote(file)
		target := `"$rootfs"` + shellQuote(file)
		b.WriteString("if [ -e " + source + " ]; then\n")
		b.WriteString("  target=" + target + "\n")
		b.WriteString("  mkdir -p \"$(dirname \"$target\")\"\n")
		b.WriteString("  if [ -L \"$target\" ]; then rm -f \"$target\"; fi\n")
		b.WriteString("  touch \"$target\" 2>/dev/null || true\n")
		b.WriteString("  mount --bind " + source + " \"$target\"\n")
		b.WriteString("  mount -o remount,bind,ro \"$target\" 2>/dev/null || true\n")
		b.WriteString("fi\n")
	}

	return b.String()
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
