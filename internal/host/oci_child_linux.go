//go:build linux

package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const ociChildUsageExit = 2

func RunOCIChild(args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "error: missing OCI child spec")
		return ociChildUsageExit
	}

	content, err := os.ReadFile(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	var spec ociChildSpec
	if err := json.Unmarshal(content, &spec); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	if err := runOCIChild(spec); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	return 1
}

func runOCIChild(spec ociChildSpec) error {
	if err := validateOCIChildSpec(spec); err != nil {
		return err
	}

	rootfs, err := filepath.Abs(spec.RootFS)
	if err != nil {
		return err
	}
	if err := makeMountsPrivate(); err != nil {
		return err
	}
	rootfs, err = prepareOCIOverlayRoot(rootfs, spec.LowerRootFS)
	if err != nil {
		return err
	}

	if err := prepareOCIChildRoot(rootfs, spec.Network, spec.Binds); err != nil {
		return err
	}

	if err := syscall.Chroot(rootfs); err != nil {
		return fmt.Errorf("chroot %s: %w", rootfs, err)
	}

	workdir := strings.TrimSpace(spec.WorkDir)
	if workdir == "" {
		workdir = "/"
	}

	if err := os.Chdir(workdir); err != nil {
		return fmt.Errorf("chdir %s: %w", workdir, err)
	}

	command, err := resolveOCICommandPath(spec.Command[0], spec.Env)
	if err != nil {
		return err
	}

	return syscall.Exec(command, spec.Command, spec.Env)
}

func validateOCIChildSpec(spec ociChildSpec) error {
	if spec.RootFS == "" {
		return errors.New("rootfs is required")
	}

	if len(spec.Command) == 0 || strings.TrimSpace(spec.Command[0]) == "" {
		return errors.New("command is required")
	}

	return nil
}

func prepareOCIChildRoot(rootfs string, network string, binds []ociBind) error {
	if err := mountRuntimeFilesystems(rootfs); err != nil {
		return err
	}

	if err := mountHostNetworkFiles(rootfs, network); err != nil {
		return err
	}

	for _, bind := range binds {
		if err := mountBind(rootfs, bind); err != nil {
			return err
		}
	}

	return nil
}

func makeMountsPrivate() error {
	if err := syscall.Mount("", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, ""); err != nil {
		return fmt.Errorf("make mounts private: %w", err)
	}

	return nil
}

func prepareOCIOverlayRoot(rootfs, lowerRootfs string) (string, error) {
	if strings.TrimSpace(lowerRootfs) == "" {
		return rootfs, nil
	}

	lowerRootfs, err := filepath.Abs(lowerRootfs)
	if err != nil {
		return "", err
	}
	if err := validateOverlayMountPath(rootfs); err != nil {
		return "", err
	}
	if err := validateOverlayMountPath(lowerRootfs); err != nil {
		return "", err
	}

	upper := filepath.Join(rootfs, "upper")
	work := filepath.Join(rootfs, "work")
	merged := filepath.Join(rootfs, "merged")
	for _, dir := range []string{upper, work, merged} {
		if err := os.MkdirAll(dir, defaultDirMode); err != nil {
			return "", err
		}
	}

	options := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lowerRootfs, upper, work)
	if err := syscall.Mount("overlay", merged, "overlay", 0, options); err != nil {
		return "", fmt.Errorf("mount overlay rootfs: %w", err)
	}

	return merged, nil
}

func validateOverlayMountPath(path string) error {
	if strings.ContainsAny(path, ",\x00") {
		return fmt.Errorf("overlay rootfs path cannot contain comma or NUL: %q", path)
	}

	return nil
}

func mountHostNetworkFiles(rootfs string, network string) error {
	if strings.EqualFold(strings.TrimSpace(network), noneValue) {
		return nil
	}

	for _, source := range []string{"/etc/resolv.conf", "/etc/hosts", "/etc/hostname"} {
		if _, err := os.Stat(source); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return fmt.Errorf("stat host network file %s: %w", source, err)
		}

		target, err := childTarget(rootfs, source)
		if err != nil {
			return err
		}

		if err := bindFileReplacingTargetSymlink(source, target, true); err != nil {
			return fmt.Errorf("bind host network file %s: %w", source, err)
		}
	}

	return nil
}

func resolveOCICommandPath(command string, env []string) (string, error) {
	if strings.Contains(command, "/") {
		return command, nil
	}

	for _, dir := range strings.Split(ociPathEnv(env), ":") {
		if dir == "" {
			dir = "."
		}

		candidate := filepath.Join(dir, command)

		info, err := os.Stat(candidate)
		if err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
			continue
		}

		return candidate, nil
	}

	return "", fmt.Errorf("executable %s not found in OCI PATH", command)
}

func ociPathEnv(env []string) string {
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok && key == "PATH" {
			return value
		}
	}

	return "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
}

func mountRuntimeFilesystems(rootfs string) error {
	proc, err := childMountDir(rootfs, "/proc")
	if err != nil {
		return err
	}

	if err := syscall.Mount("proc", proc, "proc", 0, ""); err != nil {
		return fmt.Errorf("mount proc: %w", err)
	}

	tmp, err := childMountDir(rootfs, "/tmp")
	if err != nil {
		return err
	}

	if err := syscall.Mount(
		"tmpfs",
		tmp,
		"tmpfs",
		0,
		"mode=1777",
	); err != nil {
		return fmt.Errorf("mount tmpfs: %w", err)
	}

	_, err = childMountDir(rootfs, "/dev")
	if err != nil {
		return err
	}

	for _, name := range []string{"null", "zero", "random", "urandom"} {
		source := filepath.Join("/dev", name)

		target, err := childTarget(rootfs, "/dev/"+name)
		if err != nil {
			return err
		}
		if err := rejectChildTargetSymlink(target); err != nil {
			return err
		}
		if err := bindFile(source, target, false); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	return nil
}

func childMountDir(rootfs, name string) (string, error) {
	target, err := childTarget(rootfs, name)
	if err != nil {
		return "", err
	}
	if err := rejectChildTargetSymlink(target); err != nil {
		return "", err
	}
	if err := os.MkdirAll(target, defaultDirMode); err != nil {
		return "", err
	}

	return target, nil
}

func mountBind(rootfs string, bind ociBind) error {
	if bind.Source == "" || bind.Target == "" {
		return nil
	}

	target, err := childTarget(rootfs, bind.Target)
	if err != nil {
		return err
	}
	if err := rejectChildTargetSymlink(target); err != nil {
		return err
	}

	info, err := os.Stat(bind.Source)
	if err != nil {
		return fmt.Errorf("stat bind source %s: %w", bind.Source, err)
	}

	if info.IsDir() {
		if err := os.MkdirAll(target, defaultDirMode); err != nil {
			return err
		}
	} else if err := os.MkdirAll(filepath.Dir(target), defaultDirMode); err != nil {
		return err
	} else if _, err := os.OpenFile(target, os.O_CREATE, defaultFileMode); err != nil {
		return err
	}

	flags := uintptr(syscall.MS_BIND | syscall.MS_REC)
	if err := syscall.Mount(bind.Source, target, "", flags, ""); err != nil {
		return fmt.Errorf("bind mount %s to %s: %w", bind.Source, bind.Target, err)
	}

	if bind.ReadOnly {
		if err := syscall.Mount(
			"",
			target,
			"",
			flags|syscall.MS_REMOUNT|syscall.MS_RDONLY,
			"",
		); err != nil {
			return fmt.Errorf("remount bind read-only %s: %w", bind.Target, err)
		}
	}

	return nil
}

func bindFile(source, target string, readOnly bool) error {
	if _, err := os.Stat(source); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(target), defaultDirMode); err != nil {
		return err
	}

	f, err := os.OpenFile(target, os.O_CREATE, defaultFileMode)
	if err != nil {
		return err
	}

	_ = f.Close()

	flags := uintptr(syscall.MS_BIND)
	if err := syscall.Mount(source, target, "", flags, ""); err != nil {
		return err
	}

	if readOnly {
		return syscall.Mount("", target, "", flags|syscall.MS_REMOUNT|syscall.MS_RDONLY, "")
	}

	return nil
}

func bindFileReplacingTargetSymlink(source, target string, readOnly bool) error {
	if info, err := os.Lstat(target); err == nil && info.Mode()&os.ModeSymlink != 0 {
		if err := os.Remove(target); err != nil {
			return err
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return bindFile(source, target, readOnly)
}

func childTarget(rootfs, target string) (string, error) {
	if !strings.HasPrefix(target, "/") || strings.Contains(target, "\x00") {
		return "", fmt.Errorf("invalid bind target %q", target)
	}

	clean := filepath.Clean(strings.TrimPrefix(target, "/"))
	if clean == "." {
		return rootfs, nil
	}

	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("bind target escapes root: %q", target)
	}

	joined := filepath.Join(rootfs, clean)
	if err := rejectChildSymlinkParents(rootfs, joined); err != nil {
		return "", err
	}

	return joined, nil
}

func rejectChildSymlinkParents(rootfs, target string) error {
	for parent := filepath.Dir(target); parent != rootfs; parent = filepath.Dir(parent) {
		if parent == filepath.Dir(parent) {
			return fmt.Errorf("bind target escapes rootfs: %s", target)
		}

		info, err := os.Lstat(parent)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("bind target has symlink parent: %s", parent)
		}
	}

	return nil
}

func rejectChildTargetSymlink(target string) error {
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("bind target is a symlink: %s", target)
	}

	return nil
}
