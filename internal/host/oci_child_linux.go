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
	if err := syscall.Mount("", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, ""); err != nil {
		return fmt.Errorf("make mounts private: %w", err)
	}

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
	if err := os.MkdirAll(filepath.Join(rootfs, "proc"), defaultDirMode); err != nil {
		return err
	}

	if err := syscall.Mount("proc", filepath.Join(rootfs, "proc"), "proc", 0, ""); err != nil {
		return fmt.Errorf("mount proc: %w", err)
	}

	if err := os.MkdirAll(filepath.Join(rootfs, "tmp"), defaultDirMode); err != nil {
		return err
	}

	if err := syscall.Mount(
		"tmpfs",
		filepath.Join(rootfs, "tmp"),
		"tmpfs",
		0,
		"mode=1777",
	); err != nil {
		return fmt.Errorf("mount tmpfs: %w", err)
	}

	dev := filepath.Join(rootfs, "dev")
	if err := os.MkdirAll(dev, defaultDirMode); err != nil {
		return err
	}

	for _, name := range []string{"null", "zero", "random", "urandom"} {
		source := filepath.Join("/dev", name)

		target := filepath.Join(dev, name)
		if err := bindFile(source, target, false); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	return nil
}

func mountBind(rootfs string, bind ociBind) error {
	if bind.Source == "" || bind.Target == "" {
		return nil
	}

	target, err := childTarget(rootfs, bind.Target)
	if err != nil {
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

	return filepath.Join(rootfs, clean), nil
}
