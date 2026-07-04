//go:build windows

package pathutil

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// Symlink creates a symbolic link, using WSL when the link path is inside WSL UNC storage.
func Symlink(oldname, newname string) error {
	link, ok, err := ParseWSLUNCPath(newname)
	if err != nil {
		return err
	}
	if !ok {
		return os.Symlink(oldname, newname)
	}

	return runWSLLink(link.Distro, "-s", oldname, link.LinuxPath)
}

// Link creates a hard link, using WSL when the link path is inside WSL UNC storage.
func Link(oldname, newname string) error {
	link, ok, err := ParseWSLUNCPath(newname)
	if err != nil {
		return err
	}
	if !ok {
		return os.Link(oldname, newname)
	}

	target, targetOK, err := ParseWSLUNCPath(oldname)
	if err != nil {
		return err
	}
	if !targetOK {
		return fmt.Errorf("hard link source %q is not in WSL storage", oldname)
	}
	if !strings.EqualFold(target.Distro, link.Distro) {
		return fmt.Errorf(
			"hard link source %q belongs to distro %q, not %q",
			oldname,
			target.Distro,
			link.Distro,
		)
	}

	return runWSLLink(link.Distro, "", target.LinuxPath, link.LinuxPath)
}

// Chmod changes path mode, using WSL when executable bits matter inside WSL storage.
func Chmod(path string, mode fs.FileMode) error {
	target, ok, err := ParseWSLUNCPath(path)
	if err != nil {
		return err
	}
	if !ok {
		return os.Chmod(path, mode)
	}
	if mode.Perm()&0o111 == 0 {
		return nil
	}

	out, err := exec.Command(
		"wsl.exe",
		"--distribution",
		target.Distro,
		"--exec",
		"chmod",
		strconv.FormatUint(uint64(mode.Perm()), 8),
		"--",
		target.LinuxPath,
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("chmod WSL path in %s: %s: %w", target.Distro, strings.TrimSpace(string(out)), err)
	}

	return nil
}

func runWSLLink(distro, mode, oldname, newname string) error {
	args := []string{"--distribution", distro, "--exec", "ln"}
	if mode != "" {
		args = append(args, mode)
	}
	args = append(args, "--", oldname, newname)

	out, err := exec.Command("wsl.exe", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("create WSL link in %s: %s: %w", distro, strings.TrimSpace(string(out)), err)
	}

	return nil
}
