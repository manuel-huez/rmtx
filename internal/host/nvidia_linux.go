//go:build linux

package host

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

const nvidiaCapabilities = "compute,utility"

var nvidiaLibraryNames = []string{
	"libcuda.so",
	"libcuda.so.1",
	"libnvidia-cfg.so",
	"libnvidia-compiler.so",
	"libnvidia-fatbinaryloader.so",
	"libnvidia-ml.so",
	"libnvidia-opencl.so",
	"libnvidia-ptxjitcompiler.so",
}

func nvidiaRuntime(mode string) (nvidiaRuntimeSpec, error) {
	if !strings.EqualFold(strings.TrimSpace(mode), "nvidia") {
		return nvidiaRuntimeSpec{}, nil
	}

	var spec nvidiaRuntimeSpec

	devices, err := nvidiaDeviceBinds()
	if err != nil {
		return nvidiaRuntimeSpec{}, err
	}

	spec.Binds = append(spec.Binds, devices...)

	libs, libDirs := nvidiaLibraryBinds()
	if len(libDirs) == 0 {
		return nvidiaRuntimeSpec{}, errors.New(
			"NVIDIA CUDA requested but no NVIDIA driver libraries were found",
		)
	}

	spec.Binds = append(spec.Binds, libs...)

	tools := nvidiaToolBinds()
	spec.Binds = append(spec.Binds, tools...)

	spec.Env = append(spec.Env,
		"NVIDIA_VISIBLE_DEVICES=all",
		"NVIDIA_DRIVER_CAPABILITIES="+nvidiaCapabilities,
	)

	if len(libDirs) > 0 {
		spec.Env = append(spec.Env, "LD_LIBRARY_PATH="+strings.Join(libDirs, ":"))
	}

	return spec, nil
}

type nvidiaRuntimeSpec struct {
	Binds        []ociBind
	Env          []string
	PathPrefixes []string
}

func nvidiaDeviceBinds() ([]ociBind, error) {
	var binds []ociBind

	for _, pattern := range []string{
		"/dev/nvidia*",
		"/dev/dxg",
	} {
		matches, _ := filepath.Glob(pattern)
		for _, match := range matches {
			if _, err := os.Stat(match); err == nil {
				binds = appendUniqueBind(binds, ociBind{Source: match, Target: match})
			}
		}
	}

	if len(binds) == 0 {
		return nil, errors.New("NVIDIA CUDA requested but no NVIDIA/WSL GPU devices were found")
	}

	return binds, nil
}

func nvidiaLibraryBinds() ([]ociBind, []string) {
	paths := append(nvidiaLdconfigLibraries(), nvidiaWellKnownLibraries()...)

	var (
		binds []ociBind
		dirs  []string
	)

	for _, lib := range paths {
		if _, err := os.Stat(lib); err != nil {
			continue
		}

		binds = appendUniqueBind(binds, ociBind{
			Source:   lib,
			Target:   lib,
			ReadOnly: true,
		})

		dir := filepath.Dir(lib)
		if !slices.Contains(dirs, dir) {
			dirs = append(dirs, dir)
		}
	}

	if _, err := os.Stat("/usr/lib/wsl/lib"); err == nil {
		binds = appendUniqueBind(binds, ociBind{
			Source:   "/usr/lib/wsl/lib",
			Target:   "/usr/lib/wsl/lib",
			ReadOnly: true,
		})

		if !slices.Contains(dirs, "/usr/lib/wsl/lib") {
			dirs = append(dirs, "/usr/lib/wsl/lib")
		}
	}

	return binds, dirs
}

func nvidiaToolBinds() []ociBind {
	var binds []ociBind

	for _, tool := range []string{"/usr/bin/nvidia-smi", "/usr/lib/wsl/lib/nvidia-smi"} {
		if _, err := os.Stat(tool); err == nil {
			binds = appendUniqueBind(binds, ociBind{
				Source:   tool,
				Target:   tool,
				ReadOnly: true,
			})
		}
	}

	return binds
}

func nvidiaLdconfigLibraries() []string {
	out, err := exec.Command("ldconfig", "-p").Output()
	if err != nil {
		return nil
	}

	var paths []string

	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		if !containsAnyLibrary(line, nvidiaLibraryNames) {
			continue
		}

		_, path, ok := strings.Cut(line, "=>")
		if ok {
			paths = append(paths, strings.TrimSpace(path))
		}
	}

	return paths
}

func nvidiaWellKnownLibraries() []string {
	var paths []string

	for _, dir := range []string{
		"/usr/lib/wsl/lib",
		"/usr/lib/x86_64-linux-gnu",
		"/usr/lib/aarch64-linux-gnu",
		"/usr/lib64",
		"/usr/local/cuda/compat",
	} {
		for _, name := range nvidiaLibraryNames {
			matches, _ := filepath.Glob(filepath.Join(dir, name+"*"))
			paths = append(paths, matches...)
		}
	}

	return paths
}

func containsAnyLibrary(line string, names []string) bool {
	for _, name := range names {
		if strings.Contains(line, name) {
			return true
		}
	}

	return false
}

func appendUniqueBind(binds []ociBind, bind ociBind) []ociBind {
	if bind.Source == "" || bind.Target == "" {
		return binds
	}

	if slices.ContainsFunc(binds, func(existing ociBind) bool {
		return existing.Source == bind.Source && existing.Target == bind.Target
	}) {
		return binds
	}

	return append(binds, bind)
}

func nvidiaUnavailableError(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf(
		"%w; install NVIDIA drivers with CUDA/WSL support or set runtime.gpu to \"none\"",
		err,
	)
}
