package host

import (
	"errors"
	"os"
	"strings"
)

type commandCleanup func() error

func noopCommandCleanup() error {
	return nil
}

func cleanupTempFile(path string) commandCleanup {
	return func() error {
		if strings.TrimSpace(path) == "" {
			return nil
		}

		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}

		return nil
	}
}

type ociBind struct {
	Source   string `json:"source"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"read_only,omitempty"`
}

type ociChildSpec struct {
	RootFS  string    `json:"rootfs"`
	WorkDir string    `json:"workdir"`
	Command []string  `json:"command"`
	Env     []string  `json:"env"`
	Binds   []ociBind `json:"binds,omitempty"`
	Network string    `json:"network,omitempty"`
	GPU     string    `json:"gpu,omitempty"`
}
