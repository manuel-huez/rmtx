package pathutil

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const defaultDirMode = 0o755

// WriteFileAtomically replaces path only after data is durable.
func WriteFileAtomically(path string, data []byte, mode fs.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, defaultDirMode); err != nil {
		return fmt.Errorf("create parent dir: %w", err)
	}

	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if err := tmp.Chmod(mode.Perm()); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp file: %w", err)
	}

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := ReplaceFile(tmpPath, path); err != nil {
		return fmt.Errorf("replace file: %w", err)
	}

	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync parent dir: %w", err)
	}

	return nil
}
