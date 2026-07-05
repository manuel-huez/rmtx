package host

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/manuel-huez/rmtx/internal/pathutil"
)

const rootFSOverlayMarker = ".rmtx-overlay-rootfs-ready"

func contextRootFSPath(contextDir, key string) string {
	return filepath.Join(contextDir, runtimeDirName, runtimeRootFSDirName, key)
}

func sharedRootFSRoot(runtimeRoot string) string {
	return filepath.Join(runtimeRoot, "cache", runtimeRootFSDirName)
}

func sharedRootFSPath(runtimeRoot, key string) string {
	return filepath.Join(sharedRootFSRoot(runtimeRoot), key)
}

func ensureOverlayRootFSBundle(rootfs, key, baseRootFS string) error {
	marker := filepath.Join(rootfs, rootFSOverlayMarker)
	if content, err := os.ReadFile(marker); err == nil {
		if strings.TrimSpace(string(content)) == overlayRootFSMarkerContent(key, baseRootFS) {
			return ensureRootFSInstanceMarker(rootfs)
		}

		if err := pathutil.RemoveAll(rootfs); err != nil {
			return fmt.Errorf("replace stale overlay rootfs %s: %w", rootfs, err)
		}
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read overlay rootfs marker: %w", err)
	}

	if err := pathutil.RemoveAll(rootfs); err != nil {
		return fmt.Errorf("delete incomplete overlay rootfs %s: %w", rootfs, err)
	}

	for _, name := range []string{"upper", "work", "merged"} {
		if err := os.MkdirAll(filepath.Join(rootfs, name), defaultDirMode); err != nil {
			return fmt.Errorf("create overlay rootfs %s: %w", name, err)
		}
	}

	if err := writeRootFSInstanceMarker(rootfs); err != nil {
		return err
	}

	return os.WriteFile(marker, []byte(overlayRootFSMarkerContent(key, baseRootFS)+"\n"), contextFileMode)
}

func overlayRootFSMarkerContent(key, baseRootFS string) string {
	return key + "\n" + filepath.Clean(baseRootFS)
}
