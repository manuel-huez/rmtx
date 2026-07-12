package client

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/manuel-huez/rmtx/internal/clientstate"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/syncfs"
)

const (
	sha256HexLength  = 64
	staleManifestAge = time.Hour
)

// PruneLocalCache removes client cache files not referenced by any valid manifest.
func PruneLocalCache() (protocol.CachePruneResponse, error) {
	dir, unlock, err := acquireClientCacheLock()
	if err != nil {
		return protocol.CachePruneResponse{}, err
	}

	result := protocol.CachePruneResponse{}

	result.Deleted, result.Bytes, err = pruneClientBlobCacheInDir(dir, time.Now())
	if unlockErr := unlock(); unlockErr != nil {
		err = errors.Join(err, unlockErr)
	}

	return result, err
}

func acquireClientCacheLock() (string, func() error, error) {
	dir, err := clientstate.DefaultDir()
	if err != nil {
		return "", nil, err
	}

	if err := os.MkdirAll(dir, manifestCacheDirMode); err != nil {
		return "", nil, fmt.Errorf("create client cache dir: %w", err)
	}

	unlock, err := lockClientCache(filepath.Join(dir, "cache.lock"))
	if err != nil {
		return "", nil, err
	}

	return dir, unlock, nil
}

func beginClientCacheUse(logger *runLogger) (func(), error) {
	dir, unlock, err := acquireClientCacheLock()
	if err != nil {
		return nil, err
	}

	return func() {
		if _, _, err := pruneClientBlobCacheInDir(dir, time.Now()); err != nil {
			logger.Printf("local client blob cache cleanup failed: %v", err)
		}

		if err := unlock(); err != nil {
			logger.Printf("local client blob cache unlock failed: %v", err)
		}
	}, nil
}

func openClientCacheLockFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, manifestCacheFileMode)
	if err != nil {
		return nil, fmt.Errorf("open client cache lock: %w", err)
	}

	return file, nil
}

func pruneClientBlobCacheInDir(
	dir string,
	scanStarted time.Time,
) ([]protocol.ContextArtifact, int64, error) {
	manifestDir := filepath.Join(dir, "manifests")

	referenced, staleManifests, err := cachedManifestHashes(manifestDir, scanStarted)
	if err != nil {
		return nil, 0, err
	}

	blobDir := filepath.Join(dir, "blobs")

	candidates, err := unreferencedClientBlobs(blobDir, referenced, scanStarted)
	if err != nil {
		return nil, 0, err
	}

	deleted := make([]protocol.ContextArtifact, 0, len(candidates)+len(staleManifests))

	var bytes int64

	for _, path := range staleManifests {
		artifact, removed, err := removeClientCacheFile(path, "client_manifest", scanStarted)
		if err != nil {
			return deleted, bytes, err
		}

		if removed {
			deleted = append(deleted, artifact)
			bytes += artifact.Size
		}
	}

	for _, path := range candidates {
		artifact, removed, err := removeClientCacheFile(path, "client_blob", scanStarted)
		if err != nil {
			return deleted, bytes, err
		}

		if removed {
			deleted = append(deleted, artifact)
			bytes += artifact.Size
		}
	}

	removeEmptyBlobDirs(blobDir)
	sort.Slice(deleted, func(i, j int) bool { return deleted[i].Path < deleted[j].Path })

	return deleted, bytes, nil
}

func unreferencedClientBlobs(
	dir string,
	referenced map[string]struct{},
	scanStarted time.Time,
) ([]string, error) {
	var candidates []string

	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) && path == dir {
				return fs.SkipDir
			}

			return walkErr
		}

		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}

		if !info.Mode().IsRegular() || !info.ModTime().Before(scanStarted) {
			return nil
		}

		if _, ok := referenced[entry.Name()]; !ok {
			candidates = append(candidates, path)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan client blob cache: %w", err)
	}

	sort.Strings(candidates)

	return candidates, nil
}

func cachedManifestHashes(
	dir string,
	scanStarted time.Time,
) (map[string]struct{}, []string, error) {
	referenced := map[string]struct{}{}

	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return referenced, nil, nil
	}

	if err != nil {
		return nil, nil, fmt.Errorf("read manifest cache dir: %w", err)
	}

	var stale []string

	for _, entry := range entries {
		stalePath, err := collectCachedManifestHashes(
			dir,
			entry,
			scanStarted,
			referenced,
		)
		if err != nil {
			return nil, nil, err
		}

		if stalePath != "" {
			stale = append(stale, stalePath)
		}
	}

	sort.Strings(stale)

	return referenced, stale, nil
}

func collectCachedManifestHashes(
	dir string,
	entry fs.DirEntry,
	scanStarted time.Time,
	referenced map[string]struct{},
) (string, error) {
	if entry.IsDir() {
		return "", nil
	}

	path := filepath.Join(dir, entry.Name())

	info, err := entry.Info()
	if err != nil {
		return "", fmt.Errorf("inspect cached manifest %s: %w", entry.Name(), err)
	}

	staleBefore := scanStarted.Add(-staleManifestAge)
	if strings.HasSuffix(entry.Name(), ".tmp") && info.ModTime().Before(staleBefore) {
		return path, nil
	}

	if !strings.HasSuffix(entry.Name(), ".json") {
		return "", nil
	}

	manifest, err := readCachedManifest(path)
	if err != nil {
		if info.ModTime().Before(staleBefore) {
			return path, nil
		}

		return "", err
	}

	if err := addManifestHashes(manifest, referenced); err != nil {
		if info.ModTime().Before(staleBefore) {
			return path, nil
		}

		return "", fmt.Errorf("cached manifest %s: %w", entry.Name(), err)
	}

	return "", nil
}

func readCachedManifest(path string) ([]syncfs.Entry, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cached manifest %s: %w", filepath.Base(path), err)
	}

	var manifest []syncfs.Entry
	if err := json.Unmarshal(content, &manifest); err != nil {
		return nil, fmt.Errorf("parse recent cached manifest %s: %w", filepath.Base(path), err)
	}

	return manifest, nil
}

func addManifestHashes(manifest []syncfs.Entry, referenced map[string]struct{}) error {
	for _, item := range manifest {
		if item.Kind != syncfs.KindFile || item.Hash == "" {
			continue
		}

		if !validBlobHash(item.Hash) {
			return errors.New("invalid blob hash")
		}

		referenced[item.Hash] = struct{}{}
	}

	return nil
}

func validBlobHash(hash string) bool {
	if len(hash) != sha256HexLength || strings.ToLower(hash) != hash {
		return false
	}

	_, err := hex.DecodeString(hash)

	return err == nil
}

func removeClientCacheFile(
	path string,
	kind string,
	scanStarted time.Time,
) (protocol.ContextArtifact, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return protocol.ContextArtifact{}, false, nil
	}

	if err != nil {
		return protocol.ContextArtifact{}, false, fmt.Errorf(
			"inspect client cache file %s: %w",
			path,
			err,
		)
	}

	if !info.Mode().IsRegular() || !info.ModTime().Before(scanStarted) {
		return protocol.ContextArtifact{}, false, nil
	}

	if err := os.Remove(path); err != nil {
		return protocol.ContextArtifact{}, false, fmt.Errorf(
			"delete client cache file %s: %w",
			path,
			err,
		)
	}

	return protocol.ContextArtifact{
		Kind: kind,
		Name: filepath.Base(path),
		Path: path,
		Size: info.Size(),
	}, true, nil
}

func removeEmptyBlobDirs(root string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			_ = os.Remove(filepath.Join(root, entry.Name()))
		}
	}
}
