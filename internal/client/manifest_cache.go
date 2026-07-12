package client

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/manuel-huez/rmtx/internal/clientstate"
	"github.com/manuel-huez/rmtx/internal/pathutil"
	"github.com/manuel-huez/rmtx/internal/syncfs"
)

const manifestCacheFileMode = 0o600
const manifestCacheDirMode = 0o700

func loadCachedManifest(root, contextID string) ([]syncfs.Entry, error) {
	path, err := manifestCachePath(root, contextID)
	if err != nil {
		return nil, err
	}

	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("read manifest cache: %w", err)
	}

	var entries []syncfs.Entry
	if err := json.Unmarshal(content, &entries); err != nil {
		return nil, fmt.Errorf("parse manifest cache: %w", err)
	}

	return entries, nil
}

func saveCachedManifest(root, contextID string, entries []syncfs.Entry) error {
	path, err := manifestCachePath(root, contextID)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), manifestCacheDirMode); err != nil {
		return fmt.Errorf("create manifest cache dir: %w", err)
	}

	content, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("marshal manifest cache: %w", err)
	}

	if err := pathutil.WriteFileAtomically(
		path,
		append(content, '\n'),
		manifestCacheFileMode,
	); err != nil {
		return fmt.Errorf("write manifest cache: %w", err)
	}

	return nil
}

func manifestCachePath(root, contextID string) (string, error) {
	dir, err := clientstate.DefaultDir()
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256([]byte(contextID + "\x00" + filepath.Clean(root)))
	name := hex.EncodeToString(sum[:]) + ".json"

	return filepath.Join(dir, "manifests", name), nil
}

func clientBlobStore() (*syncfs.BlobStore, error) {
	dir, err := clientstate.DefaultDir()
	if err != nil {
		return nil, err
	}

	store := syncfs.NewBlobStore(filepath.Join(dir, "blobs"))
	if err := store.Ensure(); err != nil {
		return nil, err
	}

	return store, nil
}
