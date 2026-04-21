package syncfs

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

type BlobStore struct{ root string }

const (
	hashPrefixLen       = 2
	blobDefaultFileMode = 0o644
	blobDefaultDirPerm  = 0o755
)

func NewBlobStore(root string) *BlobStore { return &BlobStore{root: root} }

func (s *BlobStore) Ensure() error {
	return os.MkdirAll(s.root, blobDefaultDirPerm)
}

func (s *BlobStore) Path(hash string) string {
	if len(hash) < hashPrefixLen {
		return filepath.Join(s.root, hash)
	}

	return filepath.Join(s.root, hash[:hashPrefixLen], hash)
}

func (s *BlobStore) Has(hash string) bool {
	_, err := os.Stat(s.Path(hash))
	return err == nil
}

func (s *BlobStore) MissingHashes(entries []Entry) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)

	for _, entry := range entries {
		if entry.Kind != KindFile || entry.Hash == "" {
			continue
		}

		if _, ok := seen[entry.Hash]; ok {
			continue
		}

		seen[entry.Hash] = struct{}{}
		if !s.Has(entry.Hash) {
			out = append(out, entry.Hash)
		}
	}

	return out
}

func (s *BlobStore) Store(hash string, size int64, src io.Reader) error {
	path := s.Path(hash)
	if err := os.MkdirAll(filepath.Dir(path), blobDefaultDirPerm); err != nil {
		return fmt.Errorf("create blob dir: %w", err)
	}

	if _, err := os.Stat(path); err == nil {
		_, copyErr := io.CopyN(io.Discard, src, size)
		return copyErr
	}

	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, blobDefaultFileMode)
	if err != nil {
		return fmt.Errorf("create blob file: %w", err)
	}

	if _, err := io.CopyN(f, src, size); err != nil {
		_ = f.Close()

		_ = os.Remove(tmp)

		return fmt.Errorf("store blob payload: %w", err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close blob file: %w", err)
	}

	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("move blob file: %w", err)
	}

	return nil
}

func (s *BlobStore) Materialize(hash, dest string, mode fs.FileMode) error {
	src := s.Path(hash)

	if err := os.MkdirAll(filepath.Dir(dest), blobDefaultDirPerm); err != nil {
		return fmt.Errorf("create destination dir: %w", err)
	}

	if err := os.Link(src, dest); err == nil {
		return os.Chmod(dest, mode)
	}

	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open blob %s: %w", hash, err)
	}

	defer func() { _ = in.Close() }()

	out, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("create destination file: %w", err)
	}

	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("copy blob: %w", err)
	}

	if err := out.Close(); err != nil {
		return fmt.Errorf("close destination file: %w", err)
	}

	return os.Chmod(dest, mode)
}
