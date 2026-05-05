package syncfs

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

type BlobStore struct{ root string }

const (
	hashPrefixLen       = 2
	blobDefaultFileMode = 0o644
	blobDefaultDirPerm  = 0o755
	materializeTempGlob = ".rmtx-tmp-*"
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

func (s *BlobStore) Materialize(hash, dest string, mode fs.FileMode, modTime int64) error {
	return s.MaterializeWithProgress(hash, dest, mode, modTime, nil)
}

func (s *BlobStore) MaterializeWithProgress(
	hash,
	dest string,
	mode fs.FileMode,
	modTime int64,
	onWrite func(int),
) error {
	src := s.Path(hash)

	if err := os.MkdirAll(filepath.Dir(dest), blobDefaultDirPerm); err != nil {
		return fmt.Errorf("create destination dir: %w", err)
	}

	if modTime == 0 {
		if err := os.Link(src, dest); err == nil {
			return os.Chmod(dest, mode)
		}
	}

	if cloned, err := cloneBlobToFile(src, dest, mode, modTime); err != nil {
		return err
	} else if cloned {
		reportLogicalWrite(src, onWrite)

		return nil
	}

	return copyBlobToFile(src, dest, mode, modTime, onWrite)
}

func cloneBlobToFile(src, dest string, mode fs.FileMode, modTime int64) (bool, error) {
	tmp, err := materializeTempPath(dest)
	if err != nil {
		return false, err
	}

	cloned, err := cloneFile(src, tmp, mode)
	if err != nil || !cloned {
		_ = os.Remove(tmp)

		return cloned, err
	}

	if err := replaceMaterializedFile(tmp, dest, mode, modTime); err != nil {
		_ = os.Remove(tmp)

		return true, err
	}

	return true, nil
}

func copyBlobToFile(
	src,
	dest string,
	mode fs.FileMode,
	modTime int64,
	onWrite func(int),
) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open blob %s: %w", filepath.Base(src), err)
	}

	defer func() { _ = in.Close() }()

	out, err := os.CreateTemp(filepath.Dir(dest), materializeTempGlob)
	if err != nil {
		return fmt.Errorf("create destination file: %w", err)
	}
	tmp := out.Name()

	srcReader := io.Reader(in)
	if onWrite != nil {
		srcReader = progressReader{src: in, onRead: onWrite}
	}

	if _, err := io.Copy(out, srcReader); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)

		return fmt.Errorf("copy blob: %w", err)
	}

	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close destination file: %w", err)
	}

	return replaceMaterializedFile(tmp, dest, mode, modTime)
}

func materializeTempPath(dest string) (string, error) {
	tmp, err := os.CreateTemp(filepath.Dir(dest), materializeTempGlob)
	if err != nil {
		return "", fmt.Errorf("create destination file: %w", err)
	}

	name := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return "", fmt.Errorf("close destination file: %w", err)
	}

	if err := os.Remove(name); err != nil {
		return "", fmt.Errorf("remove destination temp placeholder: %w", err)
	}

	return name, nil
}

func replaceMaterializedFile(tmp, dest string, mode fs.FileMode, modTime int64) error {
	if err := os.Chmod(tmp, mode); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	_ = os.RemoveAll(dest)
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename destination file: %w", err)
	}

	return setFileModTime(dest, modTime)
}

func reportLogicalWrite(src string, onWrite func(int)) {
	if onWrite == nil {
		return
	}

	info, err := os.Stat(src)
	if err != nil {
		return
	}

	remaining := info.Size()
	maxChunk := int64(int(^uint(0) >> 1))
	for remaining > 0 {
		chunk := remaining
		if chunk > maxChunk {
			chunk = maxChunk
		}

		onWrite(int(chunk))
		remaining -= chunk
	}
}

type progressReader struct {
	src    io.Reader
	onRead func(int)
}

func (r progressReader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 && r.onRead != nil {
		r.onRead(n)
	}

	return n, err
}

func setFileModTime(path string, modTime int64) error {
	if modTime == 0 {
		return nil
	}

	t := time.Unix(0, modTime)

	return os.Chtimes(path, t, t)
}
