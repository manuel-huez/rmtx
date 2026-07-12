package syncfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/manuel-huez/rmtx/internal/pathutil"
)

type blobStamp struct {
	size    int64
	modTime int64
}

type BlobStore struct {
	root string
	mu   sync.Mutex
	seen map[string]blobStamp
}

const (
	hashPrefixLen       = 2
	blobDefaultFileMode = 0o644
	blobDefaultDirPerm  = 0o755
	materializeTempGlob = ".rmtx-tmp-*"
)

func NewBlobStore(root string) *BlobStore {
	return &BlobStore{root: root, seen: make(map[string]blobStamp)}
}

func (s *BlobStore) Ensure() error {
	return os.MkdirAll(s.root, blobDefaultDirPerm)
}

func (s *BlobStore) Path(hash string) (string, error) {
	if err := validateBlobHash(hash); err != nil {
		return "", err
	}

	if len(hash) < hashPrefixLen {
		return filepath.Join(s.root, hash), nil
	}

	return filepath.Join(s.root, hash[:hashPrefixLen], hash), nil
}

func validateBlobHash(hash string) error {
	if len(hash) != sha256.Size*2 {
		return fmt.Errorf("invalid blob hash %q", hash)
	}

	decoded, err := hex.DecodeString(hash)
	if err != nil || hex.EncodeToString(decoded) != hash {
		return fmt.Errorf("invalid blob hash %q", hash)
	}

	return nil
}

func (s *BlobStore) Has(hash string, size int64) bool {
	_, ok := s.verifiedPath(hash, size)
	return ok
}

//nolint:cyclop // Cache hits still require stable-file and digest checks.
func (s *BlobStore) verifiedPath(hash string, size int64) (string, bool) {
	path, err := s.Path(hash)
	if err != nil {
		return "", false
	}

	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || size >= 0 && info.Size() != size {
		return "", false
	}

	stamp := blobStamp{size: info.Size(), modTime: info.ModTime().UnixNano()}

	s.mu.Lock()
	seen, verified := s.seen[hash]
	s.mu.Unlock()

	if verified && seen == stamp {
		return path, true
	}

	actual, err := HashFile(path)
	if err != nil || actual != hash {
		return "", false
	}

	after, err := os.Stat(path)
	if err != nil || after.Size() != info.Size() || after.ModTime() != info.ModTime() {
		return "", false
	}

	s.mu.Lock()
	s.seen[hash] = stamp
	s.mu.Unlock()

	return path, true
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
		if !s.Has(entry.Hash, entry.Size) {
			out = append(out, entry.Hash)
		}
	}

	return out
}

func (s *BlobStore) Store(hash string, size int64, src io.Reader) error {
	if size < 0 {
		return fmt.Errorf("blob %s has negative size", hash)
	}

	path, err := s.Path(hash)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), blobDefaultDirPerm); err != nil {
		return fmt.Errorf("create blob dir: %w", err)
	}

	if s.Has(hash, size) {
		_, copyErr := io.CopyN(io.Discard, src, size)
		return copyErr
	}

	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create blob file: %w", err)
	}

	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()

	if err := f.Chmod(blobDefaultFileMode); err != nil {
		_ = f.Close()
		return fmt.Errorf("chmod blob file: %w", err)
	}

	digest := sha256.New()
	if _, err := io.CopyN(io.MultiWriter(f, digest), src, size); err != nil {
		_ = f.Close()
		return fmt.Errorf("store blob payload: %w", err)
	}

	if actual := hex.EncodeToString(digest.Sum(nil)); actual != hash {
		_ = f.Close()
		return fmt.Errorf("blob hash mismatch: got %s want %s", actual, hash)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync blob file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close blob file: %w", err)
	}

	if err := pathutil.ReplaceFile(tmp, path); err != nil {
		return fmt.Errorf("move blob file: %w", err)
	}

	s.remember(hash, size, path)

	return nil
}

//nolint:cyclop // Clone validation and portable-copy fallback share one atomic store path.
func (s *BlobStore) StorePath(hash string, size int64, src string) error {
	if size < 0 {
		return fmt.Errorf("blob %s has negative size", hash)
	}

	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("stat blob source: %w", err)
	}

	if !info.Mode().IsRegular() || info.Size() != size {
		return fmt.Errorf("blob source size %d does not match %d", info.Size(), size)
	}

	path, err := s.Path(hash)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), blobDefaultDirPerm); err != nil {
		return fmt.Errorf("create blob dir: %w", err)
	}

	if s.Has(hash, size) {
		return nil
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create blob temp: %w", err)
	}

	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close blob temp: %w", err)
	}

	if err := os.Remove(tmpPath); err != nil {
		return fmt.Errorf("remove blob temp placeholder: %w", err)
	}

	mode := fs.FileMode(blobDefaultFileMode)

	cloned, err := cloneFile(src, tmpPath, mode)
	if err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	if cloned {
		actual, hashErr := HashFile(tmpPath)
		if hashErr != nil {
			_ = os.Remove(tmpPath)
			return hashErr
		}

		info, statErr := os.Stat(tmpPath)
		if statErr != nil {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("stat cloned blob: %w", statErr)
		}

		if actual != hash || info.Size() != size {
			_ = os.Remove(tmpPath)

			return fmt.Errorf(
				"blob source changed: hash=%s size=%d want hash=%s size=%d",
				actual,
				info.Size(),
				hash,
				size,
			)
		}

		if err := pathutil.ReplaceFile(tmpPath, path); err != nil {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("move blob file: %w", err)
		}

		s.remember(hash, size, path)

		return nil
	}

	_ = os.Remove(tmpPath)

	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open blob source: %w", err)
	}
	defer func() { _ = in.Close() }()

	if err := s.Store(hash, size, in); err != nil {
		return err
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
	src, ok := s.verifiedPath(hash, -1)
	if !ok {
		return fmt.Errorf("blob %s is missing or corrupt", hash)
	}

	if err := os.MkdirAll(filepath.Dir(dest), blobDefaultDirPerm); err != nil {
		return fmt.Errorf("create destination dir: %w", err)
	}

	if cloned, err := cloneBlobToFile(src, dest, mode, modTime); err != nil {
		return err
	} else if cloned {
		reportLogicalWrite(src, onWrite)

		return nil
	}

	return copyBlobToFile(src, dest, mode, modTime, onWrite)
}

func (s *BlobStore) remember(hash string, size int64, path string) {
	info, err := os.Stat(path)
	if err != nil || info.Size() != size {
		return
	}

	s.mu.Lock()
	s.seen[hash] = blobStamp{size: size, modTime: info.ModTime().UnixNano()}
	s.mu.Unlock()
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
	if err := pathutil.Chmod(tmp, mode); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	_ = pathutil.RemoveAll(dest)
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
		chunk := min(remaining, maxChunk)

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
