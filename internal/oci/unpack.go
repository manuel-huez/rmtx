//nolint:wsl_v5
package oci

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/manuel-huez/rmtx/internal/pathutil"
)

const rootfsMarker = ".rmtx-rootfs-ready"
const defaultRootDirMode = 0o755
const legacyTarRegularFile byte = 0

type contextReader struct {
	ctx context.Context
	src io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}

	return r.src.Read(p)
}

func (s *Store) UnpackImage(target string, image Image) error {
	return s.UnpackImageContext(context.Background(), target, image)
}

func (s *Store) UnpackImageContext(ctx context.Context, target string, image Image) error {
	if _, err := os.Stat(filepath.Join(target, rootfsMarker)); err == nil {
		return nil
	}

	tmp := target + ".tmp"
	_ = pathutil.RemoveAll(tmp)
	if err := os.MkdirAll(tmp, dirMode); err != nil {
		return fmt.Errorf("create rootfs temp: %w", err)
	}

	for _, layer := range image.Layers {
		if err := ctx.Err(); err != nil {
			_ = pathutil.RemoveAll(tmp)
			return err
		}

		if err := s.unpackLayer(ctx, tmp, layer.Digest); err != nil {
			_ = pathutil.RemoveAll(tmp)
			return err
		}
	}

	markerContent := []byte(image.ManifestDigest + "\n")
	markerPath := filepath.Join(tmp, rootfsMarker)
	if err := os.WriteFile(markerPath, markerContent, storeFileMode); err != nil {
		_ = pathutil.RemoveAll(tmp)
		return err
	}

	_ = pathutil.RemoveAll(target)
	if err := os.Rename(tmp, target); err != nil {
		_ = pathutil.RemoveAll(tmp)
		return fmt.Errorf("commit rootfs: %w", err)
	}

	return nil
}

func (s *Store) unpackLayer(ctx context.Context, root string, digest string) error {
	f, err := s.ReadBlob(digest)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	var r io.Reader = f
	gz, err := gzip.NewReader(f)
	if err == nil {
		defer func() { _ = gz.Close() }()
		r = gz
	} else if _, seekErr := f.Seek(0, io.SeekStart); seekErr != nil {
		return seekErr
	}

	tr := tar.NewReader(r)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read layer %s: %w", digest, err)
		}

		if err := applyTarEntry(ctx, root, hdr, tr); err != nil {
			return fmt.Errorf("apply layer %s entry %s: %w", digest, hdr.Name, err)
		}
	}
}

//nolint:cyclop // Tar entry handling must branch by OCI whiteout and entry type.
func applyTarEntry(ctx context.Context, root string, hdr *tar.Header, src io.Reader) error {
	name, err := cleanTarName(hdr.Name)
	if err != nil {
		return err
	}
	if name == "" || name == "." {
		return nil
	}

	base := path.Base(name)
	dir := path.Dir(name)
	if strings.HasPrefix(base, ".wh.") {
		return applyWhiteout(root, dir, base)
	}

	target, err := secureLayerPath(root, name)
	if err != nil {
		return err
	}

	if err := ensureNoSymlinkParent(root, name); err != nil {
		return err
	}

	switch hdr.Typeflag {
	case tar.TypeDir:
		return os.MkdirAll(target, fileMode(hdr.FileInfo().Mode(), defaultRootDirMode))
	case tar.TypeReg, legacyTarRegularFile:
		if err := os.MkdirAll(filepath.Dir(target), dirMode); err != nil {
			return err
		}

		_ = pathutil.RemoveAll(target)
		mode := fileMode(hdr.FileInfo().Mode(), storeFileMode)
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
		if err != nil {
			return err
		}

		if _, err := io.Copy(f, contextReader{ctx: ctx, src: src}); err != nil {
			_ = f.Close()
			return err
		}

		if err := f.Close(); err != nil {
			return err
		}

		if err := pathutil.Chmod(target, mode); err != nil {
			return err
		}

		return os.Chtimes(target, hdr.ModTime, hdr.ModTime)
	case tar.TypeSymlink:
		if err := validateSymlinkTarget(root, name, hdr.Linkname); err != nil {
			return err
		}

		if err := os.MkdirAll(filepath.Dir(target), dirMode); err != nil {
			return err
		}

		_ = pathutil.RemoveAll(target)
		if err := pathutil.Symlink(hdr.Linkname, target); err != nil {
			if isUnsupportedWindowsSymlink(err) {
				return nil
			}

			return err
		}

		return nil
	case tar.TypeLink:
		linkTarget, err := secureLayerPath(root, hdr.Linkname)
		if err != nil {
			return err
		}

		if err := os.MkdirAll(filepath.Dir(target), dirMode); err != nil {
			return err
		}

		_ = pathutil.RemoveAll(target)
		return pathutil.Link(linkTarget, target)
	default:
		return nil
	}
}

func cleanTarName(name string) (string, error) {
	if strings.Contains(name, "\x00") || path.IsAbs(name) {
		return "", fmt.Errorf("unsafe path %q", name)
	}

	clean := path.Clean(strings.TrimPrefix(filepath.ToSlash(name), "./"))
	if clean == "." {
		return "", nil
	}

	if slices.Contains(strings.Split(clean, "/"), "..") {
		return "", fmt.Errorf("unsafe path %q", name)
	}

	return clean, nil
}

func isUnsupportedWindowsSymlink(err error) bool {
	return runtime.GOOS == "windows" && strings.Contains(strings.ToLower(err.Error()), "privilege")
}

func applyWhiteout(root, dir, base string) error {
	if base == ".wh..wh..opq" {
		target, err := secureLayerPath(root, dir)
		if err != nil {
			return err
		}

		entries, err := os.ReadDir(target)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}

		for _, entry := range entries {
			if err := pathutil.RemoveAll(filepath.Join(target, entry.Name())); err != nil {
				return err
			}
		}

		return nil
	}

	deleted := path.Join(dir, strings.TrimPrefix(base, ".wh."))
	target, err := secureLayerPath(root, deleted)
	if err != nil {
		return err
	}

	return pathutil.RemoveAll(target)
}

func secureLayerPath(root, name string) (string, error) {
	if filepath.IsAbs(name) || strings.Contains(name, "\x00") {
		return "", fmt.Errorf("unsafe path %q", name)
	}

	clean := filepath.Clean(filepath.FromSlash(name))
	if clean == "." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || clean == ".." {
		return "", fmt.Errorf("unsafe path %q", name)
	}

	target := filepath.Join(root, clean)
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return "", err
	}

	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes root: %q", name)
	}

	return target, nil
}

func ensureNoSymlinkParent(root, name string) error {
	parts := strings.Split(path.Dir(name), "/")
	current := root
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}

		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("parent path contains symlink: %s", part)
		}
	}

	return nil
}

func validateSymlinkTarget(root, name, linkname string) error {
	if strings.Contains(linkname, "\x00") {
		return fmt.Errorf("unsafe symlink target %q", linkname)
	}

	parentDir := path.Dir(name)
	var parent string
	if parentDir == "." {
		parent = root
	} else {
		var err error
		parent, err = secureLayerPath(root, parentDir)
		if err != nil {
			return err
		}
	}

	var target string
	if path.IsAbs(linkname) {
		cleanTarget := strings.TrimPrefix(path.Clean(linkname), "/")
		target = filepath.Join(root, filepath.FromSlash(cleanTarget))
	} else {
		target = filepath.Clean(filepath.Join(parent, filepath.FromSlash(linkname)))
	}

	rel, err := filepath.Rel(root, target)
	if err != nil {
		return err
	}

	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("symlink target escapes root: %q", linkname)
	}

	return nil
}

func fileMode(mode fs.FileMode, fallback fs.FileMode) fs.FileMode {
	if mode == 0 {
		return fallback
	}

	return mode.Perm()
}
