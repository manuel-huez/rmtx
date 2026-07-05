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
	"time"

	"github.com/manuel-huez/rmtx/internal/pathutil"
)

const (
	rootfsMarker                   = ".rmtx-rootfs-ready"
	defaultRootDirMode             = 0o755
	legacyTarRegularFile      byte = 0
	unpackProgressMinBytes         = 64 << 20
	unpackProgressMinEntries       = 512
	unpackProgressMinInterval      = 5 * time.Second
)

// UnpackProgressEvent identifies the stage of one OCI layer extraction.
type UnpackProgressEvent string

const (
	UnpackProgressLayerStart    UnpackProgressEvent = "layer_start"
	UnpackProgressLayerProgress UnpackProgressEvent = "layer_progress"
	UnpackProgressLayerDone     UnpackProgressEvent = "layer_done"
)

// UnpackProgress reports compressed layer bytes plus tar entries applied so far.
type UnpackProgress struct {
	Event          UnpackProgressEvent
	LayerIndex     int
	LayerCount     int
	Digest         string
	LayerBytes     int64
	LayerDoneBytes int64
	TotalBytes     int64
	TotalDoneBytes int64
	Entries        int64
}

// UnpackProgressFunc receives throttled layer progress during rootfs unpack.
type UnpackProgressFunc func(UnpackProgress)

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

// UnpackImageContext applies image layers into a rootfs, reporting progress while extracting.
func (s *Store) UnpackImageContext(
	ctx context.Context,
	target string,
	image Image,
	progress UnpackProgressFunc,
) error {
	if _, err := os.Stat(filepath.Join(target, rootfsMarker)); err == nil {
		return nil
	}

	tmp := target + ".tmp"
	_ = pathutil.RemoveAll(tmp)
	if err := os.MkdirAll(tmp, dirMode); err != nil {
		return fmt.Errorf("create rootfs temp: %w", err)
	}

	layerTotal := unpackLayerTotalBytes(image.Layers)
	doneBytes := int64(0)
	for index, layer := range image.Layers {
		if err := ctx.Err(); err != nil {
			_ = pathutil.RemoveAll(tmp)
			return err
		}

		layerDone, err := s.unpackLayer(
			ctx,
			tmp,
			layer,
			index+1,
			len(image.Layers),
			doneBytes,
			layerTotal,
			progress,
		)
		if err != nil {
			_ = pathutil.RemoveAll(tmp)
			return err
		}
		doneBytes += layerProgressBytes(layer.Size, layerDone)
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

func unpackLayerTotalBytes(layers []Descriptor) int64 {
	var total int64
	for _, layer := range layers {
		if layer.Size > 0 {
			total += layer.Size
		}
	}

	return total
}

func layerProgressBytes(expected, actual int64) int64 {
	if expected > 0 {
		return expected
	}

	return actual
}

func (s *Store) unpackLayer(
	ctx context.Context,
	root string,
	layer Descriptor,
	layerIndex int,
	layerCount int,
	totalDoneBeforeLayer int64,
	totalBytes int64,
	progress UnpackProgressFunc,
) (int64, error) {
	digest := layer.Digest
	f, err := s.ReadBlob(digest)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	reporter := newUnpackProgressReporter(
		progress,
		layerIndex,
		layerCount,
		digest,
		layer.Size,
		totalDoneBeforeLayer,
		totalBytes,
	)
	reporter.start()

	metered := &unpackProgressReader{ctx: ctx, src: f, reporter: reporter}
	var r io.Reader = metered
	gz, err := gzip.NewReader(metered)
	if err == nil {
		defer func() { _ = gz.Close() }()
		r = gz
	} else if _, seekErr := f.Seek(0, io.SeekStart); seekErr != nil {
		return reporter.bytes(), seekErr
	} else {
		reporter.resetBytes()
		metered = &unpackProgressReader{ctx: ctx, src: f, reporter: reporter}
		r = metered
	}

	tr := tar.NewReader(r)
	for {
		if err := ctx.Err(); err != nil {
			return reporter.bytes(), err
		}

		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return reporter.done(), nil
		}
		if err != nil {
			return reporter.bytes(), fmt.Errorf("read layer %s: %w", digest, err)
		}

		if err := applyTarEntry(ctx, root, hdr, tr); err != nil {
			return reporter.bytes(), fmt.Errorf("apply layer %s entry %s: %w", digest, hdr.Name, err)
		}
		reporter.addEntry()
	}
}

type unpackProgressReader struct {
	ctx      context.Context
	src      io.Reader
	reporter *unpackProgressReporter
}

func (r *unpackProgressReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}

	n, err := r.src.Read(p)
	if n > 0 {
		r.reporter.addBytes(int64(n))
	}

	return n, err
}

type unpackProgressReporter struct {
	progress UnpackProgressFunc
	state    UnpackProgress
	last     UnpackProgress
	lastAt   time.Time
}

func newUnpackProgressReporter(
	progress UnpackProgressFunc,
	layerIndex int,
	layerCount int,
	digest string,
	layerBytes int64,
	totalDoneBeforeLayer int64,
	totalBytes int64,
) *unpackProgressReporter {
	return &unpackProgressReporter{
		progress: progress,
		state: UnpackProgress{
			LayerIndex:     layerIndex,
			LayerCount:     layerCount,
			Digest:         digest,
			LayerBytes:     layerBytes,
			TotalBytes:     totalBytes,
			TotalDoneBytes: totalDoneBeforeLayer,
		},
	}
}

func (r *unpackProgressReporter) start() {
	r.report(UnpackProgressLayerStart, true)
}

func (r *unpackProgressReporter) resetBytes() {
	r.state.TotalDoneBytes -= r.state.LayerDoneBytes
	r.state.LayerDoneBytes = 0
	r.last.LayerDoneBytes = 0
	r.last.TotalDoneBytes = r.state.TotalDoneBytes
}

func (r *unpackProgressReporter) addBytes(n int64) {
	r.state.LayerDoneBytes += n
	r.state.TotalDoneBytes += n
	r.report(UnpackProgressLayerProgress, false)
}

func (r *unpackProgressReporter) addEntry() {
	r.state.Entries++
	r.report(UnpackProgressLayerProgress, false)
}

func (r *unpackProgressReporter) done() int64 {
	r.report(UnpackProgressLayerDone, true)

	return r.bytes()
}

func (r *unpackProgressReporter) bytes() int64 {
	return r.state.LayerDoneBytes
}

func (r *unpackProgressReporter) report(event UnpackProgressEvent, force bool) {
	if r.progress == nil {
		return
	}

	now := time.Now()
	if !force &&
		r.state.LayerDoneBytes-r.last.LayerDoneBytes < unpackProgressMinBytes &&
		r.state.Entries-r.last.Entries < unpackProgressMinEntries &&
		now.Sub(r.lastAt) < unpackProgressMinInterval {
		return
	}

	snapshot := r.state
	snapshot.Event = event
	r.progress(snapshot)
	r.last = snapshot
	r.lastAt = now
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
