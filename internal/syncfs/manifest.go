package syncfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/manuel-huez/rmtx/internal/pathutil"
)

type EntryKind string

const (
	KindFile    EntryKind = "file"
	KindDir     EntryKind = "dir"
	KindSymlink EntryKind = "symlink"
)

const (
	minWorkers      = 2
	defaultDirMode  = 0o755
	defaultFileMode = 0o644
)

type MountSpec struct {
	Path    string   `json:"path"`
	Exclude []string `json:"exclude,omitempty"`
}

type Entry struct {
	Path     string    `json:"path"`
	Kind     EntryKind `json:"kind"`
	Hash     string    `json:"hash,omitempty"`
	Size     int64     `json:"size,omitempty"`
	Mode     uint32    `json:"mode,omitempty"`
	Linkname string    `json:"linkname,omitempty"`
}

type BuildResult struct {
	Entries     []Entry
	BlobSources map[string]string
}

func NonFileEntries(entries []Entry) []Entry {
	nonFiles := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Kind != KindFile {
			nonFiles = append(nonFiles, entry)
		}
	}

	return nonFiles
}

type hashJob struct {
	AbsPath string
	Entry   Entry
}

type hashResult struct {
	AbsPath string
	Entry   Entry
	Err     error
}

func BuildManifest(root string, mounts []MountSpec) (BuildResult, error) {
	return BuildManifestContext(context.Background(), root, mounts)
}

func BuildManifestContext(
	ctx context.Context,
	root string,
	mounts []MountSpec,
) (BuildResult, error) {
	root, mounts, err := normalizeBuildInputs(root, mounts)
	if err != nil {
		return BuildResult{}, err
	}

	entries := map[string]Entry{}
	blobSources := map[string]string{}

	jobs, err := enqueueMountJobs(ctx, root, mounts, entries)
	if err != nil {
		return BuildResult{}, err
	}

	if err := hashManifestFiles(ctx, jobs, entries, blobSources); err != nil {
		return BuildResult{}, err
	}

	out := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })

	return BuildResult{Entries: out, BlobSources: blobSources}, nil
}

func hashManifestFiles(
	ctx context.Context,
	jobs []hashJob,
	entries map[string]Entry,
	blobSources map[string]string,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := max(runtime.GOMAXPROCS(0), minWorkers)
	jobCh := make(chan hashJob)
	results := make(chan hashResult)

	startHashWorkers(ctx, workers, jobCh, results)
	sendHashJobs(ctx, jobs, jobCh)

	for res := range results {
		if res.Err != nil {
			cancel()

			for range results {
			}

			return res.Err
		}

		entries[res.Entry.Path] = res.Entry
		if _, exists := blobSources[res.Entry.Hash]; !exists {
			blobSources[res.Entry.Hash] = res.AbsPath
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return nil
}

func startHashWorkers(
	ctx context.Context,
	workers int,
	jobCh <-chan hashJob,
	results chan<- hashResult,
) {
	var wg sync.WaitGroup
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()

			for job := range jobCh {
				h, err := hashFileContext(ctx, job.AbsPath)
				if err != nil {
					select {
					case results <- hashResult{Err: err}:
					case <-ctx.Done():
					}

					continue
				}

				job.Entry.Hash = h
				select {
				case results <- hashResult{AbsPath: job.AbsPath, Entry: job.Entry}:
				case <-ctx.Done():
				}
			}
		}()
	}

	go func() { wg.Wait(); close(results) }()
}

func sendHashJobs(ctx context.Context, jobs []hashJob, jobCh chan<- hashJob) {
	go func() {
		defer close(jobCh)

		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return
			case jobCh <- job:
			}
		}
	}()
}

func normalizeBuildInputs(root string, mounts []MountSpec) (string, []MountSpec, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", nil, fmt.Errorf("resolve root: %w", err)
	}

	if len(mounts) == 0 {
		mounts = []MountSpec{{Path: "."}}
	}

	return absRoot, mounts, nil
}

func enqueueMountJobs(
	ctx context.Context,
	root string,
	mounts []MountSpec,
	entries map[string]Entry,
) ([]hashJob, error) {
	jobs := []hashJob{}

	for _, mount := range mounts {
		addedJobs, walkErr := walkMount(ctx, root, mount, entries)
		if walkErr != nil {
			return nil, walkErr
		}

		jobs = append(jobs, addedJobs...)
	}

	return jobs, nil
}

//nolint:cyclop // Walk callback handles context, exclude, duplicate, and entry-kind branches.
func walkMount(
	ctx context.Context,
	root string,
	mount MountSpec,
	entries map[string]Entry,
) ([]hashJob, error) {
	mountPath, err := resolveAndStatMount(root, mount.Path)
	if err != nil {
		return nil, err
	}

	jobs := []hashJob{}

	walkErr := filepath.WalkDir(
		mountPath,
		func(absPath string, d fs.DirEntry, walkErr error) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			if walkErr != nil {
				return walkErr
			}

			relRoot, relMount, err := computeRelativePaths(root, mountPath, absPath)
			if err != nil {
				return err
			}

			if excludedDir, skip := shouldSkipEntry(relRoot, relMount, mount.Exclude, d); skip {
				if excludedDir {
					return filepath.SkipDir
				}

				return nil
			}

			if relRoot == "." {
				return nil
			}

			if _, exists := entries[relRoot]; exists {
				return nil
			}

			entry, isFile, err := buildEntry(absPath, relRoot, d)
			if err != nil {
				return err
			}

			if isFile {
				jobs = append(jobs, hashJob{AbsPath: absPath, Entry: entry})

				return nil
			}

			if entry.Kind != "" {
				entries[relRoot] = entry
			}

			return nil
		},
	)
	if walkErr != nil {
		return nil, fmt.Errorf("walk mount %s: %w", mount.Path, walkErr)
	}

	return jobs, nil
}

func resolveAndStatMount(root, mountPath string) (string, error) {
	resolved, err := resolveMount(root, mountPath)
	if err != nil {
		return "", err
	}

	if _, err := os.Lstat(resolved); err != nil {
		return "", fmt.Errorf("stat mount %s: %w", mountPath, err)
	}

	return resolved, nil
}

func computeRelativePaths(root, mountPath, absPath string) (string, string, error) {
	relRoot, err := filepath.Rel(root, absPath)
	if err != nil {
		return "", "", err
	}

	relMount, err := filepath.Rel(mountPath, absPath)
	if err != nil {
		return "", "", err
	}

	return normalizeRel(relRoot), normalizeRel(relMount), nil
}

func shouldSkipEntry(relRoot, relMount string, exclude []string, d fs.DirEntry) (bool, bool) {
	if !isExcluded(relRoot, relMount, exclude) {
		return false, false
	}

	if d.IsDir() {
		return true, true
	}

	return false, true
}

func buildEntry(absPath, relRoot string, d fs.DirEntry) (Entry, bool, error) {
	info, err := d.Info()
	if err != nil {
		return Entry{}, false, err
	}

	mode := info.Mode()
	switch {
	case mode&os.ModeSymlink != 0:
		target, err := os.Readlink(absPath)
		if err != nil {
			return Entry{}, false, fmt.Errorf("read symlink %s: %w", absPath, err)
		}

		return Entry{
			Path:     relRoot,
			Kind:     KindSymlink,
			Linkname: target,
			Mode:     uint32(mode.Perm()),
		}, false, nil
	case d.IsDir():
		return Entry{
			Path: relRoot,
			Kind: KindDir,
			Mode: uint32(mode.Perm()),
		}, false, nil
	case mode.IsRegular():
		return Entry{
			Path: relRoot,
			Kind: KindFile,
			Size: info.Size(),
			Mode: uint32(mode.Perm()),
		}, true, nil
	default:
		return Entry{}, false, nil
	}
}

func Diff(before, after []Entry) (changed []Entry, deleted []string) {
	beforeMap := map[string]Entry{}
	afterMap := map[string]Entry{}

	for _, e := range before {
		beforeMap[e.Path] = e
	}

	for _, e := range after {
		afterMap[e.Path] = e
	}

	for path, cur := range afterMap {
		prev, ok := beforeMap[path]
		if !ok || !sameEntry(prev, cur) {
			changed = append(changed, cur)
		}
	}

	for path := range beforeMap {
		if _, ok := afterMap[path]; !ok {
			deleted = append(deleted, path)
		}
	}

	sort.Slice(changed, func(i, j int) bool { return changed[i].Path < changed[j].Path })
	sort.Slice(deleted, func(i, j int) bool { return depth(deleted[i]) > depth(deleted[j]) })

	return changed, deleted
}

func DeletePaths(root string, paths []string) error {
	sorted := append([]string(nil), paths...)
	sort.Slice(sorted, func(i, j int) bool { return depth(sorted[i]) > depth(sorted[j]) })

	for _, rel := range sorted {
		if rel == "." {
			continue
		}

		target, err := secureJoin(root, rel)
		if err != nil {
			return err
		}

		if err := os.RemoveAll(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("delete %s: %w", rel, err)
		}
	}

	return nil
}

func ApplyNonFileEntries(root string, entries []Entry) error {
	sorted := append([]Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Kind == KindDir && sorted[j].Kind != KindDir {
			return true
		}

		if sorted[i].Kind != KindDir && sorted[j].Kind == KindDir {
			return false
		}

		return sorted[i].Path < sorted[j].Path
	})

	for _, entry := range sorted {
		if entry.Path == "." {
			continue
		}

		target, err := secureJoin(root, entry.Path)
		if err != nil {
			return err
		}

		if err := applyNonFileEntry(entry, target); err != nil {
			return err
		}
	}

	return nil
}

func applyNonFileEntry(entry Entry, target string) error {
	switch entry.Kind {
	case KindDir:
		if err := os.MkdirAll(target, fileMode(entry.Mode, defaultDirMode)); err != nil {
			return fmt.Errorf("mkdir %s: %w", entry.Path, err)
		}

		if err := os.Chmod(target, fileMode(entry.Mode, defaultDirMode)); err != nil {
			return fmt.Errorf("chmod dir %s: %w", entry.Path, err)
		}
	case KindSymlink:
		if err := os.MkdirAll(filepath.Dir(target), defaultDirMode); err != nil {
			return fmt.Errorf("mkdir symlink parent %s: %w", entry.Path, err)
		}

		_ = os.RemoveAll(target)
		if err := os.Symlink(entry.Linkname, target); err != nil {
			return fmt.Errorf("symlink %s: %w", entry.Path, err)
		}
	case KindFile:
		return nil
	default:
		return fmt.Errorf("unsupported entry kind %q for %s", entry.Kind, entry.Path)
	}

	return nil
}

func WriteFile(root string, entry Entry, src io.Reader) error {
	if entry.Kind != KindFile {
		return fmt.Errorf("entry %s is not a file", entry.Path)
	}

	target, err := secureJoin(root, entry.Path)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(target), defaultDirMode); err != nil {
		return fmt.Errorf("mkdir file parent %s: %w", entry.Path, err)
	}

	_ = os.RemoveAll(target)
	tmp := target + ".rmtx-tmp"

	f, err := os.OpenFile(
		tmp,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		fileMode(entry.Mode, defaultFileMode),
	)
	if err != nil {
		return fmt.Errorf("create file %s: %w", entry.Path, err)
	}

	if _, err := io.Copy(f, src); err != nil {
		_ = f.Close()

		_ = os.Remove(tmp)

		return fmt.Errorf("write file %s: %w", entry.Path, err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close file %s: %w", entry.Path, err)
	}

	if err := os.Chmod(tmp, fileMode(entry.Mode, defaultFileMode)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("chmod file %s: %w", entry.Path, err)
	}

	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename file %s: %w", entry.Path, err)
	}

	return nil
}

func resolveMount(root, mountPath string) (string, error) {
	if strings.TrimSpace(mountPath) == "" {
		mountPath = "."
	}

	if filepath.IsAbs(mountPath) {
		clean := filepath.Clean(mountPath)

		rel, err := filepath.Rel(root, clean)
		if err != nil {
			return "", err
		}

		if strings.HasPrefix(rel, "..") {
			return "", fmt.Errorf("mount %s escapes root %s", mountPath, root)
		}

		return clean, nil
	}

	return secureJoin(root, mountPath)
}

func secureJoin(root, rel string) (string, error) {
	return pathutil.SecureJoin(root, rel)
}

func normalizeRel(rel string) string {
	if rel == "" {
		return "."
	}

	return filepath.ToSlash(filepath.Clean(rel))
}

func isExcluded(relRoot, relMount string, patterns []string) bool {
	for _, pattern := range patterns {
		pattern = normalizePattern(pattern)
		if pattern == "" {
			continue
		}

		if relRoot != "." && matchPattern(pattern, relRoot) {
			return true
		}

		if relMount != "." && matchPattern(pattern, relMount) {
			return true
		}
	}

	return false
}

func normalizePattern(pattern string) string {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return ""
	}

	pattern = filepath.ToSlash(filepath.Clean(pattern))
	if strings.HasSuffix(pattern, "/") {
		pattern += "**"
	}

	return pattern
}

func matchPattern(pattern, candidate string) bool {
	pattern = strings.Trim(pattern, "/")
	candidate = strings.Trim(candidate, "/")
	pSegs := splitPath(pattern)
	cSegs := splitPath(candidate)

	return matchSegments(pSegs, cSegs)
}

func splitPath(v string) []string {
	v = strings.Trim(v, "/")
	if v == "" || v == "." {
		return nil
	}

	return strings.Split(v, "/")
}

func matchSegments(pattern, candidate []string) bool {
	if len(pattern) == 0 {
		return len(candidate) == 0
	}

	if pattern[0] == "**" {
		if len(pattern) == 1 {
			return true
		}

		for i := 0; i <= len(candidate); i++ {
			if matchSegments(pattern[1:], candidate[i:]) {
				return true
			}
		}

		return false
	}

	if len(candidate) == 0 {
		return false
	}

	matched, err := path.Match(pattern[0], candidate[0])
	if err != nil || !matched {
		return false
	}

	return matchSegments(pattern[1:], candidate[1:])
}

type cancelReader struct {
	done <-chan struct{}
	err  func() error
	src  io.Reader
}

func (r cancelReader) Read(p []byte) (int, error) {
	select {
	case <-r.done:
		return 0, r.err()
	default:
	}

	return r.src.Read(p)
}

func hashFileContext(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file %s: %w", path, err)
	}

	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, cancelReader{done: ctx.Done(), err: ctx.Err, src: f}); err != nil {
		return "", fmt.Errorf("hash file %s: %w", path, err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func sameEntry(a, b Entry) bool {
	return a.Kind == b.Kind && a.Hash == b.Hash && a.Size == b.Size && a.Mode == b.Mode &&
		a.Linkname == b.Linkname
}

func depth(path string) int {
	path = normalizeRel(path)
	if path == "." {
		return 0
	}

	return len(strings.Split(path, "/"))
}

func fileMode(raw uint32, fallback fs.FileMode) fs.FileMode {
	if raw == 0 {
		return fallback
	}

	return fs.FileMode(raw)
}
