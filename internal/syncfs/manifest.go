package syncfs

import (
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
)

type EntryKind string

const (
	KindFile    EntryKind = "file"
	KindDir     EntryKind = "dir"
	KindSymlink EntryKind = "symlink"
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
	root, err := filepath.Abs(root)
	if err != nil {
		return BuildResult{}, fmt.Errorf("resolve root: %w", err)
	}

	if len(mounts) == 0 {
		mounts = []MountSpec{{Path: "."}}
	}

	jobs := make(chan hashJob)
	results := make(chan hashResult)

	workers := max(runtime.GOMAXPROCS(0), 2)

	var wg sync.WaitGroup
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()

			for job := range jobs {
				h, err := hashFile(job.AbsPath)
				if err != nil {
					results <- hashResult{Err: err}
					continue
				}

				job.Entry.Hash = h
				results <- hashResult{AbsPath: job.AbsPath, Entry: job.Entry}
			}
		}()
	}

	go func() { wg.Wait(); close(results) }()

	entries := map[string]Entry{}
	blobSources := map[string]string{}
	jobCount := 0

	for _, mount := range mounts {
		mountPath, err := resolveMount(root, mount.Path)
		if err != nil {
			close(jobs)

			for range results {
			}

			return BuildResult{}, err
		}

		if _, err := os.Lstat(mountPath); err != nil {
			close(jobs)

			for range results {
			}

			return BuildResult{}, fmt.Errorf("stat mount %s: %w", mount.Path, err)
		}

		walkErr := filepath.WalkDir(
			mountPath,
			func(absPath string, d fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}

				relRoot, err := filepath.Rel(root, absPath)
				if err != nil {
					return err
				}

				relRoot = normalizeRel(relRoot)

				relMount, err := filepath.Rel(mountPath, absPath)
				if err != nil {
					return err
				}

				relMount = normalizeRel(relMount)
				if isExcluded(relRoot, relMount, mount.Exclude) {
					if d.IsDir() {
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

				info, err := d.Info()
				if err != nil {
					return err
				}

				mode := info.Mode()
				switch {
				case mode&os.ModeSymlink != 0:
					target, err := os.Readlink(absPath)
					if err != nil {
						return fmt.Errorf("read symlink %s: %w", absPath, err)
					}

					entries[relRoot] = Entry{
						Path:     relRoot,
						Kind:     KindSymlink,
						Linkname: target,
						Mode:     uint32(mode.Perm()),
					}
				case d.IsDir():
					entries[relRoot] = Entry{
						Path: relRoot,
						Kind: KindDir,
						Mode: uint32(mode.Perm()),
					}
				case mode.IsRegular():
					jobCount++

					jobs <- hashJob{AbsPath: absPath, Entry: Entry{Path: relRoot, Kind: KindFile, Size: info.Size(), Mode: uint32(mode.Perm())}}
				default:
					return nil
				}

				return nil
			},
		)
		if walkErr != nil {
			close(jobs)

			for range results {
			}

			return BuildResult{}, fmt.Errorf("walk mount %s: %w", mount.Path, walkErr)
		}
	}

	close(jobs)

	for range jobCount {
		res, ok := <-results
		if !ok {
			break
		}

		if res.Err != nil {
			for range results {
			}

			return BuildResult{}, res.Err
		}

		entries[res.Entry.Path] = res.Entry
		if _, exists := blobSources[res.Entry.Hash]; !exists {
			blobSources[res.Entry.Hash] = res.AbsPath
		}
	}

	for range results {
	}

	out := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })

	return BuildResult{Entries: out, BlobSources: blobSources}, nil
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

		switch entry.Kind {
		case KindDir:
			if err := os.MkdirAll(target, fileMode(entry.Mode, 0o755)); err != nil {
				return fmt.Errorf("mkdir %s: %w", entry.Path, err)
			}

			if err := os.Chmod(target, fileMode(entry.Mode, 0o755)); err != nil {
				return fmt.Errorf("chmod dir %s: %w", entry.Path, err)
			}
		case KindSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return fmt.Errorf("mkdir symlink parent %s: %w", entry.Path, err)
			}

			_ = os.RemoveAll(target)
			if err := os.Symlink(entry.Linkname, target); err != nil {
				return fmt.Errorf("symlink %s: %w", entry.Path, err)
			}
		}
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

	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("mkdir file parent %s: %w", entry.Path, err)
	}

	_ = os.RemoveAll(target)
	tmp := target + ".rmtx-tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fileMode(entry.Mode, 0o644))
	if err != nil {
		return fmt.Errorf("create file %s: %w", entry.Path, err)
	}

	if _, err := io.Copy(f, src); err != nil {
		f.Close()

		_ = os.Remove(tmp)

		return fmt.Errorf("write file %s: %w", entry.Path, err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close file %s: %w", entry.Path, err)
	}

	if err := os.Chmod(tmp, fileMode(entry.Mode, 0o644)); err != nil {
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
	clean := filepath.Clean(rel)
	joined := filepath.Join(root, clean)

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}

	absJoined, err := filepath.Abs(joined)
	if err != nil {
		return "", err
	}

	if absJoined != absRoot && !strings.HasPrefix(absJoined, absRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("path %s escapes root %s", rel, root)
	}

	return absJoined, nil
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

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file %s: %w", path, err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
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
