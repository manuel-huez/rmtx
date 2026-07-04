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
	"time"

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
	progressEvery   = 3 * time.Second
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
	ModTime  int64     `json:"mod_time,omitempty"`
	Linkname string    `json:"linkname,omitempty"`
}

type BuildResult struct {
	Entries     []Entry
	BlobSources map[string]string
}

type BuildOptions struct {
	Progress         func(BuildProgress)
	ProgressInterval time.Duration
	PreviousEntries  []Entry
}

type BuildProgress struct {
	Phase      string
	Mount      string
	Scanned    int
	Skipped    int
	Dirs       int
	Files      int
	Symlinks   int
	Hashed     int
	TotalFiles int
	Bytes      int64
	Done       bool
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
	return BuildManifestContextOptions(ctx, root, mounts, BuildOptions{})
}

func BuildManifestContextOptions(
	ctx context.Context,
	root string,
	mounts []MountSpec,
	opts BuildOptions,
) (BuildResult, error) {
	root, mounts, err := normalizeBuildInputs(root, mounts)
	if err != nil {
		return BuildResult{}, err
	}

	entries := map[string]Entry{}
	blobSources := map[string]string{}
	previous := previousFileEntries(opts.PreviousEntries)
	progress := newProgressReporter(opts)

	jobs, err := enqueueMountJobs(ctx, root, mounts, entries, blobSources, previous, progress)
	if err != nil {
		return BuildResult{}, err
	}

	if err := hashManifestFiles(ctx, jobs, entries, blobSources, progress); err != nil {
		return BuildResult{}, err
	}

	out := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })

	return BuildResult{Entries: out, BlobSources: blobSources}, nil
}

type progressReporter struct {
	fn       func(BuildProgress)
	interval time.Duration
	last     time.Time
}

func newProgressReporter(opts BuildOptions) *progressReporter {
	if opts.Progress == nil {
		return nil
	}

	interval := opts.ProgressInterval
	if interval <= 0 {
		interval = progressEvery
	}

	return &progressReporter{fn: opts.Progress, interval: interval}
}

func (p *progressReporter) Report(progress BuildProgress, force bool) {
	if p == nil || p.fn == nil {
		return
	}

	now := time.Now()
	if !force && !p.last.IsZero() && now.Sub(p.last) < p.interval {
		return
	}

	p.last = now
	p.fn(progress)
}

func hashManifestFiles(
	ctx context.Context,
	jobs []hashJob,
	entries map[string]Entry,
	blobSources map[string]string,
	progress *progressReporter,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := max(runtime.GOMAXPROCS(0), minWorkers)
	jobCh := make(chan hashJob)
	results := make(chan hashResult)

	startHashWorkers(ctx, workers, jobCh, results)
	sendHashJobs(ctx, jobs, jobCh)

	stats := BuildProgress{Phase: "hash", TotalFiles: len(jobs)}
	progress.Report(stats, true)

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

		stats.Hashed++
		stats.Bytes += res.Entry.Size
		progress.Report(stats, false)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	stats.Done = true
	progress.Report(stats, true)

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
	blobSources map[string]string,
	previous map[string]Entry,
	progress *progressReporter,
) ([]hashJob, error) {
	jobs := []hashJob{}

	for _, mount := range mounts {
		addedJobs, walkErr := walkMount(ctx, root, mount, entries, blobSources, previous, progress)
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
	blobSources map[string]string,
	previous map[string]Entry,
	progress *progressReporter,
) ([]hashJob, error) {
	mountPath, err := resolveAndStatMount(root, mount.Path)
	if err != nil {
		return nil, err
	}

	jobs := []hashJob{}
	matcher := newExcludeMatcher(mount.Exclude)

	mountRelRoot, err := filepath.Rel(root, mountPath)
	if err != nil {
		return nil, err
	}

	mountRelRoot = normalizeRel(mountRelRoot)
	stats := BuildProgress{Phase: "walk", Mount: mount.Path}
	progress.Report(stats, true)

	walkErr := filepath.WalkDir(
		mountPath,
		func(absPath string, d fs.DirEntry, walkErr error) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			if walkErr != nil {
				return walkErr
			}

			relMount, err := relativeUnder(mountPath, absPath)
			if err != nil {
				return err
			}

			relRoot := joinRel(mountRelRoot, relMount)
			stats.Scanned++

			if excludedDir, skip := shouldSkipEntry(relRoot, relMount, matcher, d); skip {
				stats.Skipped++
				progress.Report(stats, false)

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

			entry, isFile, err := buildEntry(root, absPath, relRoot, d)
			if err != nil {
				return err
			}

			if isFile {
				if reuseCachedFile(entry, absPath, entries, blobSources, previous) {
					reportFileWalked(&stats, progress)

					return nil
				}

				jobs = append(jobs, hashJob{AbsPath: absPath, Entry: entry})

				reportFileWalked(&stats, progress)

				return nil
			}

			if entry.Kind != "" {
				entries[relRoot] = entry
				switch entry.Kind {
				case KindFile:
				case KindDir:
					stats.Dirs++
				case KindSymlink:
					stats.Symlinks++
				}
			}

			progress.Report(stats, false)

			return nil
		},
	)
	if walkErr != nil {
		return nil, fmt.Errorf("walk mount %s: %w", mount.Path, walkErr)
	}

	stats.Done = true
	progress.Report(stats, true)

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

func shouldSkipEntry(relRoot, relMount string, matcher excludeMatcher, d fs.DirEntry) (bool, bool) {
	if !matcher.Match(relRoot, relMount) {
		return false, false
	}

	if d.IsDir() {
		return true, true
	}

	return false, true
}

func buildEntry(root, absPath, relRoot string, d fs.DirEntry) (Entry, bool, error) {
	info, err := d.Info()
	if err != nil {
		return Entry{}, false, err
	}

	mode := info.Mode()
	switch {
	case mode&os.ModeSymlink != 0:
		linkname, ok, err := portableSymlinkTarget(root, absPath)
		if err != nil {
			return Entry{}, false, err
		}

		if !ok {
			return Entry{}, false, nil
		}

		return Entry{
			Path:     relRoot,
			Kind:     KindSymlink,
			Linkname: linkname,
			Mode:     uint32(mode.Perm()),
			ModTime:  info.ModTime().UnixNano(),
		}, false, nil
	case d.IsDir():
		return Entry{
			Path:    relRoot,
			Kind:    KindDir,
			Mode:    uint32(mode.Perm()),
			ModTime: info.ModTime().UnixNano(),
		}, false, nil
	case mode.IsRegular():
		return Entry{
			Path:    relRoot,
			Kind:    KindFile,
			Size:    info.Size(),
			Mode:    uint32(mode.Perm()),
			ModTime: info.ModTime().UnixNano(),
		}, true, nil
	default:
		return Entry{}, false, nil
	}
}

func portableSymlinkTarget(root, absPath string) (string, bool, error) {
	target, err := os.Readlink(absPath)
	if err != nil {
		return "", false, fmt.Errorf("read symlink %s: %w", absPath, err)
	}

	linkDir := filepath.Dir(absPath)

	if filepath.IsAbs(target) {
		cleanTarget := filepath.Clean(target)
		if !pathWithinRoot(root, cleanTarget) {
			return "", false, nil
		}

		rel, err := filepath.Rel(linkDir, cleanTarget)
		if err != nil {
			return "", false, fmt.Errorf("rel symlink target %s: %w", absPath, err)
		}

		return filepath.ToSlash(rel), true, nil
	}

	resolved := filepath.Clean(filepath.Join(linkDir, target))
	if !pathWithinRoot(root, resolved) {
		return "", false, nil
	}

	return filepath.ToSlash(target), true, nil
}

func pathWithinRoot(root, absPath string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(absPath))
	if err != nil {
		return false
	}

	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func previousFileEntries(entries []Entry) map[string]Entry {
	previous := map[string]Entry{}

	for _, entry := range entries {
		if entry.Kind == KindFile && entry.Hash != "" {
			previous[entry.Path] = entry
		}
	}

	return previous
}

func reuseCachedFile(
	entry Entry,
	absPath string,
	entries map[string]Entry,
	blobSources map[string]string,
	previous map[string]Entry,
) bool {
	prev := previous[entry.Path]
	if !reuseFileHash(prev, entry) {
		return false
	}

	entry.Hash = prev.Hash
	entries[entry.Path] = entry

	if _, exists := blobSources[entry.Hash]; !exists {
		blobSources[entry.Hash] = absPath
	}

	return true
}

func reuseFileHash(previous Entry, current Entry) bool {
	return previous.Kind == KindFile &&
		previous.Hash != "" &&
		previous.Size == current.Size &&
		previous.Mode == current.Mode &&
		previous.ModTime != 0 &&
		previous.ModTime == current.ModTime
}

func reportFileWalked(stats *BuildProgress, progress *progressReporter) {
	stats.Files++
	progress.Report(*stats, false)
}

type DiffOptions struct {
	IgnoreMode bool
}

func Diff(before, after []Entry, opts DiffOptions) (changed []Entry, deleted []string) {
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
		if !ok || !sameEntry(prev, cur, opts.IgnoreMode) {
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

func NormalizeModes(entries, reference []Entry) []Entry {
	out := append([]Entry(nil), entries...)
	refByPath := map[string]Entry{}

	for _, entry := range reference {
		refByPath[entry.Path] = entry
	}

	for i, entry := range out {
		if ref, ok := refByPath[entry.Path]; ok && sameEntryExceptMode(ref, entry) {
			out[i].Mode = ref.Mode
			continue
		}

		if out[i].Mode == 0 {
			switch out[i].Kind {
			case KindDir:
				out[i].Mode = uint32(defaultDirMode)
			case KindFile:
				out[i].Mode = uint32(defaultFileMode)
			case KindSymlink:
			}
		}
	}

	return out
}

func PreserveMissingEntries(entries, preserve []Entry, kind EntryKind) []Entry {
	out := append([]Entry(nil), entries...)
	seen := map[string]bool{}

	for _, entry := range out {
		seen[entry.Path] = true
	}

	for _, entry := range preserve {
		if entry.Kind == kind && !seen[entry.Path] {
			out = append(out, entry)
			seen[entry.Path] = true
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })

	return out
}

func FilterEntriesByPath(entries []Entry, includes []string) []Entry {
	matcher := newIncludeMatcher(includes)
	if matcher.all {
		return append([]Entry(nil), entries...)
	}

	out := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		if matcher.Match(entry.Path) {
			out = append(out, entry)
		}
	}

	return out
}

func ValidateSyncBack(root string, mounts []MountSpec, syncBack []string) error {
	if syncBack == nil || len(syncBack) == 0 {
		return nil
	}

	root, mounts, err := normalizeBuildInputs(root, mounts)
	if err != nil {
		return err
	}

	for _, include := range syncBack {
		normalized := normalizePattern(include)
		if normalized == "" {
			continue
		}

		if !syncBackPatternEligible(root, mounts, normalized) {
			return fmt.Errorf("sync_back path %q is not covered by mounted, non-ignored files", include)
		}
	}

	return nil
}

func syncBackPatternEligible(root string, mounts []MountSpec, pattern string) bool {
	base := literalPatternPrefix(pattern)
	sample := samplePatternPath(pattern)

	for _, mount := range mounts {
		mountPath, err := resolveMount(root, mount.Path)
		if err != nil {
			continue
		}

		mountRelRoot, err := filepath.Rel(root, mountPath)
		if err != nil {
			continue
		}

		mountRelRoot = normalizeRel(mountRelRoot)
		if !pathUnderOrEqual(base, mountRelRoot) {
			continue
		}

		matcher := newExcludeMatcher(mount.Exclude)
		relMountBase := relFromMount(mountRelRoot, base)
		relMountSample := relFromMount(mountRelRoot, sample)
		if matcher.Match(base, relMountBase) || matcher.Match(sample, relMountSample) {
			continue
		}

		return true
	}

	return false
}

func literalPatternPrefix(pattern string) string {
	parts := splitPath(pattern)
	prefix := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.ContainsAny(part, "*?[") {
			break
		}

		prefix = append(prefix, part)
	}

	if len(prefix) == 0 {
		return "."
	}

	return path.Join(prefix...)
}

func samplePatternPath(pattern string) string {
	parts := splitPath(pattern)
	for i, part := range parts {
		switch {
		case part == "**":
			parts[i] = "x"
		case strings.ContainsAny(part, "*?["):
			parts[i] = samplePatternSegment(part)
		}
	}

	if len(parts) == 0 {
		return "."
	}

	return path.Join(parts...)
}

func samplePatternSegment(pattern string) string {
	var b strings.Builder
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*', '?':
			b.WriteByte('x')
		case '[':
			end := strings.IndexByte(pattern[i+1:], ']')
			if end >= 0 {
				b.WriteByte(sampleCharClass(pattern[i+1 : i+1+end]))
				i += end + 1
				continue
			}

			b.WriteByte('x')
		default:
			b.WriteByte(pattern[i])
		}
	}

	if b.Len() == 0 {
		return "x"
	}

	return b.String()
}

func sampleCharClass(class string) byte {
	class = strings.TrimPrefix(class, "!")
	class = strings.TrimPrefix(class, "^")
	if class == "" {
		return 'x'
	}

	if len(class) >= 3 && class[1] == '-' {
		return class[0]
	}

	return class[0]
}

func pathUnderOrEqual(rel, base string) bool {
	rel = normalizeRel(rel)
	base = normalizeRel(base)

	return base == "." || rel == base || strings.HasPrefix(rel, base+"/")
}

func relFromMount(mountRelRoot, rel string) string {
	rel = normalizeRel(rel)
	mountRelRoot = normalizeRel(mountRelRoot)
	if mountRelRoot == "." {
		return rel
	}

	if rel == mountRelRoot {
		return "."
	}

	if strings.HasPrefix(rel, mountRelRoot+"/") {
		return strings.TrimPrefix(rel, mountRelRoot+"/")
	}

	return rel
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

		if err := pathutil.RemoveAll(target); err != nil && !errors.Is(err, os.ErrNotExist) {
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

		_ = pathutil.RemoveAll(target)
		if err := pathutil.Symlink(entry.Linkname, target); err != nil {
			if isUnsupportedWindowsSymlink(err) {
				return nil
			}

			return fmt.Errorf("symlink %s: %w", entry.Path, err)
		}
	case KindFile:
		return nil
	default:
		return fmt.Errorf("unsupported entry kind %q for %s", entry.Kind, entry.Path)
	}

	return nil
}

func isUnsupportedWindowsSymlink(err error) bool {
	return runtime.GOOS == "windows" && strings.Contains(strings.ToLower(err.Error()), "privilege")
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

	if err := pathutil.RemoveAll(target); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace file %s: %w", entry.Path, err)
	}

	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename file %s: %w", entry.Path, err)
	}

	return setFileModTime(target, entry.ModTime)
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

func joinRel(base, rel string) string {
	if base == "." {
		return rel
	}

	if rel == "." {
		return base
	}

	return path.Join(base, rel)
}

func relativeUnder(base, abs string) (string, error) {
	base = filepath.Clean(base)

	abs = filepath.Clean(abs)
	if abs == base {
		return ".", nil
	}

	prefix := base
	if !strings.HasSuffix(prefix, string(filepath.Separator)) {
		prefix += string(filepath.Separator)
	}

	if strings.HasPrefix(abs, prefix) {
		return normalizeRel(abs[len(prefix):]), nil
	}

	rel, err := filepath.Rel(base, abs)
	if err != nil {
		return "", err
	}

	return normalizeRel(rel), nil
}

type excludeMatcher struct {
	patterns [][]string
}

type includeMatcher struct {
	all      bool
	literals []string
	patterns [][]string
}

func newIncludeMatcher(includes []string) includeMatcher {
	if includes == nil {
		return includeMatcher{all: true}
	}

	matcher := includeMatcher{}

	for _, include := range includes {
		raw := strings.TrimSpace(include)
		if raw == "" {
			continue
		}

		if raw == "." || raw == "/" {
			return includeMatcher{all: true}
		}

		normalized := normalizePattern(raw)
		if normalized == "" || normalized == "." {
			return includeMatcher{all: true}
		}

		if hasGlob(normalized) {
			matcher.patterns = append(matcher.patterns, splitPath(normalized))
			continue
		}

		matcher.literals = append(matcher.literals, normalized)
	}

	return matcher
}

func (m includeMatcher) Match(rel string) bool {
	if m.all {
		return true
	}

	rel = normalizeRel(rel)
	for _, literal := range m.literals {
		if rel == literal || strings.HasPrefix(rel, literal+"/") {
			return true
		}
	}

	parts := splitPath(rel)
	for _, pattern := range m.patterns {
		if matchSegments(pattern, parts) {
			return true
		}
	}

	return false
}

func hasGlob(pattern string) bool {
	return strings.ContainsAny(pattern, "*?[")
}

func newExcludeMatcher(patterns []string) excludeMatcher {
	matcher := excludeMatcher{patterns: make([][]string, 0, len(patterns))}
	for _, pattern := range patterns {
		pattern = normalizePattern(pattern)
		if pattern == "" {
			continue
		}

		matcher.patterns = append(matcher.patterns, splitPath(pattern))
	}

	return matcher
}

func (m excludeMatcher) Match(relRoot, relMount string) bool {
	for _, pattern := range m.patterns {
		if relRoot != "." && matchSegments(pattern, splitPath(relRoot)) {
			return true
		}

		if relMount != "." && matchSegments(pattern, splitPath(relMount)) {
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

	if strings.HasSuffix(pattern, "/") {
		pattern = strings.TrimRight(pattern, "/") + "/**"
	}

	pattern = filepath.ToSlash(filepath.Clean(pattern))

	return pattern
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

func HashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file %s: %w", path, err)
	}

	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash file %s: %w", path, err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func sameEntry(a, b Entry, ignoreMode bool) bool {
	return sameEntryExceptMode(a, b) && (ignoreMode || a.Mode == b.Mode)
}

func sameEntryExceptMode(a, b Entry) bool {
	return a.Kind == b.Kind && a.Hash == b.Hash && a.Size == b.Size && a.Linkname == b.Linkname
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
