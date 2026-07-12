package syncfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/manuel-huez/rmtx/internal/pathutil"
)

type ApplyOptions struct {
	OnWrite func(int)
	OnFile  func(Entry)
}

type changeBackup struct {
	original string
	stored   string
}

type applyTransaction struct {
	root        string
	temp        string
	backups     []changeBackup
	createdDirs []string
	installed   []string
	dirModes    map[string]fs.FileMode
}

// ApplyChanges stages files and restores every replaced path if commit fails.
func ApplyChanges(
	ctx context.Context,
	root string,
	store *BlobStore,
	entries []Entry,
	deleted []string,
	opts ApplyOptions,
) error {
	entries, deleted, err := cleanApplyChanges(entries, deleted)
	if err != nil || len(entries) == 0 && len(deleted) == 0 {
		return err
	}

	if store == nil {
		return errors.New("blob store is required")
	}

	temp, err := os.MkdirTemp(root, ".rmtx-apply-*")
	if err != nil {
		return fmt.Errorf("create sync transaction: %w", err)
	}
	defer func() { _ = pathutil.RemoveAll(temp) }()

	stage := filepath.Join(temp, "stage")
	if err := stageChanges(ctx, stage, store, entries, opts); err != nil {
		return err
	}

	tx := applyTransaction{root: root, temp: temp, dirModes: map[string]fs.FileMode{}}

	err = tx.commit(entries, deleted)
	if err == nil {
		return nil
	}

	if rollbackErr := tx.rollback(); rollbackErr != nil {
		return errors.Join(err, fmt.Errorf("rollback sync changes: %w", rollbackErr))
	}

	return err
}

//nolint:cyclop // Validate the complete change set before touching disk.
func cleanApplyChanges(entries []Entry, deleted []string) ([]Entry, []string, error) {
	cleaned := make([]Entry, 0, len(entries))

	kinds := make(map[string]EntryKind, len(entries))
	for _, entry := range entries {
		rel, err := localApplyPath(entry.Path)
		if err != nil {
			return nil, nil, err
		}

		if rel == "." && entry.Kind != KindDir {
			return nil, nil, fmt.Errorf("entry %q cannot replace workspace root", entry.Path)
		}

		if _, exists := kinds[rel]; exists {
			return nil, nil, fmt.Errorf("duplicate sync entry %q", entry.Path)
		}

		if entry.Kind != KindFile && entry.Kind != KindDir && entry.Kind != KindSymlink {
			return nil, nil, fmt.Errorf("unsupported entry kind %q for %s", entry.Kind, entry.Path)
		}

		entry.Path = filepath.ToSlash(rel)
		kinds[rel] = entry.Kind
		cleaned = append(cleaned, entry)
	}

	for rel := range kinds {
		for parent := filepath.Dir(rel); parent != "."; parent = filepath.Dir(parent) {
			if kind, exists := kinds[parent]; exists && kind != KindDir {
				return nil, nil, fmt.Errorf("entry %q is nested below %s", rel, kind)
			}
		}
	}

	cleanedDeleted := make([]string, 0, len(deleted))

	seen := make(map[string]struct{}, len(deleted))
	for _, name := range deleted {
		rel, err := localApplyPath(name)
		if err != nil {
			return nil, nil, err
		}

		if rel == "." {
			continue
		}

		if _, exists := seen[rel]; !exists {
			seen[rel] = struct{}{}
			cleanedDeleted = append(cleanedDeleted, filepath.ToSlash(rel))
		}
	}

	return cleaned, cleanedDeleted, nil
}

func localApplyPath(name string) (string, error) {
	rel := filepath.Clean(filepath.FromSlash(strings.TrimSpace(name)))
	if !filepath.IsLocal(rel) {
		return "", fmt.Errorf("path %q escapes workspace", name)
	}

	return rel, nil
}

//nolint:cyclop // Each entry kind has distinct staging invariants.
func stageChanges(
	ctx context.Context,
	stage string,
	store *BlobStore,
	entries []Entry,
	opts ApplyOptions,
) error {
	if err := os.MkdirAll(stage, defaultDirMode); err != nil {
		return err
	}

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}

		target := filepath.Join(stage, filepath.FromSlash(entry.Path))
		switch entry.Kind {
		case KindDir:
			if entry.Path != "." {
				if err := os.MkdirAll(target, fileMode(entry.Mode, defaultDirMode)); err != nil {
					return fmt.Errorf("stage dir %s: %w", entry.Path, err)
				}
			}
		case KindFile:
			if err := store.MaterializeWithProgress(
				entry.Hash,
				target,
				fileMode(entry.Mode, defaultFileMode),
				entry.ModTime,
				opts.OnWrite,
			); err != nil {
				return fmt.Errorf("stage file %s: %w", entry.Path, err)
			}

			if opts.OnFile != nil {
				opts.OnFile(entry)
			}
		case KindSymlink:
			if err := os.MkdirAll(filepath.Dir(target), defaultDirMode); err != nil {
				return err
			}

			if err := pathutil.Symlink(entry.Linkname, target); err != nil {
				return fmt.Errorf("stage symlink %s: %w", entry.Path, err)
			}
		}
	}

	return nil
}

//nolint:cyclop,gocognit // Commit phases keep rollback state explicit and auditable.
func (t *applyTransaction) commit(entries []Entry, deleted []string) error {
	type target struct {
		deleted bool
		kind    EntryKind
	}

	targets := make(map[string]target, len(entries)+len(deleted))
	for _, name := range deleted {
		targets[filepath.FromSlash(name)] = target{deleted: true}
	}

	for _, entry := range entries {
		name := filepath.FromSlash(entry.Path)
		current := targets[name]
		current.kind = entry.Kind
		targets[name] = current
	}

	paths := make([]string, 0, len(targets))
	for name := range targets {
		if name != "." {
			paths = append(paths, name)
		}
	}

	sort.Slice(paths, func(i, j int) bool { return depth(paths[i]) < depth(paths[j]) })

	for _, name := range paths {
		path := filepath.Join(t.root, name)
		if err := rejectSymlinkParents(t.root, name); err != nil {
			return err
		}

		if coveredByChangeBackup(path, t.backups) {
			continue
		}

		info, err := os.Lstat(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}

		if err != nil {
			return err
		}

		item := targets[name]
		if !item.deleted && item.kind == KindDir && info.IsDir() {
			continue
		}

		stored := filepath.Join(t.temp, "backup", name)
		if err := os.MkdirAll(filepath.Dir(stored), defaultDirMode); err != nil {
			return err
		}

		if err := os.Rename(path, stored); err != nil {
			return err
		}

		t.backups = append(t.backups, changeBackup{original: path, stored: stored})
	}

	for _, entry := range entries {
		if entry.Kind != KindDir || entry.Path == "." {
			continue
		}

		path := filepath.Join(t.root, filepath.FromSlash(entry.Path))
		if info, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
			t.createdDirs = append(t.createdDirs, path)
		} else if err != nil {
			return err
		} else {
			t.dirModes[path] = info.Mode().Perm()
		}

		if err := os.MkdirAll(path, defaultDirMode); err != nil {
			return err
		}
	}

	for _, entry := range entries {
		if entry.Kind == KindDir {
			continue
		}

		name := filepath.FromSlash(entry.Path)

		target := filepath.Join(t.root, name)
		for parent := filepath.Dir(target); parent != t.root; parent = filepath.Dir(parent) {
			if _, err := os.Lstat(parent); errors.Is(err, fs.ErrNotExist) {
				t.createdDirs = append(t.createdDirs, parent)
				continue
			}

			break
		}

		if err := os.MkdirAll(filepath.Dir(target), defaultDirMode); err != nil {
			return err
		}

		if err := os.Rename(filepath.Join(t.temp, "stage", name), target); err != nil {
			return err
		}

		t.installed = append(t.installed, target)
	}

	for _, entry := range entries {
		if entry.Kind == KindDir && entry.Path != "." {
			path := filepath.Join(t.root, filepath.FromSlash(entry.Path))
			if err := pathutil.Chmod(path, fileMode(entry.Mode, defaultDirMode)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *applyTransaction) rollback() error {
	var out error
	for _, path := range t.installed {
		out = errors.Join(out, pathutil.RemoveAll(path))
	}

	for _, backup := range t.backups {
		out = errors.Join(out, pathutil.RemoveAll(backup.original))
		if err := os.MkdirAll(filepath.Dir(backup.original), defaultDirMode); err != nil {
			out = errors.Join(out, err)
			continue
		}

		out = errors.Join(out, os.Rename(backup.stored, backup.original))
	}

	for path, mode := range t.dirModes {
		out = errors.Join(out, pathutil.Chmod(path, mode))
	}

	sort.Slice(
		t.createdDirs,
		func(i, j int) bool { return depth(t.createdDirs[i]) > depth(t.createdDirs[j]) },
	)

	for _, path := range t.createdDirs {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			out = errors.Join(out, err)
		}
	}

	return out
}

func rejectSymlinkParents(root, name string) error {
	for parent := filepath.Dir(name); parent != "."; parent = filepath.Dir(parent) {
		info, err := os.Lstat(filepath.Join(root, parent))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}

		if err != nil {
			return err
		}

		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("workspace parent %s is a symlink", parent)
		}
	}

	return nil
}

func coveredByChangeBackup(path string, backups []changeBackup) bool {
	for _, backup := range backups {
		rel, err := filepath.Rel(backup.original, path)
		if err == nil && filepath.IsLocal(rel) {
			return true
		}
	}

	return false
}
