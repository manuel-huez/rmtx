//nolint:wsl_v5
package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/manuel-huez/rmtx/internal/oci"
	"github.com/manuel-huez/rmtx/internal/pathutil"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/syncfs"
)

func (s *Server) handleContextArtifacts(
	conn *protocol.Conn,
	req protocol.ContextArtifactsRequest,
	requestLogs *hostLogSubscription,
) error {
	contextID, err := normalizeContextID(req.ContextID)
	if err != nil {
		return err
	}

	if req.Delete || req.Prune {
		release := s.acquireContext(contextID)
		defer release()
	}

	dataDir, err := s.contextDataDir(contextID)
	if err != nil {
		return err
	}
	runtimeDir, err := s.contextRuntimeDir(contextID)
	if err != nil {
		return err
	}

	if _, err := os.Stat(s.contextMetaDir(contextID)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("context %s not found", contextID)
		}

		return err
	}

	var deleted []protocol.ContextArtifact
	if req.Delete {
		deleted, err = s.deleteContextArtifact(runtimeDir, req)
		if err != nil {
			return err
		}
	}

	if req.Prune {
		pruned, err := s.pruneContextArtifacts(runtimeDir)
		if err != nil {
			return err
		}

		deleted = append(deleted, pruned...)

		cachePruned, _, err := s.pruneUnreferencedOCICache()
		if err != nil {
			return err
		}
		deleted = append(deleted, cachePruned...)
	}

	artifacts, err := s.listContextArtifacts(contextID, dataDir, runtimeDir)
	if err != nil {
		return err
	}

	return writeJSONAfterLogs(
		conn,
		requestLogs,
		protocol.MsgContextArtifactsResponse,
		protocol.ContextArtifactsResponse{
			ContextID: contextID,
			Artifacts: artifacts,
			Deleted:   deleted,
		},
	)
}

func (s *Server) deleteContextArtifact(
	runtimeDir string,
	req protocol.ContextArtifactsRequest,
) ([]protocol.ContextArtifact, error) {
	volume := strings.TrimSpace(req.Volume)
	if volume == "" {
		return nil, errors.New("context artifacts delete requires --volume")
	}

	if strings.ContainsAny(volume, `/\`) || volume == "." || volume == ".." {
		return nil, fmt.Errorf("invalid volume name %q", volume)
	}

	path := filepath.Join(runtimeDir, "volumes", volume)
	artifact := protocol.ContextArtifact{Kind: "volume", Name: volume, Path: path}
	if err := pathutil.RemoveAll(path); err != nil {
		return nil, err
	}

	if err := removeContextSetupCache(runtimeDir); err != nil {
		return nil, err
	}

	return []protocol.ContextArtifact{artifact}, nil
}

func removeContextSetupCache(runtimeDir string) error {
	path := filepath.Join(runtimeDir, runtimeDirName, contextSetupFile)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return nil
}

func (s *Server) pruneContextArtifacts(runtimeDir string) ([]protocol.ContextArtifact, error) {
	var deleted []protocol.ContextArtifact

	specs, err := pruneRuntimeSpecs(runtimeDir)
	if err != nil {
		return nil, err
	}
	deleted = append(deleted, specs...)

	if err := compactArtifactState(runtimeDir); err != nil {
		return nil, err
	}

	prepared, err := pruneStalePreparedRuntimes(runtimeDir)
	if err != nil {
		return nil, err
	}
	deleted = append(deleted, prepared...)

	return deleted, nil
}

func compactArtifactState(runtimeDir string) error {
	path := filepath.Join(runtimeDir, runtimeDirName, artifactStateFile)
	state, err := loadArtifactState(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(state.Prepared) <= 1 && len(state.Images) <= 1 {
		return nil
	}
	if len(state.Prepared) == 0 {
		state.Images = nil

		return writeIndentedJSON(path, state)
	}

	current := state.Prepared[len(state.Prepared)-1]
	state.Prepared = []artifactPrepared{current}
	state.Images = artifactImagesForDigest(state.Images, current.ImageDigest)
	if len(state.Images) == 0 && current.ImageDigest != "" {
		state.Images = []artifactImage{{
			Reference: current.ImageReference,
			Digest:    current.ImageDigest,
		}}
	}

	return writeIndentedJSON(path, state)
}

func artifactImagesForDigest(images []artifactImage, digest string) []artifactImage {
	var out []artifactImage
	for _, image := range images {
		if image.Digest == digest {
			out = append(out, image)
		}
	}

	if len(out) > 1 {
		return out[len(out)-1:]
	}

	return out
}

func pruneRuntimeSpecs(runtimeDir string) ([]protocol.ContextArtifact, error) {
	var deleted []protocol.ContextArtifact
	specDir := filepath.Join(runtimeDir, runtimeDirName, runtimeSpecDirName)

	err := filepath.WalkDir(specDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return err
		}

		deleted = append(deleted, protocol.ContextArtifact{Kind: "spec", Path: path})

		return os.Remove(path)
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	return deleted, nil
}

func pruneStalePreparedRuntimes(runtimeDir string) ([]protocol.ContextArtifact, error) {
	state, _ := loadArtifactState(filepath.Join(runtimeDir, runtimeDirName, artifactStateFile))
	live := map[string]bool{}
	for _, prepared := range state.Prepared {
		live[prepared.Key] = true
	}

	root := filepath.Join(runtimeDir, runtimeDirName, runtimeRootFSDirName)
	var deleted []protocol.ContextArtifact
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() || live[entry.Name()] {
			continue
		}

		path := filepath.Join(root, entry.Name())
		size, _ := dirSize(path)
		deleted = append(deleted, protocol.ContextArtifact{
			Kind: "prepared-runtime",
			Name: entry.Name(),
			Path: path,
			Size: size,
		})

		if err := pathutil.RemoveAll(path); err != nil {
			return deleted, err
		}
	}

	return deleted, nil
}

func (s *Server) listContextArtifacts(
	contextID string,
	dataDir string,
	runtimeDir string,
) ([]protocol.ContextArtifact, error) {
	var out []protocol.ContextArtifact

	workspace := filepath.Join(dataDir, contextWorkspaceDir)
	if size, err := dirSize(workspace); err == nil {
		out = append(out, protocol.ContextArtifact{
			Kind: "workspace",
			Name: contextID,
			Path: workspace,
			Size: size,
		})
	}

	volumeRoot := filepath.Join(runtimeDir, "volumes")
	volumes, err := os.ReadDir(volumeRoot)
	if err == nil {
		for _, volume := range volumes {
			if !volume.IsDir() {
				continue
			}

			path := filepath.Join(volumeRoot, volume.Name())
			size, _ := dirSize(path)
			out = append(out, protocol.ContextArtifact{
				Kind: "volume",
				Name: volume.Name(),
				Path: path,
				Size: size,
			})
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	state, _ := loadArtifactState(filepath.Join(runtimeDir, runtimeDirName, artifactStateFile))
	for _, image := range state.Images {
		out = append(out, protocol.ContextArtifact{
			Kind:   "image",
			Name:   image.Reference,
			Ref:    image.Digest,
			Size:   ociBlobSize(runtimeDir, image.Blobs),
			Detail: fmt.Sprintf("%d blobs", len(image.Blobs)),
		})
	}

	for _, prepared := range state.Prepared {
		size, _ := dirSize(prepared.Path)
		detail := prepared.ImageReference
		if strings.TrimSpace(prepared.BasePath) != "" {
			detail = fmt.Sprintf("%s base=%s", prepared.ImageReference, prepared.BasePath)
		}
		out = append(out, protocol.ContextArtifact{
			Kind:   "prepared-runtime",
			Name:   prepared.Key,
			Path:   prepared.Path,
			Ref:    prepared.ImageDigest,
			Size:   size,
			Detail: detail,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind == out[j].Kind {
			return out[i].Name < out[j].Name
		}

		return out[i].Kind < out[j].Kind
	})

	return out, nil
}

func (s *Server) handleCachePrune(
	ctx context.Context,
	conn *protocol.Conn,
	requestLogs *hostLogSubscription,
) error {
	deleted, bytes, err := s.pruneAllCaches(ctx)
	if err != nil {
		return err
	}

	return writeJSONAfterLogs(
		conn,
		requestLogs,
		protocol.MsgCachePruneResponse,
		protocol.CachePruneResponse{
			Deleted: deleted,
			Bytes:   bytes,
		},
	)
}

func (s *Server) pruneAllCaches(ctx context.Context) ([]protocol.ContextArtifact, int64, error) {
	deleted, bytes, err := s.pruneUnreferencedOCICache()
	if err != nil {
		return nil, 0, err
	}

	blobDeleted, blobBytes, err := s.pruneUnreferencedBlobs()
	if err != nil {
		return nil, 0, err
	}
	deleted = append(deleted, blobDeleted...)
	bytes += blobBytes

	updateDeleted, updateBytes, err := s.pruneOldUpdateArtifacts()
	if err != nil {
		return nil, 0, err
	}
	deleted = append(deleted, updateDeleted...)
	bytes += updateBytes

	wslDeleted, wslBytes, err := s.pruneWSLStagedRootFS(ctx)
	if err != nil {
		return nil, 0, err
	}
	deleted = append(deleted, wslDeleted...)
	bytes += wslBytes

	return deleted, bytes, nil
}

func (s *Server) pruneUnreferencedOCICache() ([]protocol.ContextArtifact, int64, error) {
	roots, err := s.contextStateRoots()
	if err != nil {
		return nil, 0, err
	}

	return s.pruneUnreferencedOCICacheInRoots(roots.runtime)
}

func (s *Server) pruneUnreferencedOCICacheInRoots(
	roots []string,
) ([]protocol.ContextArtifact, int64, error) {
	s.ociMu.Lock()
	defer s.ociMu.Unlock()

	refsByRoot, err := s.referencedOCIDigestsByRoot()
	if err != nil {
		return nil, 0, err
	}

	var deleted []protocol.ContextArtifact
	var bytes int64

	for _, root := range uniqueCleanPaths(roots) {
		pruned, prunedBytes, err := pruneUnreferencedOCIStore(ociStore(root), refsByRoot[root])
		if err != nil {
			return nil, 0, err
		}
		deleted = append(deleted, pruned...)
		bytes += prunedBytes

		rootfsPruned, rootfsBytes, err := pruneUnreferencedSharedRootFS(
			sharedRootFSRoot(root),
			refsByRoot[root],
		)
		if err != nil {
			return nil, 0, err
		}
		deleted = append(deleted, rootfsPruned...)
		bytes += rootfsBytes
	}

	return deleted, bytes, nil
}

func uniqueCleanPaths(paths []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(paths))
	for _, value := range paths {
		value = filepath.Clean(value)
		if seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}

	return out
}

func pruneUnreferencedOCIStore(
	store *oci.Store,
	refs map[string]bool,
) ([]protocol.ContextArtifact, int64, error) {
	var deleted []protocol.ContextArtifact
	var bytes int64

	for _, root := range []string{
		store.BlobsDir(),
		filepath.Join(store.Root(), "manifests"),
	} {
		if err := walkOCIArtifactFiles(root, func(path string, d os.DirEntry) error {
			digest := digestFromCachePath(root, path)
			if refs[digest] {
				return nil
			}

			return removeCachedOCIArtifact(&deleted, &bytes, d, path, digest)
		}); err != nil {
			return nil, 0, err
		}
	}
	refDeleted, refBytes, err := pruneUnreferencedOCIRefs(store, refs)
	if err != nil {
		return nil, 0, err
	}
	deleted = append(deleted, refDeleted...)
	bytes += refBytes

	return deleted, bytes, nil
}

func pruneUnreferencedOCIRefs(
	store *oci.Store,
	refs map[string]bool,
) ([]protocol.ContextArtifact, int64, error) {
	var deleted []protocol.ContextArtifact
	var bytes int64
	root := filepath.Join(store.Root(), "refs")

	if err := walkOCIArtifactFiles(root, func(path string, d os.DirEntry) error {
		digest, readErr := readOCIRefManifestDigest(path)
		if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			return readErr
		}
		if refs[digest] {
			return nil
		}

		return removeCachedOCIArtifact(&deleted, &bytes, d, path, digest)
	}); err != nil {
		return nil, 0, err
	}

	return deleted, bytes, nil
}

func walkOCIArtifactFiles(root string, visit func(string, os.DirEntry) error) error {
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return err
		}

		return visit(path, d)
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return nil
}

func removeCachedOCIArtifact(
	deleted *[]protocol.ContextArtifact,
	bytes *int64,
	entry os.DirEntry,
	path string,
	digest string,
) error {
	info, statErr := entry.Info()
	if statErr == nil {
		*bytes += info.Size()
	}

	*deleted = append(*deleted, protocol.ContextArtifact{
		Kind: "cache",
		Path: path,
		Ref:  digest,
	})

	return os.Remove(path)
}

func pruneUnreferencedSharedRootFS(
	root string,
	refs map[string]bool,
) ([]protocol.ContextArtifact, int64, error) {
	var deleted []protocol.ContextArtifact
	var bytes int64

	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}

	for _, entry := range entries {
		if !entry.IsDir() || refs[entry.Name()] {
			continue
		}

		path := filepath.Join(root, entry.Name())
		size, _ := dirSize(path)
		deleted = append(deleted, protocol.ContextArtifact{
			Kind: "cache-rootfs",
			Name: entry.Name(),
			Path: path,
			Size: size,
		})
		bytes += size

		if err := pathutil.RemoveAll(path); err != nil {
			return deleted, bytes, err
		}
	}

	return deleted, bytes, nil
}

func readOCIRefManifestDigest(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	var image oci.Image
	if err := json.Unmarshal(content, &image); err != nil {
		return "", err
	}

	return image.ManifestDigest, nil
}

func (s *Server) referencedOCIDigestsByRoot() (map[string]map[string]bool, error) {
	refs := map[string]map[string]bool{}
	entries, err := os.ReadDir(s.contextsRoot())
	if errors.Is(err, os.ErrNotExist) {
		return refs, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list context metadata: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		meta, err := loadContextMetadata(filepath.Join(s.contextsRoot(), entry.Name()))
		if err != nil {
			return nil, err
		}
		root := filepath.Clean(contextRuntimeRoot(meta, s.opts.StateDir))
		if refs[root] == nil {
			refs[root] = map[string]bool{}
		}
		runtimeDir := contextDataDir(root, entry.Name())

		state, err := loadArtifactState(
			filepath.Join(runtimeDir, runtimeDirName, artifactStateFile),
		)
		if err != nil {
			continue
		}

		for _, image := range state.Images {
			refs[root][image.Digest] = true
			for _, blob := range image.Blobs {
				refs[root][blob] = true
			}
		}
		for _, prepared := range state.Prepared {
			if prepared.Key != "" {
				refs[root][prepared.Key] = true
			}
		}
	}

	return refs, nil
}

func (s *Server) pruneUnreferencedBlobs() ([]protocol.ContextArtifact, int64, error) {
	roots, err := s.contextStateRoots()
	if err != nil {
		return nil, 0, err
	}

	return s.pruneUnreferencedBlobsInRoots(append(roots.data, roots.runtime...))
}

func (s *Server) pruneUnreferencedBlobsInRoots(
	roots []string,
) ([]protocol.ContextArtifact, int64, error) {
	s.blobGCMu.Lock()
	defer s.blobGCMu.Unlock()

	refsByRoot, err := s.referencedBlobHashesByRoot()
	if err != nil {
		return nil, 0, err
	}

	var deleted []protocol.ContextArtifact
	var bytes int64

	for _, runtimeRoot := range uniqueCleanPaths(roots) {
		pruned, prunedBytes, err := pruneUnreferencedBlobsInRoot(
			filepath.Join(runtimeRoot, "blobs"),
			refsByRoot[runtimeRoot],
		)
		if err != nil {
			return nil, 0, err
		}
		deleted = append(deleted, pruned...)
		bytes += prunedBytes
	}

	return deleted, bytes, nil
}

func pruneUnreferencedBlobsInRoot(
	root string,
	refs map[string]bool,
) ([]protocol.ContextArtifact, int64, error) {
	var deleted []protocol.ContextArtifact
	var bytes int64

	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return err
		}

		hash := filepath.Base(path)
		if refs[hash] {
			return nil
		}

		info, statErr := d.Info()
		if statErr == nil {
			bytes += info.Size()
		}

		deleted = append(deleted, protocol.ContextArtifact{
			Kind: "blob",
			Path: path,
			Ref:  hash,
		})

		return os.Remove(path)
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, 0, err
	}

	if err := removeEmptyBlobDirs(root); err != nil {
		return nil, 0, err
	}

	return deleted, bytes, nil
}

func (s *Server) referencedBlobHashesByRoot() (map[string]map[string]bool, error) {
	refs := map[string]map[string]bool{}
	entries, err := os.ReadDir(s.contextsRoot())
	if errors.Is(err, os.ErrNotExist) {
		return refs, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list context metadata: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		meta, err := loadContextMetadata(filepath.Join(s.contextsRoot(), entry.Name()))
		if err != nil {
			return nil, err
		}
		root := filepath.Clean(contextDataRoot(meta, s.opts.StateDir))
		if refs[root] == nil {
			refs[root] = map[string]bool{}
		}

		entries, err := s.loadTrackedManifest(entry.Name())
		if err != nil {
			return nil, err
		}

		for _, manifestEntry := range entries {
			if manifestEntry.Kind == syncfs.KindFile && manifestEntry.Hash != "" {
				refs[root][manifestEntry.Hash] = true
			}
		}
	}

	return refs, nil
}

func removeEmptyBlobDirs(root string) error {
	dirs, err := blobDirs(root)
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		if err := removeDirIfEmpty(dir); err != nil {
			return err
		}
	}

	return nil
}

func blobDirs(root string) ([]string, error) {
	var dirs []string
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil || !d.IsDir() || path == root {
			return err
		}

		dirs = append(dirs, path)

		return nil
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	sort.Sort(sort.Reverse(sort.StringSlice(dirs)))

	return dirs, nil
}

func removeDirIfEmpty(dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if err != nil {
		return err
	}

	if len(entries) > 0 {
		return nil
	}

	if err := os.Remove(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return nil
}

func pruneStartupTempFiles(stateDir string) ([]string, error) {
	var removed []string
	targets := []struct {
		root  string
		match func(string) bool
	}{
		{root: filepath.Join(stateDir, "blobs"), match: isSyncBlobTempFile},
		{root: filepath.Join(stateDir, "cache", "oci"), match: isOCICacheTempFile},
	}

	for _, target := range targets {
		pruned, err := pruneTempFilesInRoot(target.root, target.match)
		if err != nil {
			return nil, err
		}

		removed = append(removed, pruned...)
	}

	return removed, nil
}

func pruneTempFilesInRoot(root string, match func(string) bool) ([]string, error) {
	var removed []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return err
		}

		if !match(d.Name()) {
			return nil
		}

		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}

		removed = append(removed, path)

		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	return removed, nil
}

func isSyncBlobTempFile(name string) bool {
	return strings.HasSuffix(name, ".tmp")
}

func isOCICacheTempFile(name string) bool {
	return strings.Contains(name, ".tmp-")
}

func ociBlobSize(runtimeDir string, digests []string) int64 {
	store := ociStore(runtimeRootForContextRuntimeDir(runtimeDir))
	var total int64

	for _, digest := range digests {
		if info, err := os.Stat(store.BlobPath(digest)); err == nil {
			total += info.Size()
			continue
		}

		if info, err := os.Stat(store.ManifestPath(digest)); err == nil {
			total += info.Size()
		}
	}

	return total
}

func dirSize(root string) (int64, error) {
	var total int64

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil {
			return err
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		if !info.IsDir() {
			total += info.Size()
		}

		return nil
	})

	return total, err
}

func digestFromCachePath(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return ""
	}

	base := strings.TrimSuffix(filepath.Base(rel), ".json")
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) == 0 {
		return base
	}

	algo := parts[0]
	if algo == "" {
		algo = "sha256"
	}

	return algo + ":" + strings.TrimSuffix(filepath.Base(rel), ".json")
}
