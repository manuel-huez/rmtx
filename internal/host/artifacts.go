//nolint:wsl_v5
package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/manuel-huez/rmtx/internal/oci"
	"github.com/manuel-huez/rmtx/internal/protocol"
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

	contextDir := filepath.Join(s.contextsRoot(), contextID)
	if req.Delete || req.Prune {
		release := s.acquireContext(contextID)
		defer release()
	}

	if _, err := os.Stat(contextDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("context %s not found", contextID)
		}

		return err
	}

	var deleted []protocol.ContextArtifact
	if req.Delete {
		deleted, err = s.deleteContextArtifact(contextDir, req)
		if err != nil {
			return err
		}
	}

	if req.Prune {
		pruned, err := s.pruneContextArtifacts(contextDir)
		if err != nil {
			return err
		}

		deleted = append(deleted, pruned...)
	}

	artifacts, err := s.listContextArtifacts(contextID, contextDir)
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
	contextDir string,
	req protocol.ContextArtifactsRequest,
) ([]protocol.ContextArtifact, error) {
	volume := strings.TrimSpace(req.Volume)
	if volume == "" {
		return nil, errors.New("context artifacts delete requires --volume")
	}

	if strings.ContainsAny(volume, `/\`) || volume == "." || volume == ".." {
		return nil, fmt.Errorf("invalid volume name %q", volume)
	}

	path := filepath.Join(contextDir, "volumes", volume)
	artifact := protocol.ContextArtifact{Kind: "volume", Name: volume, Path: path}
	if err := os.RemoveAll(path); err != nil {
		return nil, err
	}

	if err := removeContextSetupCache(contextDir); err != nil {
		return nil, err
	}

	return []protocol.ContextArtifact{artifact}, nil
}

func removeContextSetupCache(contextDir string) error {
	path := filepath.Join(contextDir, runtimeDirName, contextSetupFile)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return nil
}

func (s *Server) pruneContextArtifacts(contextDir string) ([]protocol.ContextArtifact, error) {
	var deleted []protocol.ContextArtifact

	specDir := filepath.Join(contextDir, runtimeDirName, runtimeSpecDirName)
	if err := filepath.WalkDir(specDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return err
		}

		deleted = append(deleted, protocol.ContextArtifact{Kind: "spec", Path: path})

		return os.Remove(path)
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	return deleted, nil
}

func (s *Server) listContextArtifacts(
	contextID string,
	contextDir string,
) ([]protocol.ContextArtifact, error) {
	var out []protocol.ContextArtifact

	workspace := filepath.Join(contextDir, contextWorkspaceDir)
	if size, err := dirSize(workspace); err == nil {
		out = append(out, protocol.ContextArtifact{
			Kind: "workspace",
			Name: contextID,
			Path: workspace,
			Size: size,
		})
	}

	volumeRoot := filepath.Join(contextDir, "volumes")
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

	state, _ := loadArtifactState(filepath.Join(contextDir, runtimeDirName, artifactStateFile))
	for _, image := range state.Images {
		out = append(out, protocol.ContextArtifact{
			Kind:   "image",
			Name:   image.Reference,
			Ref:    image.Digest,
			Size:   s.ociBlobSize(image.Blobs),
			Detail: fmt.Sprintf("%d blobs", len(image.Blobs)),
		})
	}

	for _, prepared := range state.Prepared {
		size, _ := dirSize(prepared.Path)
		out = append(out, protocol.ContextArtifact{
			Kind:   "prepared-runtime",
			Name:   prepared.Key,
			Path:   prepared.Path,
			Ref:    prepared.ImageDigest,
			Size:   size,
			Detail: prepared.ImageReference,
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
	conn *protocol.Conn,
	requestLogs *hostLogSubscription,
) error {
	deleted, bytes, err := s.pruneUnreferencedOCICache()
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

func (s *Server) pruneUnreferencedOCICache() ([]protocol.ContextArtifact, int64, error) {
	s.ociMu.Lock()
	defer s.ociMu.Unlock()

	refs, err := s.referencedOCIDigests()
	if err != nil {
		return nil, 0, err
	}

	var deleted []protocol.ContextArtifact
	var bytes int64

	store := s.ociStore()

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

func (s *Server) referencedOCIDigests() (map[string]bool, error) {
	refs := map[string]bool{}
	contexts, err := s.listContexts()
	if err != nil {
		return nil, err
	}

	for _, context := range contexts {
		state, err := loadArtifactState(
			filepath.Join(context.Path, runtimeDirName, artifactStateFile),
		)
		if err != nil {
			continue
		}

		for _, image := range state.Images {
			refs[image.Digest] = true
			for _, blob := range image.Blobs {
				refs[blob] = true
			}
		}
	}

	return refs, nil
}

func (s *Server) ociBlobSize(digests []string) int64 {
	store := s.ociStore()
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
