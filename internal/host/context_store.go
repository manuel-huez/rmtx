package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/manuel-huez/rmtx/internal/pathutil"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/syncfs"
)

const (
	contextDirName      = "contexts"
	contextWorkspaceDir = "workspace"
	contextMetaFile     = "context.json"
	contextManifestFile = "tracked-manifest.json"
)

type contextMetadata struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	RootHint  string    `json:"root_hint,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type contextHandle struct {
	meta      contextMetadata
	dir       string
	workspace string
	created   bool
}

func (s *Server) contextsRoot() string {
	return filepath.Join(s.opts.StateDir, contextDirName)
}

func (s *Server) acquireContext(id string) func() {
	mu := s.contextMutex(id)
	mu.Lock()

	s.activeMu.Lock()
	s.activeContexts[id]++
	s.activeMu.Unlock()

	return func() {
		s.activeMu.Lock()
		if s.activeContexts[id] <= 1 {
			delete(s.activeContexts, id)
		} else {
			s.activeContexts[id]--
		}
		s.activeMu.Unlock()

		mu.Unlock()
	}
}

func (s *Server) contextMutex(id string) *sync.Mutex {
	s.contextLocksMu.Lock()
	defer s.contextLocksMu.Unlock()

	if s.contextLocks == nil {
		s.contextLocks = map[string]*sync.Mutex{}
	}

	mu, ok := s.contextLocks[id]
	if !ok {
		mu = &sync.Mutex{}
		s.contextLocks[id] = mu
	}

	return mu
}

func (s *Server) contextIsActive(id string) bool {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()

	return s.activeContexts[id] > 0
}

func (s *Server) ensureContext(id, name, rootHint string) (contextHandle, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return contextHandle{}, errors.New("context id is required")
	}

	if !validContextID(id) {
		return contextHandle{}, fmt.Errorf("invalid context id %q", id)
	}

	handle := contextHandle{
		dir:       filepath.Join(s.contextsRoot(), id),
		workspace: filepath.Join(s.contextsRoot(), id, contextWorkspaceDir),
	}

	created := false
	if _, err := os.Stat(handle.dir); errors.Is(err, os.ErrNotExist) {
		created = true
	} else if err != nil {
		return contextHandle{}, fmt.Errorf("stat context dir: %w", err)
	}

	if err := os.MkdirAll(handle.workspace, defaultDirMode); err != nil {
		return contextHandle{}, fmt.Errorf("create context workspace: %w", err)
	}

	meta, err := loadContextMetadata(handle.dir)
	if err != nil {
		return contextHandle{}, err
	}

	now := time.Now().UTC()
	if meta.ID == "" {
		meta.ID = id
	}

	if meta.Name == "" {
		meta.Name = fallbackContextName(name, rootHint, id)
	}

	if strings.TrimSpace(name) != "" {
		meta.Name = strings.TrimSpace(name)
	}

	if strings.TrimSpace(rootHint) != "" {
		meta.RootHint = strings.TrimSpace(rootHint)
	}

	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = now
	}

	meta.UpdatedAt = now

	if err := saveContextMetadata(handle.dir, meta); err != nil {
		return contextHandle{}, err
	}

	handle.meta = meta
	handle.created = created

	return handle, nil
}

func loadContextMetadata(dir string) (contextMetadata, error) {
	path := filepath.Join(dir, contextMetaFile)
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return contextMetadata{}, nil
	}
	if err != nil {
		return contextMetadata{}, fmt.Errorf("read context metadata: %w", err)
	}

	var meta contextMetadata
	if err := json.Unmarshal(content, &meta); err != nil {
		return contextMetadata{}, fmt.Errorf("parse context metadata: %w", err)
	}

	return meta, nil
}

func saveContextMetadata(dir string, meta contextMetadata) error {
	return writeJSONAtomically(filepath.Join(dir, contextMetaFile), meta)
}

func (s *Server) loadTrackedManifest(id string) ([]syncfs.Entry, error) {
	path := filepath.Join(s.contextsRoot(), id, contextManifestFile)
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read tracked manifest: %w", err)
	}

	var entries []syncfs.Entry
	if err := json.Unmarshal(content, &entries); err != nil {
		return nil, fmt.Errorf("parse tracked manifest: %w", err)
	}

	return entries, nil
}

func (s *Server) saveTrackedManifest(id string, entries []syncfs.Entry) error {
	return writeJSONAtomically(filepath.Join(s.contextsRoot(), id, contextManifestFile), entries)
}

func writeJSONAtomically(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), defaultDirMode); err != nil {
		return fmt.Errorf("create parent dir: %w", err)
	}

	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(content, '\n'), 0o644); err != nil {
		return fmt.Errorf("write temp json: %w", err)
	}

	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename temp json: %w", err)
	}

	return nil
}

func fallbackContextName(name, rootHint, id string) string {
	if strings.TrimSpace(name) != "" {
		return strings.TrimSpace(name)
	}
	if strings.TrimSpace(rootHint) != "" {
		return strings.TrimSpace(rootHint)
	}
	return id
}

func validContextID(id string) bool {
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return false
		}
	}

	return true
}

func (s *Server) listContexts() ([]protocol.ContextSummary, error) {
	entries, err := os.ReadDir(s.contextsRoot())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list contexts: %w", err)
	}

	out := make([]protocol.ContextSummary, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		id := entry.Name()
		meta, err := loadContextMetadata(filepath.Join(s.contextsRoot(), id))
		if err != nil {
			return nil, err
		}

		if meta.ID == "" {
			meta.ID = id
		}
		if meta.Name == "" {
			meta.Name = id
		}

		out = append(out, protocol.ContextSummary{
			ID:        meta.ID,
			Name:      meta.Name,
			Path:      filepath.Join(s.contextsRoot(), id),
			Workspace: filepath.Join(s.contextsRoot(), id, contextWorkspaceDir),
			CreatedAt: meta.CreatedAt,
			UpdatedAt: meta.UpdatedAt,
			RootHint:  meta.RootHint,
			Active:    s.contextIsActive(meta.ID),
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return out[i].ID < out[j].ID
		}

		return out[i].Name < out[j].Name
	})

	return out, nil
}

func (s *Server) deleteContexts(req protocol.DeleteContextsRequest) (protocol.DeleteContextsResponse, error) {
	contexts, err := s.listContexts()
	if err != nil {
		return protocol.DeleteContextsResponse{}, err
	}

	targets := map[string]protocol.ContextSummary{}
	if req.All {
		for _, context := range contexts {
			targets[context.ID] = context
		}
	}

	if strings.TrimSpace(req.OlderThan) != "" {
		olderThan, err := time.ParseDuration(req.OlderThan)
		if err != nil {
			return protocol.DeleteContextsResponse{}, fmt.Errorf("parse older-than: %w", err)
		}

		cutoff := time.Now().UTC().Add(-olderThan)
		for _, context := range contexts {
			if !context.UpdatedAt.IsZero() && context.UpdatedAt.Before(cutoff) {
				targets[context.ID] = context
			}
		}
	}

	contextByID := map[string]protocol.ContextSummary{}
	for _, context := range contexts {
		contextByID[context.ID] = context
	}

	var notFound []string
	for _, id := range req.IDs {
		context, ok := contextByID[id]
		if !ok {
			notFound = append(notFound, id)
			continue
		}

		targets[id] = context
	}

	deleted := make([]protocol.ContextSummary, 0, len(targets))
	for id, context := range targets {
		release := s.acquireContext(id)
		err := os.RemoveAll(context.Path)
		release()
		if err != nil {
			return protocol.DeleteContextsResponse{}, fmt.Errorf("delete context %s: %w", id, err)
		}

		deleted = append(deleted, context)
	}

	sort.Slice(deleted, func(i, j int) bool { return deleted[i].ID < deleted[j].ID })
	sort.Strings(notFound)

	return protocol.DeleteContextsResponse{
		Deleted:  deleted,
		NotFound: notFound,
	}, nil
}

func (s *Server) syncContextFromClient(
	conn *protocol.Conn,
	contextID string,
	workspace string,
	current []syncfs.Entry,
	target []syncfs.Entry,
) error {
	changed, deleted := syncfs.Diff(current, target)

	missing := s.blobStore.MissingHashes(changed)
	if err := conn.WriteJSON(
		protocol.MsgNeedBlobs,
		protocol.NeedBlobs{Hashes: missing},
	); err != nil {
		return err
	}

	if err := s.receiveBlobs(conn); err != nil {
		return err
	}

	if err := syncfs.DeletePaths(workspace, deleted); err != nil {
		return fmt.Errorf("delete tracked paths in context %s: %w", contextID, err)
	}

	if err := syncfs.ApplyNonFileEntries(workspace, filterChangedNonFileEntries(changed)); err != nil {
		return fmt.Errorf("apply non-file entries in context %s: %w", contextID, err)
	}

	for _, entry := range changed {
		if entry.Kind != syncfs.KindFile {
			continue
		}

		targetPath, err := pathutil.SecureJoin(workspace, filepath.FromSlash(entry.Path))
		if err != nil {
			return err
		}

		mode := os.FileMode(entry.Mode)
		if mode == 0 {
			mode = defaultFileMode
		}

		if err := s.blobStore.Materialize(
			entry.Hash,
			targetPath,
			mode,
		); err != nil {
			return fmt.Errorf("materialize %s in context %s: %w", entry.Path, contextID, err)
		}
	}

	return nil
}

func filterChangedNonFileEntries(entries []syncfs.Entry) []syncfs.Entry {
	nonFiles := make([]syncfs.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Kind != syncfs.KindFile {
			nonFiles = append(nonFiles, entry)
		}
	}

	return nonFiles
}
