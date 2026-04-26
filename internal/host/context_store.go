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
	contextFileMode     = 0o644
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
	id, err := normalizeContextID(id)
	if err != nil {
		return contextHandle{}, err
	}

	handle := contextHandle{
		dir:       filepath.Join(s.contextsRoot(), id),
		workspace: filepath.Join(s.contextsRoot(), id, contextWorkspaceDir),
	}

	created, err := contextDirCreated(handle.dir)
	if err != nil {
		return contextHandle{}, err
	}

	if err := os.MkdirAll(handle.workspace, defaultDirMode); err != nil {
		return contextHandle{}, fmt.Errorf("create context workspace: %w", err)
	}

	meta, err := loadContextMetadata(handle.dir)
	if err != nil {
		return contextHandle{}, err
	}

	meta = hydrateContextMetadata(meta, id, name, rootHint, time.Now().UTC())

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
	if err := os.WriteFile(tmp, append(content, '\n'), contextFileMode); err != nil {
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

func normalizeContextID(id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", errors.New("context id is required")
	}

	if !validContextID(id) {
		return "", fmt.Errorf("invalid context id %q", id)
	}

	return id, nil
}

func contextDirCreated(dir string) (bool, error) {
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("stat context dir: %w", err)
	}

	return false, nil
}

func hydrateContextMetadata(
	meta contextMetadata,
	id, name, rootHint string,
	now time.Time,
) contextMetadata {
	name = strings.TrimSpace(name)
	rootHint = strings.TrimSpace(rootHint)

	if meta.ID == "" {
		meta.ID = id
	}

	if meta.Name == "" {
		meta.Name = fallbackContextName(name, rootHint, id)
	}

	if name != "" {
		meta.Name = name
	}

	if rootHint != "" {
		meta.RootHint = rootHint
	}

	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = now
	}

	meta.UpdatedAt = now

	return meta
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

func (s *Server) deleteContexts(
	req protocol.DeleteContextsRequest,
) (protocol.DeleteContextsResponse, error) {
	contexts, err := s.listContexts()
	if err != nil {
		return protocol.DeleteContextsResponse{}, err
	}

	targets, notFound, err := selectDeleteTargets(req, contexts)
	if err != nil {
		return protocol.DeleteContextsResponse{}, err
	}

	deleted, err := s.removeContexts(targets)
	if err != nil {
		return protocol.DeleteContextsResponse{}, err
	}

	sort.Slice(deleted, func(i, j int) bool { return deleted[i].ID < deleted[j].ID })
	sort.Strings(notFound)

	return protocol.DeleteContextsResponse{
		Deleted:  deleted,
		NotFound: notFound,
	}, nil
}

func selectDeleteTargets(
	req protocol.DeleteContextsRequest,
	contexts []protocol.ContextSummary,
) (map[string]protocol.ContextSummary, []string, error) {
	targets := map[string]protocol.ContextSummary{}
	if req.All {
		addAllTargets(targets, contexts)
	}

	if err := addOlderThanTargets(targets, req.OlderThan, contexts); err != nil {
		return nil, nil, err
	}

	contextByID := mapContextsByID(contexts)
	notFound := addExplicitIDTargets(targets, req.IDs, contextByID)

	return targets, notFound, nil
}

func addAllTargets(targets map[string]protocol.ContextSummary, contexts []protocol.ContextSummary) {
	for _, context := range contexts {
		targets[context.ID] = context
	}
}

func addOlderThanTargets(
	targets map[string]protocol.ContextSummary,
	olderThanValue string,
	contexts []protocol.ContextSummary,
) error {
	if strings.TrimSpace(olderThanValue) == "" {
		return nil
	}

	olderThan, err := time.ParseDuration(olderThanValue)
	if err != nil {
		return fmt.Errorf("parse older-than: %w", err)
	}

	cutoff := time.Now().UTC().Add(-olderThan)
	for _, context := range contexts {
		if !context.UpdatedAt.IsZero() && context.UpdatedAt.Before(cutoff) {
			targets[context.ID] = context
		}
	}

	return nil
}

func mapContextsByID(contexts []protocol.ContextSummary) map[string]protocol.ContextSummary {
	contextByID := make(map[string]protocol.ContextSummary, len(contexts))
	for _, context := range contexts {
		contextByID[context.ID] = context
	}

	return contextByID
}

func addExplicitIDTargets(
	targets map[string]protocol.ContextSummary,
	ids []string,
	contextByID map[string]protocol.ContextSummary,
) []string {
	var notFound []string

	for _, id := range ids {
		context, ok := contextByID[id]
		if !ok {
			notFound = append(notFound, id)
			continue
		}

		targets[id] = context
	}

	return notFound
}

func (s *Server) removeContexts(
	targets map[string]protocol.ContextSummary,
) ([]protocol.ContextSummary, error) {
	deleted := make([]protocol.ContextSummary, 0, len(targets))
	for id, context := range targets {
		if err := s.removeContextDir(id, context.Path); err != nil {
			return nil, err
		}

		deleted = append(deleted, context)
	}

	return deleted, nil
}

func (s *Server) removeContextDir(id, path string) error {
	release := s.acquireContext(id)
	err := os.RemoveAll(path)

	release()

	if err != nil {
		return fmt.Errorf("delete context %s: %w", id, err)
	}

	return nil
}

func (s *Server) syncContextFromClient(
	ctx context.Context,
	conn *protocol.Conn,
	contextID string,
	session string,
	workspace string,
	current []syncfs.Entry,
	target []syncfs.Entry,
) error {
	changed, deleted := syncfs.Diff(current, target)

	missing := s.blobStore.MissingHashes(changed)

	upload, err := s.prepareBlobUploadSession(contextID, session, missing)
	if err != nil {
		return err
	}

	defer s.unregisterBlobUploadSession(upload.token)

	s.logger.Printf(
		"sync from client started: context=%s session=%s changed=%d deleted=%d missing_blobs=%d",
		contextID,
		session,
		len(changed),
		len(deleted),
		len(missing),
	)

	if err := conn.WriteJSON(
		protocol.MsgNeedBlobs,
		needBlobsMessage(missing, upload),
	); err != nil {
		return err
	}

	if err := s.receiveBlobs(ctx, conn, contextID, session, len(missing), upload); err != nil {
		return err
	}

	s.logger.Printf(
		"applying client sync to workspace: context=%s session=%s deleted=%d changed=%d",
		contextID,
		session,
		len(deleted),
		len(changed),
	)

	if err := syncfs.DeletePaths(workspace, deleted); err != nil {
		return fmt.Errorf("delete tracked paths in context %s: %w", contextID, err)
	}

	if err := syncfs.ApplyNonFileEntries(
		workspace,
		syncfs.NonFileEntries(changed),
	); err != nil {
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
			entry.ModTime,
		); err != nil {
			return fmt.Errorf("materialize %s in context %s: %w", entry.Path, contextID, err)
		}
	}

	s.logger.Printf("sync from client complete: context=%s session=%s", contextID, session)

	return nil
}

func (s *Server) prepareBlobUploadSession(
	contextID string,
	session string,
	missing []string,
) (*blobUploadSession, error) {
	token, err := protocol.RandomNonce()
	if err != nil {
		return nil, err
	}

	upload := newBlobUploadSession(contextID, session, token, missing)
	s.registerBlobUploadSession(upload)

	return upload, nil
}

func needBlobsMessage(missing []string, upload *blobUploadSession) protocol.NeedBlobs {
	msg := protocol.NeedBlobs{Hashes: missing}
	if upload != nil {
		msg.UploadToken = upload.token
		msg.Parallel = blobUploadParallel
	}

	return msg
}
