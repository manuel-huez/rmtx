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
	contextCleanFile    = "workspace-cleaned"
	contextFileMode     = 0o644
)

type contextMetadata struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	RootHint string `json:"root_hint,omitempty"`
	// DataRoot is the runtime data root; empty means the host StateDir.
	DataRoot  string    `json:"data_root,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type contextHandle struct {
	meta      contextMetadata
	metaDir   string
	dataDir   string
	storage   runtimeStorage
	workspace string
	created   bool
}

func (s *Server) contextsRoot() string {
	return filepath.Join(s.opts.StateDir, contextDirName)
}

func (s *Server) contextMetaDir(id string) string {
	return filepath.Join(s.contextsRoot(), id)
}

func (s *Server) contextDataDir(id string) (string, error) {
	metaDir := s.contextMetaDir(id)
	if _, err := os.Stat(metaDir); errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("context %s not found", id)
	} else if err != nil {
		return "", fmt.Errorf("stat context metadata: %w", err)
	}

	meta, err := loadContextMetadata(metaDir)
	if err != nil {
		return "", err
	}

	return contextDataDir(contextDataRoot(meta, s.opts.StateDir), id), nil
}

func contextDataDir(root, id string) string {
	return filepath.Join(root, contextDirName, id)
}

func runtimeRootForContextDataDir(dir string) string {
	return filepath.Dir(filepath.Dir(dir))
}

func contextDataRoot(meta contextMetadata, fallback string) string {
	root := strings.TrimSpace(meta.DataRoot)
	if root == "" {
		return fallback
	}

	return root
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

func (s *Server) ensureContext(
	id, name, rootHint string,
	storage runtimeStorage,
) (contextHandle, error) {
	id, err := normalizeContextID(id)
	if err != nil {
		return contextHandle{}, err
	}

	metaDir := s.contextMetaDir(id)
	dataDir := contextDataDir(storage.root, id)
	handle := contextHandle{
		metaDir: metaDir,
		dataDir: dataDir,
		storage: storage,
	}
	handle.workspace = filepath.Join(handle.dataDir, contextWorkspaceDir)

	meta, err := loadContextMetadata(metaDir)
	if err != nil {
		return contextHandle{}, err
	}

	oldDataDir := contextDataDir(contextDataRoot(meta, s.opts.StateDir), id)
	meta = hydrateContextMetadata(meta, id, name, rootHint, time.Now().UTC())
	if samePath(storage.root, s.opts.StateDir) {
		meta.DataRoot = ""
	} else {
		meta.DataRoot = storage.root
	}

	if err := resetContextDataAfterRootChange(metaDir, oldDataDir, handle.dataDir); err != nil {
		return contextHandle{}, err
	}

	created, err := contextDirCreated(handle.dataDir)
	if err != nil {
		return contextHandle{}, err
	}

	if err := os.MkdirAll(handle.workspace, defaultDirMode); err != nil {
		return contextHandle{}, fmt.Errorf("create context workspace: %w", err)
	}

	if err := saveContextMetadata(metaDir, meta); err != nil {
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

func (s *Server) workspaceWasCleaned(id string) bool {
	path := filepath.Join(s.contextsRoot(), id, contextCleanFile)
	_, err := os.Stat(path)

	return err == nil
}

func (s *Server) markWorkspaceCleaned(id string) error {
	return writeJSONAtomically(filepath.Join(s.contextsRoot(), id, contextCleanFile), true)
}

func (s *Server) clearWorkspaceCleaned(id string) error {
	path := filepath.Join(s.contextsRoot(), id, contextCleanFile)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return nil
}

func cleanWorkspace(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("delete workspace: %w", err)
	}

	if err := os.MkdirAll(path, defaultDirMode); err != nil {
		return fmt.Errorf("recreate workspace: %w", err)
	}

	return nil
}

func resetContextDataAfterRootChange(metaDir, oldDataDir, newDataDir string) error {
	if samePath(oldDataDir, newDataDir) {
		return nil
	}

	// Manifest and runtime data share one correctness boundary: changing roots
	// means the next sync must rehydrate the target from scratch.
	if err := removeContextRuntimeData(metaDir, oldDataDir); err != nil {
		return fmt.Errorf("delete replaced context data: %w", err)
	}
	if err := removeContextRuntimeData(metaDir, newDataDir); err != nil {
		return fmt.Errorf("delete replacement context data: %w", err)
	}
	for _, name := range []string{contextManifestFile, contextCleanFile} {
		if err := os.Remove(filepath.Join(metaDir, name)); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("delete replaced context metadata %s: %w", name, err)
		}
	}

	return nil
}

func removeContextRuntimeData(metaDir, dataDir string) error {
	if samePath(dataDir, metaDir) {
		for _, name := range []string{contextWorkspaceDir, "volumes", runtimeDirName} {
			if err := os.RemoveAll(filepath.Join(dataDir, name)); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}

		return nil
	}

	return os.RemoveAll(dataDir)
}

func samePath(a, b string) bool {
	if hostIsWindows() {
		return strings.EqualFold(filepath.Clean(a), filepath.Clean(b))
	}

	return filepath.Clean(a) == filepath.Clean(b)
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
	name = strings.TrimSpace(name)
	if name != "" {
		return name
	}

	rootHint = strings.TrimSpace(rootHint)
	if rootHint != "" {
		return rootHint
	}

	return id
}

func validContextID(id string) bool {
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
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

		dataRoot := contextDataRoot(meta, s.opts.StateDir)
		dataDir := contextDataDir(dataRoot, id)
		out = append(out, protocol.ContextSummary{
			ID:        meta.ID,
			Name:      meta.Name,
			Path:      dataDir,
			Workspace: filepath.Join(dataDir, contextWorkspaceDir),
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
	ctx context.Context,
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

	s.logger.Printf(
		"context delete targets selected: requested_ids=%s all=%t older_than=%q available=%d targets=%s not_found=%s",
		formatStrings(req.IDs),
		req.All,
		req.OlderThan,
		len(contexts),
		formatContextMapIDs(targets),
		formatStrings(notFound),
	)

	deleted, err := s.removeContexts(targets)
	if err != nil {
		return protocol.DeleteContextsResponse{}, err
	}

	if len(deleted) > 0 {
		if err := s.pruneCachesAfterContextDelete(ctx); err != nil {
			return protocol.DeleteContextsResponse{}, err
		}
	}

	sort.Slice(deleted, func(i, j int) bool { return deleted[i].ID < deleted[j].ID })
	sort.Strings(notFound)

	return protocol.DeleteContextsResponse{
		Deleted:  deleted,
		NotFound: notFound,
	}, nil
}

func (s *Server) pruneCachesAfterContextDelete(ctx context.Context) error {
	if err := s.pruneLoggedCache("OCI", s.pruneUnreferencedOCICache); err != nil {
		return err
	}

	if err := s.pruneLoggedCache("blob", s.pruneUnreferencedBlobs); err != nil {
		return err
	}

	pruned, bytes, err := s.pruneWSLStagedRootFS(ctx)
	if err != nil {
		return err
	}

	s.logContextDeletePrune("WSL rootfs", pruned, bytes)

	return nil
}

func (s *Server) pruneWSLStagedRootFS(ctx context.Context) ([]protocol.ContextArtifact, int64, error) {
	contexts, err := s.listContexts()
	if err != nil {
		return nil, 0, err
	}

	dataDirs := make([]string, 0, len(contexts))
	for _, context := range contexts {
		dataDirs = append(dataDirs, context.Path)
	}

	return pruneWSLStagedRootFS(ctx, dataDirs)
}

func (s *Server) pruneLoggedCache(
	name string,
	prune func() ([]protocol.ContextArtifact, int64, error),
) error {
	pruned, bytes, err := prune()
	if err != nil {
		return err
	}

	s.logContextDeletePrune(name, pruned, bytes)

	return nil
}

func (s *Server) logContextDeletePrune(
	name string,
	pruned []protocol.ContextArtifact,
	bytes int64,
) {
	if len(pruned) == 0 {
		return
	}

	s.logger.Printf(
		"context delete pruned %s cache: deleted=%d bytes=%d",
		name,
		len(pruned),
		bytes,
	)
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
		s.logger.Printf(
			"deleting context: id=%s name=%q path=%s workspace=%s active=%t",
			id,
			context.Name,
			context.Path,
			context.Workspace,
			context.Active,
		)

		if err := s.removeContext(id); err != nil {
			return nil, err
		}

		s.logger.Printf("context deleted: id=%s path=%s", id, context.Path)

		deleted = append(deleted, context)
	}

	return deleted, nil
}

func formatStrings(values []string) string {
	if len(values) == 0 {
		return "-"
	}

	ordered := append([]string(nil), values...)
	sort.Strings(ordered)

	return strings.Join(ordered, ",")
}

func formatContextSummaryIDs(contexts []protocol.ContextSummary) string {
	if len(contexts) == 0 {
		return "-"
	}

	ids := make([]string, 0, len(contexts))
	for _, context := range contexts {
		ids = append(ids, context.ID)
	}

	return formatStrings(ids)
}

func formatContextMapIDs(contexts map[string]protocol.ContextSummary) string {
	if len(contexts) == 0 {
		return "-"
	}

	ids := make([]string, 0, len(contexts))
	for id := range contexts {
		ids = append(ids, id)
	}

	return formatStrings(ids)
}

func (s *Server) removeContext(id string) error {
	release := s.acquireContext(id)
	defer release()

	dataPath, err := s.contextDataDir(id)
	if err != nil {
		return err
	}

	metaPath := s.contextMetaDir(id)
	if samePath(metaPath, dataPath) {
		if err := os.RemoveAll(metaPath); err != nil {
			return fmt.Errorf("delete context %s: %w", id, err)
		}

		return nil
	}

	if err := os.RemoveAll(dataPath); err != nil {
		return fmt.Errorf("delete context %s data: %w", id, err)
	}
	if err := os.RemoveAll(metaPath); err != nil {
		return fmt.Errorf("delete context %s metadata: %w", id, err)
	}

	return nil
}

func (s *Server) syncContextFromClient(
	ctx context.Context,
	conn *protocol.Conn,
	contextID string,
	session string,
	workspace string,
	store *syncfs.BlobStore,
	current []syncfs.Entry,
	target []syncfs.Entry,
	runLogs *hostLogSubscription,
) error {
	changed, deleted := syncfs.Diff(current, target, syncfs.DiffOptions{})

	missing := store.MissingHashes(changed)

	transfer, err := s.prepareBlobReceiveSession(contextID, session, store, changed, missing)
	if err != nil {
		return err
	}

	if transfer != nil {
		defer s.unregisterBlobTransferSession(transfer.token)
	}

	s.logRun(
		runLogs,
		"sync from client started: context=%s session=%s changed=%d deleted=%d missing_blobs=%d",
		contextID,
		session,
		len(changed),
		len(deleted),
		len(missing),
	)

	runLogs.Flush()

	if err := conn.WriteJSON(
		protocol.MsgNeedBlobs,
		needBlobsMessage(missing, transfer),
	); err != nil {
		return err
	}

	if err := s.waitForClientBlobTransfer(ctx, conn, contextID, session, len(missing), transfer); err != nil {
		return err
	}

	fileTotal, fileBytes := syncFileTotals(changed)

	s.logRun(
		runLogs,
		"applying client sync to workspace: context=%s session=%s deleted=%d changed=%d files=%d file_bytes=%d",
		contextID,
		session,
		len(deleted),
		len(changed),
		fileTotal,
		fileBytes,
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

	if err := s.applyClientFiles(
		ctx,
		contextID,
		session,
		workspace,
		store,
		changed,
		fileTotal,
		fileBytes,
		runLogs,
	); err != nil {
		return err
	}

	s.logRun(runLogs, "sync from client complete: context=%s session=%s", contextID, session)

	return nil
}

func (s *Server) applyClientFiles(
	parent context.Context,
	contextID,
	session,
	workspace string,
	store *syncfs.BlobStore,
	changed []syncfs.Entry,
	fileTotal int,
	fileBytes int64,
	runLogs *hostLogSubscription,
) error {
	if fileTotal == 0 {
		return nil
	}

	workers := materializeWorkerCount(fileTotal)
	s.logRun(
		runLogs,
		"apply client files started: context=%s session=%s workers=%d",
		contextID,
		session,
		workers,
	)

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	jobs := make(chan syncfs.Entry)
	errCh := make(chan error, 1)

	var progressMu sync.Mutex
	materialized := 0
	var bytesApplied int64
	lastProgress := time.Time{}

	reportLocked := func() {
		now := time.Now()
		if !lastProgress.IsZero() && now.Sub(lastProgress) < progressEvery {
			return
		}

		lastProgress = now
		s.logRun(
			runLogs,
			"apply client file progress: context=%s session=%s files=%d/%d bytes=%d/%d",
			contextID,
			session,
			materialized,
			fileTotal,
			bytesApplied,
			fileBytes,
		)
	}

	addBytes := func(n int) {
		progressMu.Lock()
		bytesApplied += int64(n)
		reportLocked()
		progressMu.Unlock()
	}

	finishEntry := func(copied, size int64) {
		progressMu.Lock()
		if copied < size {
			bytesApplied += size - copied
		}
		materialized++
		reportLocked()
		progressMu.Unlock()
	}

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case entry, ok := <-jobs:
					if !ok {
						return
					}

					copied, err := s.materializeClientFile(workspace, store, entry, addBytes)
					if err != nil {
						select {
						case errCh <- fmt.Errorf(
							"materialize %s in context %s: %w",
							entry.Path,
							contextID,
							err,
						):
						default:
						}
						cancel()

						return
					}

					finishEntry(copied, entry.Size)
				}
			}
		}()
	}

sendJobs:
	for _, entry := range changed {
		if entry.Kind != syncfs.KindFile {
			continue
		}

		select {
		case jobs <- entry:
		case <-ctx.Done():
			break sendJobs
		}
	}

	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	progressMu.Lock()
	s.logRun(
		runLogs,
		"apply client file done: context=%s session=%s files=%d/%d bytes=%d/%d",
		contextID,
		session,
		materialized,
		fileTotal,
		bytesApplied,
		fileBytes,
	)
	progressMu.Unlock()

	return nil
}

func (s *Server) materializeClientFile(
	workspace string,
	store *syncfs.BlobStore,
	entry syncfs.Entry,
	onWrite func(int),
) (int64, error) {
	targetPath, err := pathutil.SecureJoin(workspace, filepath.FromSlash(entry.Path))
	if err != nil {
		return 0, err
	}

	mode := os.FileMode(entry.Mode)
	if mode == 0 {
		mode = defaultFileMode
	}

	var copied int64
	err = store.MaterializeWithProgress(
		entry.Hash,
		targetPath,
		mode,
		entry.ModTime,
		func(n int) {
			copied += int64(n)
			onWrite(n)
		},
	)
	if err != nil {
		return copied, err
	}

	return copied, nil
}

func materializeWorkerCount(files int) int {
	if files <= 1 {
		return 1
	}

	if files < materializeParallel {
		return files
	}

	return materializeParallel
}

func syncFileTotals(entries []syncfs.Entry) (int, int64) {
	files := 0
	var bytes int64

	for _, entry := range entries {
		if entry.Kind != syncfs.KindFile {
			continue
		}

		files++
		bytes += entry.Size
	}

	return files, bytes
}

func blobDescriptorsForHashes(
	entries []syncfs.Entry,
	hashes []string,
) ([]protocol.BlobDescriptor, error) {
	byHash := make(map[string]int64, len(entries))
	for _, entry := range entries {
		if entry.Kind != syncfs.KindFile || entry.Hash == "" {
			continue
		}
		if _, ok := byHash[entry.Hash]; !ok {
			byHash[entry.Hash] = entry.Size
		}
	}

	descriptors := make([]protocol.BlobDescriptor, 0, len(hashes))
	for _, hash := range hashes {
		size, ok := byHash[hash]
		if !ok {
			return nil, fmt.Errorf("unknown blob hash %s", hash)
		}
		descriptors = append(descriptors, protocol.BlobDescriptor{Hash: hash, Size: size})
	}

	return descriptors, nil
}

func (s *Server) prepareBlobReceiveSession(
	contextID string,
	session string,
	store *syncfs.BlobStore,
	changed []syncfs.Entry,
	missing []string,
) (*blobTransferSession, error) {
	if len(missing) == 0 {
		return nil, nil
	}

	token, err := protocol.RandomNonce()
	if err != nil {
		return nil, err
	}

	descriptors, err := blobDescriptorsForHashes(changed, missing)
	if err != nil {
		return nil, err
	}

	chunkSize := protocol.DefaultBlobChunkSize
	chunks := len(protocol.PlanBlobChunks(descriptors, chunkSize))
	parallel := transferParallelism(chunks)
	transfer, err := newBlobReceiveSession(
		contextID,
		session,
		token,
		store,
		descriptors,
		chunkSize,
		parallel,
	)
	if err != nil {
		return nil, err
	}

	s.registerBlobTransferSession(transfer)

	return transfer, nil
}

func needBlobsMessage(missing []string, transfer *blobTransferSession) protocol.NeedBlobs {
	msg := protocol.NeedBlobs{Hashes: missing}
	if transfer != nil {
		msg.TransferToken = transfer.token
		msg.Parallel = transfer.parallel
		msg.ChunkSize = transfer.chunkSize
	}

	return msg
}
