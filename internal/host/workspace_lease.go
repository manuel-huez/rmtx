package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/manuel-huez/rmtx/internal/pathutil"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/syncfs"
)

const (
	workspaceLeasesDir       = "workspaces"
	workspaceLeaseMetaFile   = "workspace.json"
	workspaceLeaseManifest   = "workspace-manifest.json"
	workspaceLeaseWorkspace  = "workspace"
	workspaceLeaseIDPrefix   = "ws_"
	workspaceLeaseIDHexChars = 16
)

var errWorkspaceLeaseNotFound = errors.New("workspace lease not found")

type workspaceLeaseState struct {
	ID                string
	ContextID         string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	ExpiresAt         time.Time
	Dirty             bool
	LastSession       string
	WorkspaceManifest []syncfs.Entry
}

type workspaceLeaseMetadata struct {
	ID          string    `json:"id"`
	ContextID   string    `json:"context_id"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	Dirty       bool      `json:"dirty,omitempty"`
	LastSession string    `json:"last_session,omitempty"`

	// Older development builds embedded manifests in workspace.json.
	WorkspaceManifest []syncfs.Entry `json:"workspace_manifest,omitempty"`
}

type runWorkspace struct {
	workspace      string
	beforeManifest []syncfs.Entry
	lease          *workspaceLeaseRun
}

type workspaceLeaseRun struct {
	state  workspaceLeaseState
	reused bool
	keep   time.Duration
}

type workspaceLeasePrepare struct {
	state  workspaceLeaseState
	reused bool
}

func (s *Server) prepareRunWorkspace(
	handle contextHandle,
	request protocol.RunRequest,
	runLogs *hostLogSubscription,
) (runWorkspace, error) {
	keep, err := parseWorkspaceKeepDuration(request.KeepWorkspace)
	if err != nil {
		return runWorkspace{}, err
	}

	reuseID := strings.TrimSpace(request.ReuseWorkspace)
	if keep <= 0 && reuseID == "" {
		return runWorkspace{workspace: handle.workspace}, nil
	}

	prepared, err := s.prepareWorkspaceLease(handle, request, keep, reuseID)
	if err != nil {
		return runWorkspace{}, err
	}

	state := prepared.state

	s.logRun(
		runLogs,
		"workspace lease prepared: context=%s session=%s id=%s reused=%t expires=%s dirty=%t",
		request.ContextID,
		request.Session,
		state.ID,
		prepared.reused,
		state.ExpiresAt.Format(time.RFC3339),
		state.Dirty,
	)

	return runWorkspace{
		workspace: workspaceLeaseWorkspacePath(handle.dataDir, state.ID),
		lease: &workspaceLeaseRun{
			state:  state,
			reused: prepared.reused,
			keep:   keep,
		},
	}, nil
}

func parseWorkspaceKeepDuration(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}

	ttl, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse keep-workspace duration: %w", err)
	}

	if ttl <= 0 {
		return 0, errors.New("keep-workspace duration must be positive")
	}

	return ttl, nil
}

func (s *Server) prepareWorkspaceLease(
	handle contextHandle,
	request protocol.RunRequest,
	keep time.Duration,
	reuseID string,
) (workspaceLeasePrepare, error) {
	now := time.Now().UTC()
	if reuseID != "" {
		return s.prepareReusedWorkspaceLease(handle, request, keep, reuseID, now)
	}

	return s.prepareNewWorkspaceLease(handle, request, keep, now)
}

func (s *Server) prepareReusedWorkspaceLease(
	handle contextHandle,
	request protocol.RunRequest,
	keep time.Duration,
	reuseID string,
	now time.Time,
) (workspaceLeasePrepare, error) {
	id, err := normalizeWorkspaceLeaseID(reuseID)
	if err != nil {
		return workspaceLeasePrepare{}, err
	}

	state, err := loadWorkspaceLease(handle.dataDir, id)
	if err != nil {
		return workspaceLeasePrepare{}, err
	}

	if state.Dirty {
		return workspaceLeasePrepare{},
			fmt.Errorf("workspace lease %s is dirty; delete it or create a new lease", id)
	}

	if !state.ExpiresAt.IsZero() && !state.ExpiresAt.After(now) {
		_ = removeWorkspaceLease(handle.dataDir, id)

		return workspaceLeasePrepare{}, fmt.Errorf("workspace lease %s expired", id)
	}

	if _, err := os.Stat(workspaceLeaseWorkspacePath(handle.dataDir, id)); err != nil {
		state.Dirty = true
		state.UpdatedAt = now
		_ = saveWorkspaceLease(handle.dataDir, state)

		return workspaceLeasePrepare{},
			fmt.Errorf("workspace lease %s workspace missing: %w", id, err)
	}

	if keep > 0 {
		state.ExpiresAt = now.Add(keep)
	}

	state.UpdatedAt = now
	state.LastSession = request.Session
	state.Dirty = true

	return workspaceLeasePrepare{state: state, reused: true},
		saveWorkspaceLease(handle.dataDir, state)
}

func (s *Server) prepareNewWorkspaceLease(
	handle contextHandle,
	request protocol.RunRequest,
	keep time.Duration,
	now time.Time,
) (workspaceLeasePrepare, error) {
	if err := s.pruneExpiredWorkspaceLeases(handle.dataDir, now); err != nil {
		return workspaceLeasePrepare{}, err
	}

	id, err := newWorkspaceLeaseID(handle.dataDir)
	if err != nil {
		return workspaceLeasePrepare{}, err
	}

	state := workspaceLeaseState{
		ID:          id,
		ContextID:   request.ContextID,
		CreatedAt:   now,
		UpdatedAt:   now,
		ExpiresAt:   now.Add(keep),
		Dirty:       true,
		LastSession: request.Session,
	}
	if err := saveWorkspaceLease(handle.dataDir, state); err != nil {
		return workspaceLeasePrepare{}, err
	}

	if err := os.MkdirAll(
		workspaceLeaseWorkspacePath(handle.dataDir, id),
		defaultDirMode,
	); err != nil {
		_ = removeWorkspaceLease(handle.dataDir, id)

		return workspaceLeasePrepare{}, fmt.Errorf("create workspace lease: %w", err)
	}

	return workspaceLeasePrepare{state: state}, nil
}

func newWorkspaceLeaseID(contextDir string) (string, error) {
	for range 16 {
		nonce, err := protocol.RandomNonce()
		if err != nil {
			return "", err
		}

		id := workspaceLeaseIDPrefix + nonce[:workspaceLeaseIDHexChars]
		if _, err := os.Stat(workspaceLeaseDir(contextDir, id)); errors.Is(err, os.ErrNotExist) {
			return id, nil
		} else if err != nil {
			return "", err
		}
	}

	return "", errors.New("allocate workspace lease id")
}

func normalizeWorkspaceLeaseID(id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", errors.New("workspace lease id is required")
	}

	if id == "." || id == ".." || len(id) > 80 {
		return "", fmt.Errorf("invalid workspace lease id %q", id)
	}

	for _, r := range id {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' {
			continue
		}

		return "", fmt.Errorf("invalid workspace lease id %q", id)
	}

	return id, nil
}

func workspaceLeaseDir(contextDir, id string) string {
	return filepath.Join(contextDir, workspaceLeasesDir, id)
}

func workspaceLeaseWorkspacePath(contextDir, id string) string {
	return filepath.Join(workspaceLeaseDir(contextDir, id), workspaceLeaseWorkspace)
}

func workspaceLeaseMetaPath(contextDir, id string) string {
	return filepath.Join(workspaceLeaseDir(contextDir, id), workspaceLeaseMetaFile)
}

func workspaceLeaseManifestPath(contextDir, id string) string {
	return filepath.Join(workspaceLeaseDir(contextDir, id), workspaceLeaseManifest)
}

func loadWorkspaceLease(contextDir, id string) (workspaceLeaseState, error) {
	state, legacyManifest, err := loadWorkspaceLeaseMetadata(contextDir, id)
	if err != nil {
		return workspaceLeaseState{}, err
	}

	if len(legacyManifest) > 0 {
		state.WorkspaceManifest = legacyManifest
	}

	content, err := os.ReadFile(workspaceLeaseManifestPath(contextDir, state.ID))
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}

	if err != nil {
		return workspaceLeaseState{},
			fmt.Errorf("read workspace lease manifest %s: %w", state.ID, err)
	}

	if err := json.Unmarshal(content, &state.WorkspaceManifest); err != nil {
		return workspaceLeaseState{},
			fmt.Errorf("parse workspace lease manifest %s: %w", state.ID, err)
	}

	return state, nil
}

func loadWorkspaceLeaseMetadata(
	contextDir,
	id string,
) (workspaceLeaseState, []syncfs.Entry, error) {
	id, err := normalizeWorkspaceLeaseID(id)
	if err != nil {
		return workspaceLeaseState{}, nil, err
	}

	content, err := os.ReadFile(workspaceLeaseMetaPath(contextDir, id))
	if errors.Is(err, os.ErrNotExist) {
		return workspaceLeaseState{}, nil,
			fmt.Errorf("workspace lease %s: %w", id, errWorkspaceLeaseNotFound)
	}

	if err != nil {
		return workspaceLeaseState{}, nil, fmt.Errorf("read workspace lease %s: %w", id, err)
	}

	var meta workspaceLeaseMetadata
	if err := json.Unmarshal(content, &meta); err != nil {
		return workspaceLeaseState{}, nil, fmt.Errorf("parse workspace lease %s: %w", id, err)
	}

	if meta.ID == "" {
		meta.ID = id
	}

	return workspaceLeaseState{
		ID:          meta.ID,
		ContextID:   meta.ContextID,
		CreatedAt:   meta.CreatedAt,
		UpdatedAt:   meta.UpdatedAt,
		ExpiresAt:   meta.ExpiresAt,
		Dirty:       meta.Dirty,
		LastSession: meta.LastSession,
	}, meta.WorkspaceManifest, nil
}

func saveWorkspaceLease(contextDir string, state workspaceLeaseState) error {
	if _, err := normalizeWorkspaceLeaseID(state.ID); err != nil {
		return err
	}

	meta := workspaceLeaseMetadata{
		ID:          state.ID,
		ContextID:   state.ContextID,
		CreatedAt:   state.CreatedAt,
		UpdatedAt:   state.UpdatedAt,
		ExpiresAt:   state.ExpiresAt,
		Dirty:       state.Dirty,
		LastSession: state.LastSession,
	}
	if err := writeJSONAtomically(workspaceLeaseMetaPath(contextDir, state.ID), meta); err != nil {
		return err
	}

	return writeJSONAtomically(
		workspaceLeaseManifestPath(contextDir, state.ID),
		state.WorkspaceManifest,
	)
}

func removeWorkspaceLease(contextDir, id string) error {
	id, err := normalizeWorkspaceLeaseID(id)
	if err != nil {
		return err
	}

	return pathutil.RemoveAll(workspaceLeaseDir(contextDir, id))
}

func (s *Server) finalizeWorkspaceLease(
	contextDir string,
	lease *workspaceLeaseRun,
	request protocol.RunRequest,
	workspaceManifest []syncfs.Entry,
) error {
	if lease == nil {
		return nil
	}

	now := time.Now().UTC()
	state := lease.state

	state.UpdatedAt = now
	if lease.keep > 0 {
		state.ExpiresAt = now.Add(lease.keep)
	}

	state.Dirty = false
	state.LastSession = request.Session

	state.WorkspaceManifest = append([]syncfs.Entry(nil), workspaceManifest...)

	lease.state = state

	return saveWorkspaceLease(contextDir, state)
}

func (s *Server) markWorkspaceLeaseDirty(contextDir string, lease *workspaceLeaseRun) {
	if lease == nil {
		return
	}

	state := lease.state
	state.Dirty = true
	state.UpdatedAt = time.Now().UTC()
	lease.state = state

	if err := saveWorkspaceLease(contextDir, state); err != nil {
		s.logger.Printf("mark workspace lease dirty failed: id=%s error=%v", state.ID, err)
	}
}

func (s *Server) listWorkspaceLeases(
	contextDir string,
	contextID string,
) ([]protocol.WorkspaceLeaseSummary, error) {
	if err := s.pruneExpiredWorkspaceLeases(contextDir, time.Now().UTC()); err != nil {
		return nil, err
	}

	var out []protocol.WorkspaceLeaseSummary

	err := forEachWorkspaceLeaseDir(contextDir, func(id string) error {
		state, _, err := loadWorkspaceLeaseMetadata(contextDir, id)
		if err != nil {
			return err
		}

		out = append(out, s.workspaceLeaseSummary(contextDir, contextID, state))

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list workspace leases: %w", err)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out, nil
}

func (s *Server) workspaceLeaseSummary(
	contextDir string,
	contextID string,
	state workspaceLeaseState,
) protocol.WorkspaceLeaseSummary {
	return protocol.WorkspaceLeaseSummary{
		ID:        state.ID,
		ContextID: contextID,
		Path:      workspaceLeaseWorkspacePath(contextDir, state.ID),
		CreatedAt: state.CreatedAt,
		UpdatedAt: state.UpdatedAt,
		ExpiresAt: state.ExpiresAt,
		Dirty:     state.Dirty,
		Active:    s.workspaceLeaseIsActive(contextID, state.ID),
	}
}

func (s *Server) deleteWorkspaceLeases(
	contextID string,
	contextDir string,
	ids []string,
) (deleted []protocol.WorkspaceLeaseSummary, notFound []string, err error) {
	for _, rawID := range ids {
		id, err := normalizeWorkspaceLeaseID(rawID)
		if err != nil {
			return nil, nil, err
		}

		state, _, loadErr := loadWorkspaceLeaseMetadata(contextDir, id)
		if loadErr != nil {
			if errors.Is(loadErr, errWorkspaceLeaseNotFound) {
				_ = removeWorkspaceLease(contextDir, id)
				notFound = append(notFound, id)

				continue
			}

			return nil, nil, loadErr
		}

		summary := s.workspaceLeaseSummary(contextDir, contextID, state)
		if err := removeWorkspaceLease(contextDir, id); err != nil {
			return nil, nil, err
		}

		deleted = append(deleted, summary)
	}

	sort.Slice(deleted, func(i, j int) bool { return deleted[i].ID < deleted[j].ID })
	sort.Strings(notFound)

	return deleted, notFound, nil
}

func (s *Server) pruneExpiredWorkspaceLeases(contextDir string, now time.Time) error {
	_, err := s.pruneExpiredWorkspaceLeasesInContext(contextDir, now)

	return err
}

func (s *Server) pruneExpiredWorkspaceLeasesInAllContexts(now time.Time) ([]string, error) {
	contexts, err := s.listContexts()
	if err != nil {
		return nil, err
	}

	var removed []string

	for _, context := range contexts {
		release := s.acquireContext(context.ID)
		deleted, err := s.pruneExpiredWorkspaceLeasesInContext(context.Path, now)

		release()

		if err != nil {
			return removed, err
		}

		removed = append(removed, deleted...)
	}

	sort.Strings(removed)

	return removed, nil
}

func (s *Server) pruneExpiredWorkspaceLeasesInContext(
	contextDir string,
	now time.Time,
) ([]string, error) {
	var removed []string

	err := forEachWorkspaceLeaseDir(contextDir, func(id string) error {
		state, _, err := loadWorkspaceLeaseMetadata(contextDir, id)
		if err != nil {
			if errors.Is(err, errWorkspaceLeaseNotFound) {
				if removeErr := removeWorkspaceLease(contextDir, id); removeErr != nil {
					return removeErr
				}

				removed = append(removed, id)

				return nil
			}

			return err
		}

		if state.ExpiresAt.IsZero() || state.ExpiresAt.After(now) {
			return nil
		}

		if s.workspaceLeaseIsActive(state.ContextID, state.ID) {
			return nil
		}

		if err := removeWorkspaceLease(contextDir, state.ID); err != nil {
			return err
		}

		removed = append(removed, state.ID)

		return nil
	})
	if err != nil {
		return removed, fmt.Errorf("prune workspace leases: %w", err)
	}

	sort.Strings(removed)

	return removed, nil
}

func forEachWorkspaceLeaseDir(contextDir string, fn func(id string) error) error {
	root := filepath.Join(contextDir, workspaceLeasesDir)

	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("list workspace leases: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		if _, err := normalizeWorkspaceLeaseID(entry.Name()); err != nil {
			continue
		}

		if err := fn(entry.Name()); err != nil {
			return err
		}
	}

	return nil
}
