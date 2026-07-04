package host

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/syncfs"
)

const (
	testContextID = "ctx"
	testHash      = "hash"
	testHelloPath = "hello.txt"
)

func TestPruneExpiredWorkspaceLeases(t *testing.T) {
	contextDir := t.TempDir()
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)

	expired := workspaceLeaseState{
		ID:        "ws_expired",
		ContextID: testContextID,
		CreatedAt: now.Add(-2 * time.Hour),
		UpdatedAt: now.Add(-2 * time.Hour),
		ExpiresAt: now.Add(-time.Minute),
	}
	live := workspaceLeaseState{
		ID:        "ws_live",
		ContextID: testContextID,
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: now.Add(time.Hour),
	}

	if err := saveWorkspaceLease(contextDir, expired); err != nil {
		t.Fatal(err)
	}

	if err := saveWorkspaceLease(contextDir, live); err != nil {
		t.Fatal(err)
	}

	server := &Server{}

	removed, err := server.pruneExpiredWorkspaceLeasesInContext(contextDir, now)
	if err != nil {
		t.Fatal(err)
	}

	if len(removed) != 1 || removed[0] != expired.ID {
		t.Fatalf("removed=%#v want %s", removed, expired.ID)
	}

	_, err = os.Stat(workspaceLeaseDir(contextDir, expired.ID))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired lease still exists: %v", err)
	}

	_, err = os.Stat(
		filepath.Join(workspaceLeaseDir(contextDir, live.ID), workspaceLeaseMetaFile),
	)
	if err != nil {
		t.Fatalf("live lease missing: %v", err)
	}
}

func TestPruneExpiredWorkspaceLeasesSkipsActiveLease(t *testing.T) {
	contextDir := t.TempDir()
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)

	state := workspaceLeaseState{
		ID:        "ws_active",
		ContextID: testContextID,
		CreatedAt: now.Add(-2 * time.Hour),
		UpdatedAt: now.Add(-2 * time.Hour),
		ExpiresAt: now.Add(-time.Minute),
	}
	if err := saveWorkspaceLease(contextDir, state); err != nil {
		t.Fatal(err)
	}

	server := &Server{}
	release := server.acquireWorkspaceLease(testContextID, state.ID)

	defer release()

	removed, err := server.pruneExpiredWorkspaceLeasesInContext(contextDir, now)
	if err != nil {
		t.Fatal(err)
	}

	if len(removed) != 0 {
		t.Fatalf("active lease removed: %#v", removed)
	}

	_, err = os.Stat(
		filepath.Join(workspaceLeaseDir(contextDir, state.ID), workspaceLeaseMetaFile),
	)
	if err != nil {
		t.Fatalf("active expired lease missing: %v", err)
	}
}

func TestWorkspaceLeaseMetadataDoesNotEmbedManifest(t *testing.T) {
	contextDir := t.TempDir()
	state := workspaceLeaseState{
		ID:        "ws_manifest",
		ContextID: testContextID,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
		WorkspaceManifest: []syncfs.Entry{
			{Path: testHelloPath, Kind: syncfs.KindFile, Hash: testHash},
		},
	}

	if err := saveWorkspaceLease(contextDir, state); err != nil {
		t.Fatal(err)
	}

	metaContent, err := os.ReadFile(workspaceLeaseMetaPath(contextDir, state.ID))
	if err != nil {
		t.Fatal(err)
	}

	if string(metaContent) == "" || strings.Contains(string(metaContent), "workspace_manifest") {
		t.Fatalf("metadata should not embed manifest: %s", string(metaContent))
	}

	loaded, err := loadWorkspaceLease(contextDir, state.ID)
	if err != nil {
		t.Fatal(err)
	}

	if len(loaded.WorkspaceManifest) != 1 || loaded.WorkspaceManifest[0].Path != testHelloPath {
		t.Fatalf("manifest not loaded from split file: %#v", loaded.WorkspaceManifest)
	}
}

func TestPruneExpiredWorkspaceLeasesRemovesPartialDir(t *testing.T) {
	contextDir := t.TempDir()

	id := "ws_partial"
	if err := os.MkdirAll(workspaceLeaseDir(contextDir, id), defaultDirMode); err != nil {
		t.Fatal(err)
	}

	server := &Server{}

	removed, err := server.pruneExpiredWorkspaceLeasesInContext(contextDir, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}

	if len(removed) != 1 || removed[0] != id {
		t.Fatalf("removed=%#v want %s", removed, id)
	}

	if _, err := os.Stat(workspaceLeaseDir(contextDir, id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial lease still exists: %v", err)
	}
}

func TestFinalizeWorkspaceLeaseExtendsTTLFromFinish(t *testing.T) {
	contextDir := t.TempDir()
	start := time.Now().UTC()
	lease := &workspaceLeaseRun{
		keep: time.Hour,
		state: workspaceLeaseState{
			ID:        "ws_ttl",
			ContextID: testContextID,
			CreatedAt: start,
			UpdatedAt: start,
			ExpiresAt: start.Add(-time.Minute),
			Dirty:     true,
		},
	}

	server := &Server{}

	err := server.finalizeWorkspaceLease(
		contextDir,
		lease,
		protocol.RunRequest{Session: "session"},
		[]syncfs.Entry{{Path: testHelloPath, Kind: syncfs.KindFile, Hash: testHash}},
	)
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := loadWorkspaceLease(contextDir, lease.state.ID)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.ExpiresAt.Before(start.Add(time.Hour)) {
		t.Fatalf("expiry=%s want at least %s", loaded.ExpiresAt, start.Add(time.Hour))
	}

	if loaded.Dirty {
		t.Fatalf("lease should be clean: %#v", loaded)
	}
}
