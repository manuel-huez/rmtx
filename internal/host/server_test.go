//nolint:cyclop,goconst,wsl_v5 // Server scenarios keep protocol fixtures and lifecycle checks together.
package host

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/config"
	"github.com/manuel-huez/rmtx/internal/oci"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/syncfs"
)

func TestIsDisconnectErrorRecognizesTypedNetworkCloseErrors(t *testing.T) {
	for _, err := range []error{
		syscall.ECONNRESET,
		syscall.ECONNABORTED,
		syscall.EPIPE,
		syscall.Errno(10054),
	} {
		if !protocol.IsDisconnectError(err) {
			t.Fatalf("expected disconnect error for %v", err)
		}
	}

	if protocol.IsDisconnectError(errors.New("apply non-file entries failed")) {
		t.Fatal("non-disconnect error should not match")
	}
}

func TestIsDisconnectErrorDistinguishesConnDeadlineFromContextDeadline(t *testing.T) {
	if protocol.IsDisconnectError(context.DeadlineExceeded) {
		t.Fatal("context deadline should not be treated as a disconnect")
	}

	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	if err := serverConn.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}

	_, err := serverConn.Read([]byte{0})
	if err == nil {
		t.Fatal("expected read deadline error")
	}

	if !protocol.IsDisconnectError(err) {
		t.Fatalf("read deadline should be treated as a disconnect: %v", err)
	}
}

func TestRMTXRunEnvIdentifiesHostResources(t *testing.T) {
	env := envMap(rmtxRunEnv(context.Background(), "/workspace", "ctx-1"))

	if env["RMTX"] != "1" {
		t.Fatalf("RMTX=%q want 1", env["RMTX"])
	}
	if env["RMTX_RUNNER"] != "host" {
		t.Fatalf("RMTX_RUNNER=%q want host", env["RMTX_RUNNER"])
	}
	if env["RMTX_WORKSPACE"] != "/workspace" {
		t.Fatalf("RMTX_WORKSPACE=%q want /workspace", env["RMTX_WORKSPACE"])
	}
	if env["RMTX_CONTEXT_ID"] != "ctx-1" {
		t.Fatalf("RMTX_CONTEXT_ID=%q want ctx-1", env["RMTX_CONTEXT_ID"])
	}
	cpuCount, err := strconv.Atoi(env["RMTX_CPU_COUNT"])
	if err != nil {
		t.Fatalf("RMTX_CPU_COUNT should be numeric: %v", err)
	}
	if cpuCount < 1 {
		t.Fatalf("RMTX_CPU_COUNT=%d want >= 1", cpuCount)
	}
	if _, err := strconv.ParseUint(env["RMTX_MEMORY_AVAILABLE_BYTES"], 10, 64); err != nil {
		t.Fatalf("RMTX_MEMORY_AVAILABLE_BYTES should be numeric: %v", err)
	}
}

func TestRMTXRunEnvOverridesExistingEntries(t *testing.T) {
	env := mergeEnvEntries(
		[]string{
			"RMTX=old",
			"RMTX_RUNNER=old",
			"RMTX_WORKSPACE=/old",
			"RMTX_CONTEXT_ID=old",
			"RMTX_CPU_COUNT=old",
			"RMTX_MEMORY_AVAILABLE_BYTES=old",
			"OTHER=value",
		},
		rmtxRunEnv(context.Background(), "/workspace", "ctx-1"),
	)

	seen := map[string]int{}
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		seen[key]++
	}
	for _, key := range []string{
		"RMTX",
		"RMTX_RUNNER",
		"RMTX_WORKSPACE",
		"RMTX_CONTEXT_ID",
		"RMTX_CPU_COUNT",
		"RMTX_MEMORY_AVAILABLE_BYTES",
	} {
		if seen[key] != 1 {
			t.Fatalf("%s count=%d want 1 in %#v", key, seen[key], env)
		}
	}

	values := envMap(env)
	if values["RMTX"] != "1" {
		t.Fatalf("RMTX=%q want 1", values["RMTX"])
	}
	if values["RMTX_RUNNER"] != "host" {
		t.Fatalf("RMTX_RUNNER=%q want host", values["RMTX_RUNNER"])
	}
	if values["RMTX_WORKSPACE"] != "/workspace" {
		t.Fatalf("RMTX_WORKSPACE=%q want /workspace", values["RMTX_WORKSPACE"])
	}
	if values["RMTX_CONTEXT_ID"] != "ctx-1" {
		t.Fatalf("RMTX_CONTEXT_ID=%q want ctx-1", values["RMTX_CONTEXT_ID"])
	}
	if values["RMTX_CPU_COUNT"] == "old" {
		t.Fatalf("RMTX_CPU_COUNT was not overridden: %#v", env)
	}
	if values["RMTX_MEMORY_AVAILABLE_BYTES"] == "old" {
		t.Fatalf("RMTX_MEMORY_AVAILABLE_BYTES was not overridden: %#v", env)
	}
}

func envMap(entries []string) map[string]string {
	out := make(map[string]string, len(entries))
	for _, entry := range entries {
		key, value, _ := strings.Cut(entry, "=")
		out[key] = value
	}

	return out
}

func TestWaitForClientSyncCompleteKeepsFinishedTransferOpen(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	s := &Server{logger: log.New(io.Discard, "", 0)}
	transfer := newBlobSendSession(
		"ctx",
		"session",
		"token",
		map[string]downloadBlobItem{},
		1,
		syncfs.DefaultBlobChunkSize,
		1,
	)
	transfer.completeChunk(syncfs.BlobChunkInfo{Hash: "hash"})

	done := make(chan error, 1)
	go func() {
		done <- s.waitForClientSyncCompleteAndBlobTransfer(
			context.Background(),
			protocol.NewConn(serverConn),
			transfer,
		)
	}()

	select {
	case err := <-done:
		t.Fatalf("wait returned before client sync_complete: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := protocol.NewConn(clientConn).WriteJSON(protocol.MsgSyncComplete, nil); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not finish after sync_complete")
	}
}

func TestHandleConnIgnoresDisconnectBeforeRequestHeader(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	var logs bytes.Buffer
	s := &Server{logger: log.New(&logs, "", 0)}

	if err := clientConn.Close(); err != nil {
		t.Fatal(err)
	}

	s.handleConn(context.Background(), serverConn)

	if logs.Len() != 0 {
		t.Fatalf("expected no request failure log for disconnect, got %q", logs.String())
	}
}

func TestHandleConnSessionIgnoresTopLevelHeartbeat(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()

	var logs bytes.Buffer
	s := &Server{logger: log.New(&logs, "", 0)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- s.handleConnSession(ctx, cancel, protocol.NewConn(serverConn))
	}()

	client := protocol.NewConn(clientConn)
	if err := client.WriteJSON(protocol.MsgHeartbeat, nil); err != nil {
		t.Fatal(err)
	}
	if err := clientConn.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("session did not finish after client disconnect")
	}

	if logs.Len() != 0 {
		t.Fatalf("expected no request log for heartbeat, got %q", logs.String())
	}
}

func TestDiffWorkspaceChangesCanIgnoreModeOnlyChanges(t *testing.T) {
	before := []syncfs.Entry{{
		Path: "file.txt",
		Kind: syncfs.KindFile,
		Hash: "hash",
		Size: 4,
		Mode: 0o644,
	}}
	after := []syncfs.Entry{{
		Path: "file.txt",
		Kind: syncfs.KindFile,
		Hash: "hash",
		Size: 4,
		Mode: 0o666,
	}}

	changed, deleted := diffWorkspaceChanges(before, after, nil, true)
	if len(changed) != 0 || len(deleted) != 0 {
		t.Fatalf("mode-only change should be ignored: changed=%#v deleted=%#v", changed, deleted)
	}

	changed, deleted = diffWorkspaceChanges(before, after, nil, false)
	if len(changed) != 1 || len(deleted) != 0 {
		t.Fatalf("mode-only change should be reported: changed=%#v deleted=%#v", changed, deleted)
	}
}

func TestListContextArtifactsIncludesWorkspaceVolumesAndImageRefs(t *testing.T) {
	stateDir := t.TempDir()
	s := &Server{opts: Options{StateDir: stateDir}}
	contextID := "ctx-1"
	contextDir := filepath.Join(stateDir, contextDirName, contextID)
	workspace := filepath.Join(contextDir, contextWorkspaceDir)
	volume := filepath.Join(contextDir, "volumes", "npm-cache")

	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(volume, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(workspace, "file.txt"),
		[]byte("workspace"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(volume, "cache.txt"), []byte("cache"), 0o644); err != nil {
		t.Fatal(err)
	}

	image := oci.Image{
		Reference:      "docker.io/library/node:22",
		ManifestDigest: "sha256:manifest",
		ConfigDigest:   "sha256:config",
		Layers:         []oci.Descriptor{{Digest: "sha256:layer"}},
	}
	rootfs := filepath.Join(contextDir, runtimeDirName, runtimeRootFSDirName, "key")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		t.Fatal(err)
	}
	baseRootFS := sharedRootFSPath(stateDir, "key")
	if err := saveArtifactState(contextDir, image, "key", rootfs, baseRootFS); err != nil {
		t.Fatal(err)
	}

	artifacts, err := s.listContextArtifacts(contextID, contextDir, contextDir)
	if err != nil {
		t.Fatal(err)
	}

	kinds := map[string]bool{}
	for _, artifact := range artifacts {
		kinds[artifact.Kind] = true
	}

	for _, want := range []string{"workspace", "volume", "image", "prepared-runtime"} {
		if !kinds[want] {
			t.Fatalf("missing artifact kind %q in %#v", want, artifacts)
		}
	}
}

func TestDeleteContextArtifactInvalidatesContextSetupCache(t *testing.T) {
	contextDir := t.TempDir()
	volume := filepath.Join(contextDir, "volumes", "npm-cache")
	if err := os.MkdirAll(volume, 0o755); err != nil {
		t.Fatal(err)
	}

	statePath := filepath.Join(contextDir, runtimeDirName, contextSetupFile)
	if err := saveContextSetupState(statePath, contextSetupState{Key: "cached"}); err != nil {
		t.Fatal(err)
	}

	s := &Server{}
	if _, err := s.deleteContextArtifact(contextDir, protocol.ContextArtifactsRequest{
		Delete: true,
		Volume: "npm-cache",
	}); err != nil {
		t.Fatal(err)
	}

	assertPathMissing(t, volume)
	assertPathMissing(t, statePath)
}

func TestCleanRunWorkspaceInvalidatesContextSetupCache(t *testing.T) {
	stateDir := t.TempDir()
	contextID := "ctx"
	contextDir := filepath.Join(stateDir, contextDirName, contextID)
	workspace := filepath.Join(contextDir, contextWorkspaceDir)
	workspaceFile := filepath.Join(workspace, "node_modules", "tool")
	statePath := filepath.Join(contextDir, runtimeDirName, contextSetupFile)

	if err := os.MkdirAll(filepath.Dir(workspaceFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(workspaceFile, []byte("cached setup output"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := saveContextSetupState(statePath, contextSetupState{Key: "cached"}); err != nil {
		t.Fatal(err)
	}

	s := &Server{
		opts:   Options{StateDir: stateDir},
		logger: log.New(io.Discard, "", 0),
	}
	if err := s.cleanRunWorkspace(contextID, workspace, contextDir, nil); err != nil {
		t.Fatal(err)
	}

	assertPathMissing(t, workspaceFile)
	assertPathMissing(t, statePath)
	assertPathExists(t, workspace)
	assertPathExists(t, filepath.Join(contextDir, contextCleanFile))
}

func TestSyncClientManifestRecoversInterruptedWorkspaceCleanup(t *testing.T) {
	stateDir := t.TempDir()
	contextID := "ctx"
	contextDir := filepath.Join(stateDir, contextDirName, contextID)
	workspace := filepath.Join(contextDir, contextWorkspaceDir)
	stalePath := filepath.Join(workspace, "cache", "stale")
	if err := os.MkdirAll(filepath.Dir(stalePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stalePath, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := New(t.Context(), Options{
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	storage := defaultTestStorage(t, s)

	hash := testBlobHash("live")
	if err := storage.blobStore.Store(hash, 4, bytes.NewReader([]byte("live"))); err != nil {
		t.Fatal(err)
	}
	manifest := []syncfs.Entry{{
		Path: "live.txt",
		Kind: syncfs.KindFile,
		Hash: hash,
		Size: 4,
		Mode: 0o644,
	}}
	if err := s.saveTrackedManifest(contextID, manifest); err != nil {
		t.Fatal(err)
	}
	if err := s.markWorkspaceCleaned(contextID); err != nil {
		t.Fatal(err)
	}

	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.syncClientManifestAndSave(
			context.Background(),
			protocol.NewConn(serverConn),
			protocol.RunRequest{
				ContextID: contextID,
				Session:   "session",
				Manifest:  manifest,
			},
			contextHandle{
				metaDir:    contextDir,
				dataDir:    contextDir,
				runtimeDir: contextDir,
				storage:    storage,
				workspace:  workspace,
			},
			&runWorkspace{workspace: workspace},
			nil,
		)
	}()

	client := protocol.NewConn(clientConn)
	head, err := client.ReadHeader()
	if err != nil {
		t.Fatalf("read need blobs: %v", err)
	}
	if head.Type != protocol.MsgNeedBlobs {
		t.Fatalf("need blobs type=%q want %q", head.Type, protocol.MsgNeedBlobs)
	}
	if err := client.DiscardPayload(head); err != nil {
		t.Fatal(err)
	}
	if err := client.WriteJSON(protocol.MsgSyncComplete, nil); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sync did not finish")
	}

	assertPathMissing(t, stalePath)
	content, err := os.ReadFile(filepath.Join(workspace, "live.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "live" {
		t.Fatalf("live.txt=%q want live", string(content))
	}
	assertPathMissing(t, filepath.Join(contextDir, contextCleanFile))
}

func TestDeleteContextsPrunesUnreferencedOCICache(t *testing.T) {
	stateDir := t.TempDir()
	var logs bytes.Buffer
	s, err := New(t.Context(), Options{
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(&logs, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	store := ociStore(stateDir)
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	deletedImage := storeTestOCIImage(
		t,
		store,
		"docker.io/library/alpine:deleted",
		"deleted",
	)
	keptImage := storeTestOCIImage(t, store, "docker.io/library/alpine:kept", "kept")

	deletedContextDir := filepath.Join(stateDir, contextDirName, "delete-me")
	keptContextDir := filepath.Join(stateDir, contextDirName, "keep-me")
	deletedRootFS := sharedRootFSPath(stateDir, "deleted-key")
	keptRootFS := sharedRootFSPath(stateDir, "kept-key")
	for _, path := range []string{deletedRootFS, keptRootFS} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "file"), []byte(path), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveArtifactState(
		deletedContextDir,
		deletedImage,
		"deleted-key",
		filepath.Join(deletedContextDir, runtimeDirName, runtimeRootFSDirName, "deleted-key"),
		deletedRootFS,
	); err != nil {
		t.Fatal(err)
	}
	if err := saveArtifactState(
		keptContextDir,
		keptImage,
		"kept-key",
		filepath.Join(keptContextDir, runtimeDirName, runtimeRootFSDirName, "kept-key"),
		keptRootFS,
	); err != nil {
		t.Fatal(err)
	}

	result, err := s.deleteContexts(
		context.Background(),
		protocol.DeleteContextsRequest{IDs: []string{"delete-me"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Deleted) != 1 || result.Deleted[0].ID != "delete-me" {
		t.Fatalf("unexpected delete result: %#v", result.Deleted)
	}

	assertPathMissing(t, filepath.Join(stateDir, contextDirName, "delete-me"))
	assertPathMissing(t, store.ManifestPath(deletedImage.ManifestDigest))
	assertPathMissing(t, store.BlobPath(deletedImage.ConfigDigest))
	assertPathMissing(t, store.BlobPath(deletedImage.Layers[0].Digest))
	assertPathMissing(t, deletedRootFS)
	if _, err := store.LoadRef(mustOCIReference(t, deletedImage.Reference)); err == nil {
		t.Fatal("deleted image ref should be pruned")
	}

	assertPathExists(t, filepath.Join(stateDir, contextDirName, "keep-me"))
	assertPathExists(t, store.ManifestPath(keptImage.ManifestDigest))
	assertPathExists(t, store.BlobPath(keptImage.ConfigDigest))
	assertPathExists(t, store.BlobPath(keptImage.Layers[0].Digest))
	assertPathExists(t, keptRootFS)
	if _, err := store.LoadRef(mustOCIReference(t, keptImage.Reference)); err != nil {
		t.Fatalf("kept image ref should remain: %v", err)
	}
}

func TestPruneUnreferencedBlobsKeepsManifestHashes(t *testing.T) {
	stateDir := t.TempDir()
	s, err := New(t.Context(), Options{
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	storage := defaultTestStorage(t, s)

	liveHash := testBlobHash("live")
	staleHash := testBlobHash("stale")
	if err := storage.blobStore.Store(liveHash, 4, bytes.NewReader([]byte("live"))); err != nil {
		t.Fatal(err)
	}
	if err := storage.blobStore.Store(staleHash, 5, bytes.NewReader([]byte("stale"))); err != nil {
		t.Fatal(err)
	}
	if err := s.saveTrackedManifest("ctx", []syncfs.Entry{{
		Path: "live.txt",
		Kind: syncfs.KindFile,
		Hash: liveHash,
		Size: 4,
	}}); err != nil {
		t.Fatal(err)
	}

	deleted, bytesDeleted, err := s.pruneUnreferencedBlobs()
	if err != nil {
		t.Fatal(err)
	}

	if len(deleted) != 1 || deleted[0].Kind != "blob" || deleted[0].Ref != staleHash {
		t.Fatalf("unexpected deleted blobs: %#v", deleted)
	}
	if bytesDeleted != 5 {
		t.Fatalf("deleted bytes=%d want 5", bytesDeleted)
	}
	assertPathExists(t, mustHostBlobPath(t, storage.blobStore, liveHash))
	assertPathMissing(t, mustHostBlobPath(t, storage.blobStore, staleHash))
}

func TestEnsureContextSplitsWorkspaceAndRuntimeRoots(t *testing.T) {
	stateDir := t.TempDir()
	runtimeRoot := t.TempDir()
	s, err := New(t.Context(), Options{
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	contextID := "ctx"
	metaDir := s.contextMetaDir(contextID)
	oldWorkspace := filepath.Join(stateDir, contextDirName, contextID, contextWorkspaceDir)
	oldVolume := filepath.Join(stateDir, contextDirName, contextID, "volumes", "cache")
	oldRuntime := filepath.Join(stateDir, contextDirName, contextID, runtimeDirName)
	staleRuntimeWorkspace := filepath.Join(
		runtimeRoot,
		contextDirName,
		contextID,
		contextWorkspaceDir,
		"stale.txt",
	)
	for _, path := range []string{oldWorkspace, oldVolume, oldRuntime, filepath.Dir(staleRuntimeWorkspace)} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(staleRuntimeWorkspace, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := saveContextMetadata(metaDir, contextMetadata{
		ID:       contextID,
		Name:     "ctx",
		DataRoot: stateDir,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.saveTrackedManifest(contextID, []syncfs.Entry{{
		Path: "stale.txt",
		Kind: syncfs.KindFile,
		Hash: "aa-stale",
		Size: 5,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := s.markWorkspaceCleaned(contextID); err != nil {
		t.Fatal(err)
	}

	store := syncfs.NewBlobStore(filepath.Join(runtimeRoot, "blobs"))
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}
	handle, err := s.ensureContext(contextID, "ctx", "", runtimeStorage{
		dataRoot:    stateDir,
		runtimeRoot: runtimeRoot,
		blobStore:   store,
	})
	if err != nil {
		t.Fatal(err)
	}

	assertPathMissing(t, oldVolume)
	assertPathMissing(t, oldRuntime)
	assertPathMissing(t, staleRuntimeWorkspace)
	assertPathExists(t, handle.workspace)
	if handle.dataDir != filepath.Join(stateDir, contextDirName, contextID) {
		t.Fatalf("dataDir=%s want data root context dir", handle.dataDir)
	}
	if handle.runtimeDir != filepath.Join(runtimeRoot, contextDirName, contextID) {
		t.Fatalf("runtimeDir=%s want runtime root context dir", handle.runtimeDir)
	}
	if handle.meta.DataRoot != "" {
		t.Fatalf("DataRoot=%s want empty host StateDir marker", handle.meta.DataRoot)
	}
	if handle.meta.RuntimeRoot != runtimeRoot {
		t.Fatalf("RuntimeRoot=%s want %s", handle.meta.RuntimeRoot, runtimeRoot)
	}
	assertPathExists(t, filepath.Join(metaDir, contextMetaFile))
}

func TestEnsureContextMigratesLegacyRuntimeDataRoot(t *testing.T) {
	stateDir := t.TempDir()
	legacyRuntimeRoot := t.TempDir()
	s, err := New(t.Context(), Options{
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	contextID := "ctx"
	metaDir := s.contextMetaDir(contextID)
	legacyContextDir := filepath.Join(legacyRuntimeRoot, contextDirName, contextID)
	legacyWorkspaceFile := filepath.Join(legacyContextDir, contextWorkspaceDir, "stale.txt")
	legacyLease := filepath.Join(legacyContextDir, workspaceLeasesDir, "ws_old")
	legacyVolumeFile := filepath.Join(legacyContextDir, "volumes", "cache", "data")
	legacyRuntimeFile := filepath.Join(legacyContextDir, runtimeDirName, "state")
	legacySetupCache := filepath.Join(legacyContextDir, runtimeDirName, contextSetupFile)
	for _, path := range []string{
		filepath.Dir(legacyWorkspaceFile),
		legacyLease,
		filepath.Dir(legacyVolumeFile),
		filepath.Dir(legacyRuntimeFile),
	} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{legacyWorkspaceFile, legacyVolumeFile, legacyRuntimeFile} {
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveContextMetadata(metaDir, contextMetadata{
		ID:       contextID,
		Name:     "ctx",
		DataRoot: legacyRuntimeRoot,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.saveTrackedManifest(contextID, []syncfs.Entry{{
		Path: "stale.txt",
		Kind: syncfs.KindFile,
		Hash: "aa-stale",
		Size: 5,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := saveContextSetupState(
		legacySetupCache,
		contextSetupState{Key: "old-workspace"},
	); err != nil {
		t.Fatal(err)
	}

	store := syncfs.NewBlobStore(filepath.Join(stateDir, "blobs"))
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}
	handle, err := s.ensureContext(contextID, "ctx", "", runtimeStorage{
		dataRoot:    stateDir,
		runtimeRoot: legacyRuntimeRoot,
		blobStore:   store,
	})
	if err != nil {
		t.Fatal(err)
	}

	assertPathMissing(t, legacyWorkspaceFile)
	assertPathMissing(t, legacyLease)
	assertPathMissing(t, legacySetupCache)
	assertPathExists(t, legacyVolumeFile)
	assertPathExists(t, legacyRuntimeFile)
	assertPathMissing(t, filepath.Join(metaDir, contextManifestFile))
	assertPathExists(t, handle.workspace)
	if handle.dataDir != filepath.Join(stateDir, contextDirName, contextID) {
		t.Fatalf("dataDir=%s want host data root context dir", handle.dataDir)
	}
	if handle.runtimeDir != legacyContextDir {
		t.Fatalf("runtimeDir=%s want legacy runtime dir", handle.runtimeDir)
	}
	if handle.meta.DataRoot != "" {
		t.Fatalf("DataRoot=%s want empty host StateDir marker", handle.meta.DataRoot)
	}
	if handle.meta.RuntimeRoot != legacyRuntimeRoot {
		t.Fatalf("RuntimeRoot=%s want %s", handle.meta.RuntimeRoot, legacyRuntimeRoot)
	}
}

func TestPruneUnreferencedBlobsUsesContextDataRoot(t *testing.T) {
	stateDir := t.TempDir()
	runtimeRoot := t.TempDir()
	s, err := New(t.Context(), Options{
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	hash := testBlobHash("live")
	defaultStorage := defaultTestStorage(t, s)
	runtimeStore := syncfs.NewBlobStore(filepath.Join(runtimeRoot, "blobs"))
	if err := runtimeStore.Ensure(); err != nil {
		t.Fatal(err)
	}
	for _, store := range []*syncfs.BlobStore{defaultStorage.blobStore, runtimeStore} {
		if err := store.Store(hash, 4, bytes.NewReader([]byte("live"))); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveContextMetadata(s.contextMetaDir("ctx"), contextMetadata{
		ID:       "ctx",
		Name:     "ctx",
		DataRoot: runtimeRoot,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.saveTrackedManifest("ctx", []syncfs.Entry{{
		Path: "live.txt",
		Kind: syncfs.KindFile,
		Hash: hash,
		Size: 4,
	}}); err != nil {
		t.Fatal(err)
	}

	if _, _, err := s.pruneUnreferencedBlobs(); err != nil {
		t.Fatal(err)
	}

	assertPathMissing(t, mustHostBlobPath(t, defaultStorage.blobStore, hash))
	assertPathExists(t, mustHostBlobPath(t, runtimeStore, hash))
}

func TestPruneUnreferencedBlobsScansLegacyRuntimeRoot(t *testing.T) {
	stateDir := t.TempDir()
	legacyRuntimeRoot := t.TempDir()
	s, err := New(t.Context(), Options{
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := saveContextMetadata(s.contextMetaDir("ctx"), contextMetadata{
		ID:          "ctx",
		Name:        "ctx",
		RuntimeRoot: legacyRuntimeRoot,
	}); err != nil {
		t.Fatal(err)
	}
	staleHash := testBlobHash("stale")
	legacyStore := syncfs.NewBlobStore(filepath.Join(legacyRuntimeRoot, "blobs"))
	if err := legacyStore.Ensure(); err != nil {
		t.Fatal(err)
	}
	if err := legacyStore.Store(staleHash, 5, bytes.NewReader([]byte("stale"))); err != nil {
		t.Fatal(err)
	}

	deleted, bytesDeleted, err := s.pruneUnreferencedBlobs()
	if err != nil {
		t.Fatal(err)
	}

	if bytesDeleted != 5 {
		t.Fatalf("deleted bytes=%d want 5", bytesDeleted)
	}
	if len(deleted) != 1 || deleted[0].Kind != "blob" || deleted[0].Ref != staleHash {
		t.Fatalf("unexpected deleted blobs: %#v", deleted)
	}
	assertPathMissing(t, mustHostBlobPath(t, legacyStore, staleHash))
}

func TestDeleteContextsPrunesUnreferencedBlobs(t *testing.T) {
	stateDir := t.TempDir()
	s, err := New(t.Context(), Options{
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	storage := defaultTestStorage(t, s)

	deletedHash := testBlobHash("deleted")
	keptHash := testBlobHash("kept")
	if err := storage.blobStore.Store(
		deletedHash,
		7,
		bytes.NewReader([]byte("deleted")),
	); err != nil {
		t.Fatal(err)
	}
	if err := storage.blobStore.Store(keptHash, 4, bytes.NewReader([]byte("kept"))); err != nil {
		t.Fatal(err)
	}
	if err := s.saveTrackedManifest("delete-me", []syncfs.Entry{{
		Path: "deleted.txt",
		Kind: syncfs.KindFile,
		Hash: deletedHash,
		Size: 7,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := s.saveTrackedManifest("keep-me", []syncfs.Entry{{
		Path: "kept.txt",
		Kind: syncfs.KindFile,
		Hash: keptHash,
		Size: 4,
	}}); err != nil {
		t.Fatal(err)
	}

	result, err := s.deleteContexts(
		context.Background(),
		protocol.DeleteContextsRequest{IDs: []string{"delete-me"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Deleted) != 1 || result.Deleted[0].ID != "delete-me" {
		t.Fatalf("unexpected delete result: %#v", result.Deleted)
	}

	assertPathMissing(t, mustHostBlobPath(t, storage.blobStore, deletedHash))
	assertPathExists(t, mustHostBlobPath(t, storage.blobStore, keptHash))
}

func TestPruneUnreferencedBlobsKeepsHashUsedByAnotherContext(t *testing.T) {
	stateDir := t.TempDir()
	s, err := New(t.Context(), Options{
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	storage := defaultTestStorage(t, s)

	sharedHash := testBlobHash("shared")
	if err := storage.blobStore.Store(
		sharedHash,
		6,
		bytes.NewReader([]byte("shared")),
	); err != nil {
		t.Fatal(err)
	}
	for _, contextID := range []string{"delete-me", "keep-me"} {
		if err := s.saveTrackedManifest(contextID, []syncfs.Entry{{
			Path: "shared.txt",
			Kind: syncfs.KindFile,
			Hash: sharedHash,
			Size: 6,
		}}); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := s.deleteContexts(
		context.Background(),
		protocol.DeleteContextsRequest{IDs: []string{"delete-me"}},
	); err != nil {
		t.Fatal(err)
	}

	assertPathExists(t, mustHostBlobPath(t, storage.blobStore, sharedHash))
}

func TestCachePruneDeletesUnreferencedBlobs(t *testing.T) {
	stateDir := t.TempDir()
	s, err := New(t.Context(), Options{
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	storage := defaultTestStorage(t, s)

	staleHash := testBlobHash("stale")
	if err := storage.blobStore.Store(staleHash, 5, bytes.NewReader([]byte("stale"))); err != nil {
		t.Fatal(err)
	}

	deleted, bytesDeleted, err := s.pruneAllCaches(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if bytesDeleted != 5 {
		t.Fatalf("deleted bytes=%d want 5", bytesDeleted)
	}

	found := false
	for _, artifact := range deleted {
		if artifact.Kind == "blob" && artifact.Ref == staleHash {
			found = true
		}
	}
	if !found {
		t.Fatalf("cache prune did not report stale blob: %#v", deleted)
	}
	assertPathMissing(t, mustHostBlobPath(t, storage.blobStore, staleHash))
}

func TestCachePruneDeletesExpiredWorkspaceLeases(t *testing.T) {
	stateDir := t.TempDir()
	s, err := New(t.Context(), Options{
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	contextID := "ctx"
	contextDir := s.contextMetaDir(contextID)
	if err := saveContextMetadata(
		contextDir,
		contextMetadata{ID: contextID, Name: contextID},
	); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	leaseID := "ws_expired"
	if err := saveWorkspaceLease(contextDir, workspaceLeaseState{
		ID:        leaseID,
		ContextID: contextID,
		CreatedAt: now.Add(-2 * time.Hour),
		UpdatedAt: now.Add(-2 * time.Hour),
		ExpiresAt: now.Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	workspacePath := workspaceLeaseWorkspacePath(contextDir, leaseID)
	if err := os.MkdirAll(workspacePath, defaultDirMode); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(workspacePath, "marker"),
		[]byte("stale"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	deleted, bytesDeleted, err := s.pruneAllCaches(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	var found protocol.ContextArtifact
	for _, artifact := range deleted {
		if artifact.Kind == "workspace" && artifact.Name == leaseID {
			found = artifact
			break
		}
	}
	if found.Path == "" {
		t.Fatalf("cache prune did not report expired workspace lease: %#v", deleted)
	}
	if found.Path != workspaceLeaseDir(contextDir, leaseID) {
		t.Fatalf("workspace path=%q want %q", found.Path, workspaceLeaseDir(contextDir, leaseID))
	}
	if found.Size == 0 || bytesDeleted < found.Size {
		t.Fatalf("deleted bytes=%d artifact=%#v", bytesDeleted, found)
	}
	assertPathMissing(t, workspaceLeaseDir(contextDir, leaseID))
}

func TestCachePruneDeletesUnreferencedSharedRootFS(t *testing.T) {
	stateDir := t.TempDir()
	s := &Server{opts: Options{StateDir: stateDir}}
	contextID := "ctx"
	contextDir := filepath.Join(stateDir, contextDirName, contextID)
	if err := saveContextMetadata(
		s.contextMetaDir(contextID),
		contextMetadata{ID: contextID, Name: contextID},
	); err != nil {
		t.Fatal(err)
	}

	live := sharedRootFSPath(stateDir, "live-key")
	stale := sharedRootFSPath(stateDir, "stale-key")
	for _, path := range []string{live, stale} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "file"), []byte(path), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	image := oci.Image{
		Reference:      "docker.io/library/alpine:live",
		ManifestDigest: "sha256:live-manifest",
		ConfigDigest:   "sha256:live-config",
		Layers:         []oci.Descriptor{{Digest: "sha256:live-layer"}},
	}
	if err := saveArtifactState(
		contextDir,
		image,
		"live-key",
		contextRootFSPath(contextDir, "live-key"),
		live,
	); err != nil {
		t.Fatal(err)
	}

	deleted, _, err := s.pruneUnreferencedOCICache()
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, artifact := range deleted {
		if artifact.Kind == "cache-rootfs" && artifact.Name == "stale-key" {
			found = true
		}
	}
	if !found {
		t.Fatalf("cache prune did not report stale shared rootfs: %#v", deleted)
	}
	assertPathExists(t, live)
	assertPathMissing(t, stale)
}

func TestPruneStartupTempFilesKeepsVolumeAndRootFSTemps(t *testing.T) {
	stateDir := t.TempDir()
	blobTemp := filepath.Join(stateDir, "blobs", "aa", "aa-live.tmp")
	ociTemp := filepath.Join(stateDir, "cache", "oci", "blobs", "sha256", "aa", "aa.tmp-123")
	volumeTemp := filepath.Join(stateDir, contextDirName, "ctx", "volumes", "cache", "cache.tmp")
	rootfsTemp := filepath.Join(
		stateDir,
		contextDirName,
		"ctx",
		runtimeDirName,
		runtimeRootFSDirName,
		"rootfs",
		"file.tmp-keep",
	)

	for _, path := range []string{blobTemp, ociTemp, volumeTemp, rootfsTemp} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("tmp"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	removed, err := pruneStartupTempFiles(stateDir)
	if err != nil {
		t.Fatal(err)
	}

	if len(removed) != 2 {
		t.Fatalf("removed=%#v want two cache temps", removed)
	}
	assertPathMissing(t, blobTemp)
	assertPathMissing(t, ociTemp)
	assertPathExists(t, volumeTemp)
	assertPathExists(t, rootfsTemp)
}

func TestSaveArtifactStateReplacesStaleRuntimeRefs(t *testing.T) {
	contextDir := t.TempDir()
	first := oci.Image{
		Reference:      "docker.io/library/alpine:first",
		ManifestDigest: "sha256:first-manifest",
		ConfigDigest:   "sha256:first-config",
		Layers:         []oci.Descriptor{{Digest: "sha256:first-layer"}},
	}
	second := oci.Image{
		Reference:      "docker.io/library/alpine:second",
		ManifestDigest: "sha256:second-manifest",
		ConfigDigest:   "sha256:second-config",
		Layers:         []oci.Descriptor{{Digest: "sha256:second-layer"}},
	}

	if err := saveArtifactState(
		contextDir,
		first,
		"first",
		filepath.Join(contextDir, "first"),
		sharedRootFSPath(contextDir, "first"),
	); err != nil {
		t.Fatal(err)
	}
	if err := saveArtifactState(
		contextDir,
		second,
		"second",
		filepath.Join(contextDir, "second"),
		sharedRootFSPath(contextDir, "second"),
	); err != nil {
		t.Fatal(err)
	}

	state, err := loadArtifactState(filepath.Join(contextDir, runtimeDirName, artifactStateFile))
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Images) != 1 || state.Images[0].Digest != second.ManifestDigest {
		t.Fatalf("images were not replaced: %#v", state.Images)
	}
	if len(state.Prepared) != 1 || state.Prepared[0].Key != "second" {
		t.Fatalf("prepared runtimes were not replaced: %#v", state.Prepared)
	}
}

func TestPruneStalePreparedRuntimesRemovesUnreferencedRootFS(t *testing.T) {
	contextDir := t.TempDir()
	root := filepath.Join(contextDir, runtimeDirName, runtimeRootFSDirName)
	live := filepath.Join(root, "live")
	stale := filepath.Join(root, "stale")
	if err := os.MkdirAll(live, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatal(err)
	}

	image := oci.Image{
		Reference:      "docker.io/library/alpine:live",
		ManifestDigest: "sha256:live-manifest",
		ConfigDigest:   "sha256:live-config",
		Layers:         []oci.Descriptor{{Digest: "sha256:live-layer"}},
	}
	if err := saveArtifactState(
		contextDir,
		image,
		"live",
		live,
		sharedRootFSPath(contextDir, "live"),
	); err != nil {
		t.Fatal(err)
	}

	deleted, err := pruneStalePreparedRuntimes(contextDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 1 || deleted[0].Name != "stale" {
		t.Fatalf("unexpected deleted runtimes: %#v", deleted)
	}
	assertPathExists(t, live)
	assertPathMissing(t, stale)
}

func TestCompactArtifactStateKeepsLatestPreparedRuntime(t *testing.T) {
	contextDir := t.TempDir()
	path := filepath.Join(contextDir, runtimeDirName, artifactStateFile)
	state := artifactState{
		Images: []artifactImage{
			{Reference: "old", Digest: "sha256:old", Blobs: []string{"sha256:old-blob"}},
			{Reference: "new", Digest: "sha256:new", Blobs: []string{"sha256:new-blob"}},
		},
		Prepared: []artifactPrepared{
			{Key: "old", Path: filepath.Join(contextDir, "old"), ImageDigest: "sha256:old"},
			{Key: "new", Path: filepath.Join(contextDir, "new"), ImageDigest: "sha256:new"},
		},
	}
	if err := writeJSONAtomically(path, state, contextFileMode); err != nil {
		t.Fatal(err)
	}

	if err := compactArtifactState(contextDir); err != nil {
		t.Fatal(err)
	}

	got, err := loadArtifactState(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Prepared) != 1 || got.Prepared[0].Key != "new" {
		t.Fatalf("prepared state not compacted: %#v", got.Prepared)
	}
	if len(got.Images) != 1 || got.Images[0].Digest != "sha256:new" {
		t.Fatalf("image state not compacted: %#v", got.Images)
	}
}

func TestValidateRuntimeSpecRejectsInvalidVolumeTarget(t *testing.T) {
	err := validateRuntimeSpec(config.RuntimeConfig{
		Type:  "oci",
		Image: "node:22",
		Volumes: []config.RuntimeVolume{{
			Name:   "cache",
			Target: "relative",
		}},
	})
	if err == nil {
		t.Fatal("expected invalid target error")
	}
}

func TestOCIWorkspaceTargetsKeepWorkspaceRootForSubdir(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "project")
	workdir := filepath.Join(workspace, "src", "cmd")

	runtimeWorkspace, runtimeCommandWorkdir := ociWorkspaceTargets(
		workspace,
		workdir,
		"/workspace",
	)

	if runtimeWorkspace != "/workspace" {
		t.Fatalf("runtime workspace=%s want /workspace", runtimeWorkspace)
	}
	if runtimeCommandWorkdir != "/workspace/src/cmd" {
		t.Fatalf("runtime command workdir=%s want /workspace/src/cmd", runtimeCommandWorkdir)
	}
}

func TestRuntimeCacheKeyIncludesSetupRuntimeOptions(t *testing.T) {
	base := config.RuntimeConfig{
		Type:    "oci",
		Image:   "node:22",
		Network: "host",
		User:    "root",
		GPU:     noneValue,
		Setup: config.RuntimeSetup{
			ImageCommands: []string{"apt-get update"},
		},
	}

	networkNone := base
	networkNone.Network = noneValue

	withGPU := base
	withGPU.GPU = "nvidia"

	first := runtimeCacheKey("sha256:manifest", base)
	if first == runtimeCacheKey("sha256:manifest", networkNone) {
		t.Fatal("runtime cache key should change when setup network changes")
	}

	if first == runtimeCacheKey("sha256:manifest", withGPU) {
		t.Fatal("runtime cache key should change when setup GPU mode changes")
	}
}

func TestOCIBaseEnvPreservesImagePath(t *testing.T) {
	env := ociBaseEnv([]string{
		"PATH=/usr/local/go/bin:/usr/bin",
		"GOROOT=/usr/local/go",
	})

	if len(env) != 2 {
		t.Fatalf("env=%#v", env)
	}

	if env[0] != "PATH=/usr/local/go/bin:/usr/bin" {
		t.Fatalf("PATH not overridden by image env: %#v", env)
	}
}

func TestMergeNvidiaRuntimeEnvPrependsPathWithoutDroppingImagePath(t *testing.T) {
	env := mergeNvidiaRuntimeEnv(
		[]string{
			"PATH=/image/bin:/usr/bin",
			"FOO=bar",
		},
		nvidiaRuntimeSpec{
			Env:          []string{"LD_LIBRARY_PATH=/driver/lib"},
			PathPrefixes: []string{"/usr/lib/wsl/lib", "/image/bin"},
		},
	)

	want := []string{
		"PATH=/usr/lib/wsl/lib:/image/bin:/usr/bin",
		"FOO=bar",
		"LD_LIBRARY_PATH=/driver/lib",
	}
	if !slices.Equal(env, want) {
		t.Fatalf("env=%#v want %#v", env, want)
	}
}

func TestEnsureRootFSInstanceMarkerCreatesMarker(t *testing.T) {
	rootfs := t.TempDir()

	if err := ensureRootFSInstanceMarker(rootfs); err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(filepath.Join(rootfs, rootFSInstanceMarker))
	if err != nil {
		t.Fatal(err)
	}

	if len(content) == 0 {
		t.Fatal("expected rootfs instance marker content")
	}
}

func TestEnsureRootFSInstanceMarkerPreservesExistingMarker(t *testing.T) {
	rootfs := t.TempDir()
	path := filepath.Join(rootfs, rootFSInstanceMarker)
	if err := os.WriteFile(path, []byte("existing\n"), contextFileMode); err != nil {
		t.Fatal(err)
	}

	if err := ensureRootFSInstanceMarker(rootfs); err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if string(content) != "existing\n" {
		t.Fatalf("marker was overwritten: %q", content)
	}
}

func TestContextSetupKeyIncludesRuntimeIdentity(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(workspace, "package-lock.json"),
		[]byte("lock"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	runtimeSpec := config.RuntimeConfig{
		Type:    "oci",
		Image:   "node:22",
		WorkDir: "/workspace",
		Network: "host",
		User:    "root",
		GPU:     noneValue,
		Setup: config.RuntimeSetup{
			ContextCommands: []string{"npm ci"},
			ContextInputs:   []string{"package-lock.json"},
		},
	}

	first, err := contextSetupKey(workspace, ".", runtimeSpec, "runtime-a")
	if err != nil {
		t.Fatal(err)
	}

	second, err := contextSetupKey(workspace, ".", runtimeSpec, "runtime-b")
	if err != nil {
		t.Fatal(err)
	}

	if first == second {
		t.Fatal("context setup key should change when prepared runtime changes")
	}
}

func TestContextSetupKeyIncludesCommandWorkdir(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(workspace, "package-lock.json"),
		[]byte("lock"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	runtimeSpec := config.RuntimeConfig{
		Type:    "oci",
		Image:   "node:22",
		WorkDir: "/workspace",
		Network: "host",
		User:    "root",
		GPU:     noneValue,
		Setup: config.RuntimeSetup{
			ContextCommands: []string{"npm ci"},
			ContextInputs:   []string{"package-lock.json"},
		},
	}

	first, err := contextSetupKey(workspace, "pkg/a", runtimeSpec, "runtime")
	if err != nil {
		t.Fatal(err)
	}

	second, err := contextSetupKey(workspace, "pkg/b", runtimeSpec, "runtime")
	if err != nil {
		t.Fatal(err)
	}

	if first == second {
		t.Fatal("context setup key should change when command workdir changes")
	}
}

func storeTestOCIImage(t *testing.T, store *oci.Store, ref string, seed string) oci.Image {
	t.Helper()

	manifest := []byte("manifest:" + seed)
	config := fmt.Appendf(nil, `{"config":{"Env":[%q]}}`, "SEED="+seed)
	layer := []byte("layer:" + seed)
	image := oci.Image{
		Reference:      ref,
		ManifestDigest: oci.DigestBytes(manifest),
		ConfigDigest:   oci.DigestBytes(config),
		Layers: []oci.Descriptor{{
			Digest: oci.DigestBytes(layer),
			Size:   int64(len(layer)),
		}},
	}
	if err := store.StoreManifest(image.ManifestDigest, manifest); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBlob(
		image.ConfigDigest,
		int64(len(config)),
		bytes.NewReader(config),
	); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBlob(
		image.Layers[0].Digest,
		int64(len(layer)),
		bytes.NewReader(layer),
	); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreRef(mustOCIReference(t, ref), image); err != nil {
		t.Fatal(err)
	}

	return image
}

func mustOCIReference(t *testing.T, ref string) oci.Reference {
	t.Helper()

	parsed, err := oci.ParseReference(ref)
	if err != nil {
		t.Fatal(err)
	}

	return parsed
}

func defaultTestStorage(t *testing.T, s *Server) runtimeStorage {
	t.Helper()

	storage, err := s.defaultRuntimeStorage()
	if err != nil {
		t.Fatal(err)
	}

	return storage
}

func assertPathExists(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", path, err)
	}
}

func assertPathMissing(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected %s to be missing, got %v", path, err)
	}
}

func mustHostBlobPath(t *testing.T, store *syncfs.BlobStore, hash string) string {
	t.Helper()

	path, err := store.Path(hash)
	if err != nil {
		t.Fatal(err)
	}

	return path
}

func testBlobHash(content string) string {
	sum := sha256.Sum256([]byte(content))

	return hex.EncodeToString(sum[:])
}
