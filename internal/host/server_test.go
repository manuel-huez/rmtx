//nolint:wsl_v5
package host

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/manuel-huez/rmtx/internal/oci"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/syncfs"
)

func TestIsDisconnectErrorRecognizesTypedNetworkCloseErrors(t *testing.T) {
	for _, err := range []error{
		syscall.ECONNRESET,
		syscall.ECONNABORTED,
		syscall.EPIPE,
		windowsConnectionReset,
	} {
		if !isDisconnectError(err) {
			t.Fatalf("expected disconnect error for %v", err)
		}
	}

	if isDisconnectError(errors.New("apply non-file entries failed")) {
		t.Fatal("non-disconnect error should not match")
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
	if err := saveArtifactState(contextDir, image, "key", rootfs); err != nil {
		t.Fatal(err)
	}

	artifacts, err := s.listContextArtifacts(contextID, contextDir)
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

func TestDeleteContextsPrunesUnreferencedOCICache(t *testing.T) {
	stateDir := t.TempDir()
	var logs bytes.Buffer
	s, err := New(Options{
		StateDir:         stateDir,
		DisableDiscovery: true,
		Logger:           log.New(&logs, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	store := s.ociStore()
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
	if err := saveArtifactState(
		deletedContextDir,
		deletedImage,
		"deleted-key",
		filepath.Join(deletedContextDir, runtimeDirName, runtimeRootFSDirName, "deleted-key"),
	); err != nil {
		t.Fatal(err)
	}
	if err := saveArtifactState(
		keptContextDir,
		keptImage,
		"kept-key",
		filepath.Join(keptContextDir, runtimeDirName, runtimeRootFSDirName, "kept-key"),
	); err != nil {
		t.Fatal(err)
	}

	result, err := s.deleteContexts(protocol.DeleteContextsRequest{IDs: []string{"delete-me"}})
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
	if _, err := store.LoadRef(mustOCIReference(t, deletedImage.Reference)); err == nil {
		t.Fatal("deleted image ref should be pruned")
	}

	assertPathExists(t, filepath.Join(stateDir, contextDirName, "keep-me"))
	assertPathExists(t, store.ManifestPath(keptImage.ManifestDigest))
	assertPathExists(t, store.BlobPath(keptImage.ConfigDigest))
	assertPathExists(t, store.BlobPath(keptImage.Layers[0].Digest))
	if _, err := store.LoadRef(mustOCIReference(t, keptImage.Reference)); err != nil {
		t.Fatalf("kept image ref should remain: %v", err)
	}
}

func TestValidateRuntimeSpecRejectsInvalidVolumeTarget(t *testing.T) {
	err := validateRuntimeSpec(protocol.RuntimeSpec{
		Type:  "oci",
		Image: "node:22",
		Volumes: []protocol.RuntimeVolume{{
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
	base := protocol.RuntimeSpec{
		Type:    "oci",
		Image:   "node:22",
		Network: "host",
		User:    "root",
		GPU:     "none",
		Setup: protocol.RuntimeSetup{
			ImageCommands: []string{"apt-get update"},
		},
	}

	networkNone := base
	networkNone.Network = "none"

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

func TestContextSetupKeyIncludesRuntimeIdentity(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(workspace, "package-lock.json"),
		[]byte("lock"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	runtimeSpec := protocol.RuntimeSpec{
		Type:    "oci",
		Image:   "node:22",
		WorkDir: "/workspace",
		Network: "host",
		User:    "root",
		GPU:     "none",
		Setup: protocol.RuntimeSetup{
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

	runtimeSpec := protocol.RuntimeSpec{
		Type:    "oci",
		Image:   "node:22",
		WorkDir: "/workspace",
		Network: "host",
		User:    "root",
		GPU:     "none",
		Setup: protocol.RuntimeSetup{
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
	config := []byte("config:" + seed)
	layer := []byte("layer:" + seed)
	image := oci.Image{
		Reference:      ref,
		ManifestDigest: oci.DigestBytes(manifest),
		ConfigDigest:   oci.DigestBytes(config),
		Layers: []oci.Descriptor{{
			Digest: oci.DigestBytes(layer),
		}},
	}
	if err := store.StoreManifest(image.ManifestDigest, manifest); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBlob(image.ConfigDigest, bytes.NewReader(config)); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBlob(image.Layers[0].Digest, bytes.NewReader(layer)); err != nil {
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
