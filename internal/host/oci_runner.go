//nolint:wsl_v5
package host

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/manuel-huez/rmtx/internal/config"
	"github.com/manuel-huez/rmtx/internal/oci"
	"github.com/manuel-huez/rmtx/internal/pathutil"
	"github.com/manuel-huez/rmtx/internal/protocol"
)

const (
	runtimeDirName       = "runtime"
	runtimeRootFSDirName = "rootfs"
	runtimeSpecDirName   = "specs"
	runtimeSetupMarker   = ".rmtx-image-setup-ready"
	rootFSInstanceMarker = ".rmtx-rootfs-instance"
	contextSetupFile     = "context-setup.json"
	artifactStateFile    = "artifacts.json"
	defaultOCIPathEnv    = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	defaultOCINetwork    = "host"
	defaultOCIUser       = "root"
	defaultOCIWorkdir    = "/workspace"
	pullProgressFields   = 2
	// Rootfs format gates prepared-base reuse; bump only when rootfs contents
	// produced from the same image/setup/runtime options become incompatible.
	rootFSCacheFormatVersion = "overlay-v1"
)

type preparedRuntime struct {
	RootFS      string
	LowerRootFS string
	Image       oci.Image
	Key         string
}

type artifactState struct {
	Images    []artifactImage    `json:"images,omitempty"`
	Prepared  []artifactPrepared `json:"prepared,omitempty"`
	UpdatedAt time.Time          `json:"updated_at"`
}

type artifactImage struct {
	Reference string   `json:"reference"`
	Digest    string   `json:"digest"`
	Blobs     []string `json:"blobs,omitempty"`
}

type artifactPrepared struct {
	Key            string `json:"key"`
	Path           string `json:"path"`
	BasePath       string `json:"base_path,omitempty"`
	ImageDigest    string `json:"image_digest"`
	ImageReference string `json:"image_reference"`
}

type contextSetupState struct {
	Key string `json:"key"`
}

func isOCIRuntime(runtime config.RuntimeConfig) bool {
	return strings.EqualFold(strings.TrimSpace(runtime.Type), "oci")
}

func validateRuntimeSpec(runtime config.RuntimeConfig) error {
	return config.ValidateRuntime(runtime)
}

func (s *Server) prepareRuntimeBeforeSync(
	ctx context.Context,
	handle contextHandle,
	request protocol.RunRequest,
	runLogs *hostLogSubscription,
) (preparedRuntime, bool, error) {
	if !isOCIRuntime(request.Runtime) {
		return preparedRuntime{}, false, nil
	}

	if !hostSupportsOCIRuntime() {
		return preparedRuntime{}, false, fmt.Errorf(
			"OCI runtime is not supported on %s hosts yet",
			runtime.GOOS,
		)
	}

	prepared, err := s.prepareOCIRuntime(ctx, handle, request, runLogs)
	if err != nil {
		return preparedRuntime{}, false, err
	}

	return prepared, true, nil
}

func (s *Server) runOCIPipeCommand(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	runtimeDir string,
	workspace string,
	workdir string,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
	runLogs *hostLogSubscription,
) (int, error) {
	cancelRun := newRunCancelHandle(cancel)
	input := s.startPipeInputForwarding(conn, cancelRun.Cancel)

	if err := s.prepareOCIContextSetup(
		ctx,
		runtimeDir,
		workspace,
		request,
		preparedRuntime,
		runLogs,
	); err != nil {
		_ = stopPipeInputReader(conn, input)
		return 1, err
	}
	runLogs.Flush()

	cmd, cleanup, err := s.newOCICommand(
		ctx,
		runtimeDir,
		workspace,
		workdir,
		request,
		preparedRuntime,
		runLogs,
	)
	if err != nil {
		_ = stopPipeInputReader(conn, input)
		return 1, err
	}

	code, runErr := s.runPipeExecCommandWithInput(ctx, cancel, conn, cmd, input, cancelRun)

	return finishCommandWithCleanup(code, runErr, cleanup)
}

func (s *Server) prepareOCIRuntime(
	ctx context.Context,
	handle contextHandle,
	request protocol.RunRequest,
	runLogs *hostLogSubscription,
) (preparedRuntime, error) {
	s.logRun(
		runLogs,
		"waiting for OCI runtime preparation: context=%s image=%s",
		request.ContextID,
		request.Runtime.Image,
	)
	runLogs.Flush()

	s.ociMu.Lock()
	defer s.ociMu.Unlock()

	return s.prepareOCIRuntimeLocked(ctx, handle, request, runLogs)
}

//nolint:cyclop,nestif // Cache validation and creation stay under one OCI lock transaction.
func (s *Server) prepareOCIRuntimeLocked(
	ctx context.Context,
	handle contextHandle,
	request protocol.RunRequest,
	runLogs *hostLogSubscription,
) (preparedRuntime, error) {
	s.logRun(
		runLogs,
		"checking OCI runtime cache: context=%s image=%s",
		request.ContextID,
		request.Runtime.Image,
	)

	ref, err := oci.ParseReference(request.Runtime.Image)
	if err != nil {
		return preparedRuntime{}, err
	}

	runtimeRoot := runtimeRootForContextRuntimeDir(handle.runtimeDir)
	store := ociStore(runtimeRoot)
	if err := store.Ensure(); err != nil {
		return preparedRuntime{}, err
	}

	image, err := s.pullOCIImage(ctx, ref, request.Runtime, store, request.ContextID, runLogs)
	if err != nil {
		return preparedRuntime{}, err
	}

	key := runtimeCacheKey(image.ManifestDigest, request.Runtime)
	baseRootFS := sharedRootFSPath(runtimeRoot, key)
	rootfs := contextRootFSPath(handle.runtimeDir, key)

	setupMarker := filepath.Join(baseRootFS, runtimeSetupMarker)
	if _, err := os.Stat(setupMarker); errors.Is(err, os.ErrNotExist) {
		s.logRun(
			runLogs,
			"unpacking OCI image: context=%s rootfs=%s image=%s",
			request.ContextID,
			baseRootFS,
			request.Runtime.Image,
		)
		runLogs.Flush()

		stopProgress := startRunLogProgress(
			runLogs,
			progressEvery,
			"unpacking OCI image still running: context=%s image=%s rootfs=%s key=%s",
			request.ContextID,
			request.Runtime.Image,
			baseRootFS,
			key,
		)
		err := store.UnpackImageContext(
			ctx,
			baseRootFS,
			image,
			s.logOCIUnpackProgress(runLogs, request.ContextID, request.Runtime.Image),
		)
		stopProgress()
		if err != nil {
			return preparedRuntime{}, err
		}
		s.logRun(
			runLogs,
			"unpacking OCI image done: context=%s image=%s",
			request.ContextID,
			request.Runtime.Image,
		)

		if err := writeRootFSInstanceMarker(baseRootFS); err != nil {
			_ = pathutil.RemoveAll(baseRootFS)
			return preparedRuntime{}, err
		}

		if err := s.runImageSetupCommands(
			ctx,
			baseRootFS,
			request.Runtime,
			image.Env,
			runLogs,
		); err != nil {
			_ = pathutil.RemoveAll(baseRootFS)
			return preparedRuntime{}, err
		}

		content := []byte(time.Now().UTC().Format(time.RFC3339) + "\n")
		if err := pathutil.WriteFileAtomically(setupMarker, content, contextFileMode); err != nil {
			return preparedRuntime{}, err
		}
	} else {
		s.logRun(
			runLogs,
			"using shared OCI runtime setup marker: context=%s image=%s",
			request.ContextID,
			request.Runtime.Image,
		)

		if err := ensureRootFSInstanceMarker(baseRootFS); err != nil {
			return preparedRuntime{}, err
		}
	}

	if err := ensureOverlayRootFSBundle(rootfs, key, baseRootFS); err != nil {
		return preparedRuntime{}, err
	}

	if err := s.ensureOCIVolumes(handle.runtimeDir, request.Runtime.Volumes); err != nil {
		return preparedRuntime{}, err
	}

	if err := savePreparedRuntimeState(
		handle.runtimeDir,
		image,
		key,
		rootfs,
		baseRootFS,
	); err != nil {
		return preparedRuntime{}, err
	}

	return preparedRuntime{RootFS: rootfs, LowerRootFS: baseRootFS, Image: image, Key: key}, nil
}

func savePreparedRuntimeState(
	runtimeDir string,
	image oci.Image,
	key string,
	rootfs string,
	baseRootFS string,
) error {
	if err := saveArtifactState(runtimeDir, image, key, rootfs, baseRootFS); err != nil {
		return err
	}

	_, err := pruneStalePreparedRuntimes(runtimeDir)

	return err
}

func (s *Server) pullOCIImage(
	ctx context.Context,
	ref oci.Reference,
	runtimeSpec config.RuntimeConfig,
	store *oci.Store,
	contextID string,
	runLogs io.Writer,
) (oci.Image, error) {
	policy := strings.TrimSpace(runtimeSpec.PullPolicy)
	if policy == "" {
		policy = "if_missing"
	}

	s.logRun(
		runLogs,
		"runtime image pull start: context=%s image=%s pull_policy=%s",
		contextID,
		ref.Normalized(),
		policy,
	)

	if !strings.EqualFold(policy, "always") {
		image, err := store.LoadRef(ref)
		if err == nil && store.ImageComplete(image) {
			s.logRun(
				runLogs,
				"runtime image cache hit: context=%s image=%s digest=%s",
				contextID,
				ref.Normalized(),
				image.ManifestDigest,
			)
			return image, nil
		}

		if strings.EqualFold(policy, "never") {
			return oci.Image{}, fmt.Errorf("image %s not found in local cache", ref.Normalized())
		}
	}

	client := oci.NewClient(nil)
	image, err := client.Pull(ctx, ref, store, oci.PullOptions{
		PlatformOS:   "linux",
		Architecture: runtime.GOARCH,
		Progress: func(format string, args ...any) {
			fields := make([]any, 0, len(args)+pullProgressFields)
			fields = append(fields, contextID, ref.Normalized())
			fields = append(fields, args...)
			s.logRun(
				runLogs,
				"runtime image pull: context=%s image=%s: "+format,
				fields...,
			)
		},
	})
	if err != nil {
		return oci.Image{}, err
	}

	s.logRun(
		runLogs,
		"runtime image pull done: context=%s image=%s digest=%s",
		contextID,
		ref.Normalized(),
		image.ManifestDigest,
	)

	return image, nil
}

func ensureRootFSInstanceMarker(rootfs string) error {
	path := filepath.Join(rootfs, rootFSInstanceMarker)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat rootfs instance marker: %w", err)
	}

	return writeRootFSInstanceMarker(rootfs)
}

func writeRootFSInstanceMarker(rootfs string) error {
	content := fmt.Appendf(nil, "%d-%d\n", time.Now().UTC().UnixNano(), os.Getpid())
	if err := pathutil.WriteFileAtomically(
		filepath.Join(rootfs, rootFSInstanceMarker),
		content,
		contextFileMode,
	); err != nil {
		return fmt.Errorf("write rootfs instance marker: %w", err)
	}

	return nil
}

func (s *Server) runImageSetupCommands(
	ctx context.Context,
	rootfs string,
	runtimeSpec config.RuntimeConfig,
	imageEnv []string,
	runLogs *hostLogSubscription,
) error {
	gpuRuntime, err := nvidiaRuntime(runtimeSpec.GPU)
	if err != nil {
		return nvidiaUnavailableError(err)
	}

	env := mergeNvidiaRuntimeEnv(ociBaseEnv(imageEnv), gpuRuntime)

	for _, command := range runtimeSpec.Setup.ImageCommands {
		if strings.TrimSpace(command) == "" {
			continue
		}

		writeRunLogLine(runLogs, "=== runtime image setup ===")
		writeRunLogLine(runLogs, "setup command: %s", command)
		spec := ociChildSpec{
			RootFS:    rootfs,
			WorkDir:   "/",
			Command:   []string{"/bin/sh", "-c", command},
			Env:       env,
			Binds:     gpuRuntime.Binds,
			Network:   runtimeSpec.Network,
			GPU:       runtimeSpec.GPU,
			WSLDistro: runtimeSpec.WSLDistro,
		}

		cmd, cleanup, err := s.ociChildCommand(ctx, spec, rootfs, runLogs)
		if err != nil {
			return err
		}

		s.logRun(runLogs, "runtime image setup command start: command=%q", command)

		out, err := runCommandWithLiveOutput(
			s.hostOnlyLogger(),
			cmd,
			"runtime image setup command",
			runLogs,
			runLogs,
		)
		runLogs.Flush()

		cleanupErr := cleanup()
		if err != nil {
			return fmt.Errorf(
				"runtime image setup command %q failed: %w\n%s",
				command,
				err,
				truncateOutput(out),
			)
		}

		if cleanupErr != nil {
			return fmt.Errorf("clean runtime image setup spec: %w", cleanupErr)
		}
		s.logRun(runLogs, "runtime image setup command done: command=%q", command)
	}

	return nil
}

func (s *Server) logOCIUnpackProgress(
	runLogs io.Writer,
	contextID string,
	image string,
) oci.UnpackProgressFunc {
	layerStarted := make(map[int]time.Time)

	return func(progress oci.UnpackProgress) {
		digest := shortOCIDigest(progress.Digest)
		layer := fmt.Sprintf("%d/%d", progress.LayerIndex, progress.LayerCount)
		bytes := unpackProgressBytes(progress.LayerDoneBytes, progress.LayerBytes)
		total := unpackProgressBytes(progress.TotalDoneBytes, progress.TotalBytes)

		switch progress.Event {
		case oci.UnpackProgressLayerStart:
			layerStarted[progress.LayerIndex] = time.Now()
			s.logRun(
				runLogs,
				"unpacking OCI layer start: context=%s image=%s layer=%s digest=%s bytes=%s total=%s",
				contextID,
				image,
				layer,
				digest,
				bytes,
				total,
			)
		case oci.UnpackProgressLayerDone:
			elapsed := time.Since(layerStarted[progress.LayerIndex]).Round(time.Millisecond)
			s.logRun(
				runLogs,
				"unpacking OCI layer done: context=%s image=%s layer=%s digest=%s bytes=%s entries=%d elapsed=%s total=%s",
				contextID,
				image,
				layer,
				digest,
				bytes,
				progress.Entries,
				elapsed,
				total,
			)
		case oci.UnpackProgressLayerProgress:
			s.logRun(
				runLogs,
				"unpacking OCI layer progress: context=%s image=%s layer=%s digest=%s bytes=%s entries=%d total=%s",
				contextID,
				image,
				layer,
				digest,
				bytes,
				progress.Entries,
				total,
			)
		}
	}
}

func shortOCIDigest(digest string) string {
	parts := strings.SplitN(digest, ":", splitNEquals)
	if len(parts) != 2 || len(parts[1]) <= 12 {
		return digest
	}

	return parts[0] + ":" + parts[1][:12]
}

func unpackProgressBytes(done, total int64) string {
	if total <= 0 {
		return strconv.FormatInt(done, 10)
	}

	return fmt.Sprintf("%d/%d", done, total)
}

func (s *Server) prepareOCIContextSetup(
	ctx context.Context,
	runtimeDir string,
	workspace string,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
	runLogs *hostLogSubscription,
) error {
	if len(request.Runtime.Setup.ContextCommands) == 0 {
		return nil
	}

	workdir, err := secureJoin(workspace, request.WorkDir)
	if err != nil {
		return err
	}

	handle := contextHandle{
		runtimeDir: runtimeDir,
		workspace:  workspace,
	}
	prepared, err := s.ensurePreparedOCIRuntime(
		ctx,
		handle,
		request,
		preparedRuntime,
		runLogs,
	)
	if err != nil {
		return err
	}

	key, err := contextSetupKey(workspace, request.WorkDir, request.Runtime, prepared.Key)
	if err != nil {
		return err
	}

	statePath := filepath.Join(
		handle.runtimeDir,
		runtimeDirName,
		contextSetupFile,
	)
	if contextSetupCacheHit(statePath, key) {
		s.logRun(
			runLogs,
			"runtime context setup cache hit: context=%s",
			request.ContextID,
		)

		return nil
	}

	if err := s.runOCIContextSetupCommands(
		ctx,
		handle.runtimeDir,
		workspace,
		workdir,
		request,
		&prepared,
		runLogs,
	); err != nil {
		return err
	}

	return saveContextSetupCache(statePath, key)
}

func (s *Server) runOCIContextSetupCommands(
	ctx context.Context,
	runtimeDir string,
	workspace string,
	workdir string,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
	runLogs *hostLogSubscription,
) error {
	for _, command := range request.Runtime.Setup.ContextCommands {
		if strings.TrimSpace(command) == "" {
			continue
		}

		setupReq := request
		setupReq.Command = []string{"/bin/sh", "-c", command}
		setupReq.WorkDir = request.WorkDir
		s.logRun(
			runLogs,
			"runtime context setup command start: context=%s command=%q",
			request.ContextID,
			command,
		)
		writeRunLogLine(runLogs, "=== runtime context setup ===")
		writeRunLogLine(runLogs, "setup command: %s", command)
		writeRunLogLine(runLogs, "setup workdir: %s", request.WorkDir)

		cmd, cleanup, err := s.newOCICommand(
			ctx,
			runtimeDir,
			workspace,
			workdir,
			setupReq,
			preparedRuntime,
			runLogs,
		)
		if err != nil {
			return err
		}

		out, err := runCommandWithLiveOutput(
			s.hostOnlyLogger(),
			cmd,
			"runtime context setup command",
			runLogs,
			runLogs,
		)
		runLogs.Flush()

		cleanupErr := cleanup()
		if err != nil {
			return fmt.Errorf(
				"runtime context setup command %q failed: %w\n%s",
				command,
				err,
				truncateOutput(out),
			)
		}

		if cleanupErr != nil {
			return fmt.Errorf("clean runtime context setup spec: %w", cleanupErr)
		}
		s.logRun(
			runLogs,
			"runtime context setup command done: context=%s command=%q",
			request.ContextID,
			command,
		)
	}

	return nil
}

func contextSetupCacheHit(path string, key string) bool {
	if key == "" {
		return false
	}

	state, _ := loadContextSetupState(path)

	return state.Key == key
}

func saveContextSetupCache(path string, key string) error {
	if key == "" {
		return nil
	}

	return saveContextSetupState(path, contextSetupState{Key: key})
}

func (s *Server) newOCICommand(
	ctx context.Context,
	runtimeDir string,
	workspace string,
	workdir string,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
	runLogs *hostLogSubscription,
) (*exec.Cmd, commandCleanup, error) {
	handle := contextHandle{
		runtimeDir: runtimeDir,
		workspace:  workspace,
	}

	prepared, err := s.ensurePreparedOCIRuntime(ctx, handle, request, preparedRuntime, runLogs)
	if err != nil {
		return nil, noopCommandCleanup, err
	}

	runtimeWorkspace, runtimeCommandWorkdir := ociWorkspaceTargets(
		workspace,
		workdir,
		request.Runtime.WorkDir,
	)

	env := mergeEnv(ociBaseEnv(prepared.Image.Env), request.Env)
	env = mergeEnvEntries(env, rmtxRunEnv(ctx, runtimeWorkspace, request.ContextID))

	binds := []ociBind{
		{Source: workspace, Target: runtimeWorkspace},
	}
	for _, volume := range request.Runtime.Volumes {
		binds = append(binds, ociBind{
			Source: filepath.Join(handle.runtimeDir, "volumes", volume.Name),
			Target: volume.Target,
		})
	}

	gpuRuntime, err := nvidiaRuntime(request.Runtime.GPU)
	if err != nil {
		return nil, noopCommandCleanup, nvidiaUnavailableError(err)
	}
	binds = append(binds, gpuRuntime.Binds...)
	env = mergeNvidiaRuntimeEnv(env, gpuRuntime)

	spec := ociChildSpec{
		RootFS:      prepared.RootFS,
		LowerRootFS: prepared.LowerRootFS,
		WorkDir:     runtimeCommandWorkdir,
		Command:     append([]string(nil), request.Command...),
		Env:         env,
		Binds:       binds,
		Network:     request.Runtime.Network,
		GPU:         request.Runtime.GPU,
		WSLDistro:   request.Runtime.WSLDistro,
	}

	return s.ociChildCommand(ctx, spec, handle.runtimeDir, runLogs)
}

func finishCommandWithCleanup(code int, runErr error, cleanup commandCleanup) (int, error) {
	if cleanup == nil {
		return code, runErr
	}

	cleanupErr := cleanup()
	if cleanupErr == nil {
		return code, runErr
	}

	if runErr != nil {
		return code, errors.Join(runErr, fmt.Errorf("clean OCI command spec: %w", cleanupErr))
	}

	return 1, fmt.Errorf("clean OCI command spec: %w", cleanupErr)
}

func ociBaseEnv(imageEnv []string) []string {
	return mergeEnvEntries([]string{defaultOCIPathEnv}, imageEnv)
}

func mergeNvidiaRuntimeEnv(env []string, runtime nvidiaRuntimeSpec) []string {
	env = mergeEnvEntries(env, runtime.Env)
	if len(runtime.PathPrefixes) == 0 {
		return env
	}

	current := ""
	for _, entry := range env {
		parts := strings.SplitN(entry, "=", splitNEquals)
		if parts[0] != "PATH" {
			continue
		}
		if len(parts) == splitNEquals {
			current = parts[1]
		}
		break
	}
	if current == "" {
		current = strings.TrimPrefix(defaultOCIPathEnv, "PATH=")
	}

	seen := map[string]bool{}
	paths := make([]string, 0, len(runtime.PathPrefixes)+len(strings.Split(current, ":")))
	for _, prefix := range runtime.PathPrefixes {
		prefix = strings.TrimSpace(prefix)
		if prefix == "" || seen[prefix] {
			continue
		}
		seen[prefix] = true
		paths = append(paths, prefix)
	}
	for path := range strings.SplitSeq(current, ":") {
		path = strings.TrimSpace(path)
		if path == "" || seen[path] {
			continue
		}
		seen[path] = true
		paths = append(paths, path)
	}

	return mergeEnvEntries(env, []string{"PATH=" + strings.Join(paths, ":")})
}

func mergeEnvEntries(base []string, overrides []string) []string {
	merged := map[string]string{}
	order := make([]string, 0, len(base)+len(overrides))

	for _, entry := range append(append([]string(nil), base...), overrides...) {
		parts := strings.SplitN(entry, "=", splitNEquals)
		key := parts[0]

		value := ""
		if len(parts) == splitNEquals {
			value = parts[1]
		}

		if _, ok := merged[key]; !ok {
			order = append(order, key)
		}

		merged[key] = value
	}

	out := make([]string, 0, len(order))
	for _, key := range order {
		out = append(out, key+"="+merged[key])
	}

	return out
}

func (s *Server) ensurePreparedOCIRuntime(
	ctx context.Context,
	handle contextHandle,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
	runLogs *hostLogSubscription,
) (preparedRuntime, error) {
	if preparedRuntime != nil {
		return *preparedRuntime, nil
	}

	return s.prepareOCIRuntime(ctx, handle, request, runLogs)
}

func ociWorkspaceTargets(workspace, workdir, configuredWorkdir string) (string, string) {
	runtimeWorkspace := strings.TrimSpace(configuredWorkdir)
	if runtimeWorkspace == "" {
		runtimeWorkspace = defaultOCIWorkdir
	}

	runtimeCommandWorkdir := runtimeWorkspace
	relWorkdir, err := filepath.Rel(workspace, workdir)
	if err == nil && relWorkdir != "." {
		runtimeCommandWorkdir = pathJoin(runtimeWorkspace, filepath.ToSlash(relWorkdir))
	}

	return runtimeWorkspace, runtimeCommandWorkdir
}

func ociStore(runtimeRoot string) *oci.Store {
	return oci.NewStore(filepath.Join(runtimeRoot, "cache", "oci"))
}

func runtimeCacheKey(manifestDigest string, runtimeSpec config.RuntimeConfig) string {
	network := strings.TrimSpace(runtimeSpec.Network)
	if network == "" {
		network = defaultOCINetwork
	}

	user := strings.TrimSpace(runtimeSpec.User)
	if user == "" {
		user = defaultOCIUser
	}

	gpu := strings.TrimSpace(runtimeSpec.GPU)
	if gpu == "" {
		gpu = noneValue
	}

	payload, _ := json.Marshal(struct {
		ManifestDigest string   `json:"manifest_digest"`
		ImageCommands  []string `json:"image_commands"`
		Network        string   `json:"network"`
		User           string   `json:"user"`
		GPU            string   `json:"gpu"`
		RootFSFormat   string   `json:"rootfs_format"`
	}{
		ManifestDigest: manifestDigest,
		ImageCommands:  runtimeSpec.Setup.ImageCommands,
		Network:        network,
		User:           user,
		GPU:            gpu,
		RootFSFormat:   rootFSCacheFormatVersion,
	})
	sum := sha256.Sum256(payload)

	return hex.EncodeToString(sum[:])[:24]
}

func contextSetupKey(
	workspace string,
	commandWorkDir string,
	runtimeSpec config.RuntimeConfig,
	preparedKey string,
) (string, error) {
	setup := runtimeSpec.Setup
	if len(setup.ContextInputs) == 0 {
		return "", nil
	}

	h := sha256.New()
	enc := json.NewEncoder(h)
	if err := enc.Encode(struct {
		PreparedKey     string                 `json:"prepared_key"`
		CommandWorkDir  string                 `json:"command_workdir"`
		WorkDir         string                 `json:"workdir"`
		Network         string                 `json:"network"`
		User            string                 `json:"user"`
		GPU             string                 `json:"gpu"`
		Volumes         []config.RuntimeVolume `json:"volumes"`
		ContextCommands []string               `json:"context_commands"`
	}{
		PreparedKey:     preparedKey,
		CommandWorkDir:  normalizedWorkDir(commandWorkDir),
		WorkDir:         runtimeSpec.WorkDir,
		Network:         runtimeSpec.Network,
		User:            runtimeSpec.User,
		GPU:             runtimeSpec.GPU,
		Volumes:         runtimeSpec.Volumes,
		ContextCommands: setup.ContextCommands,
	}); err != nil {
		return "", err
	}

	for _, input := range setup.ContextInputs {
		target, err := secureJoin(workspace, input)
		if err != nil {
			return "", err
		}

		content, err := os.ReadFile(target)
		if errors.Is(err, os.ErrNotExist) {
			_, _ = h.Write([]byte(input + "\x00missing\x00"))
			continue
		}

		if err != nil {
			return "", err
		}

		_, _ = h.Write([]byte(input + "\x00"))
		_, _ = h.Write(content)
		_, _ = h.Write([]byte("\x00"))
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func normalizedWorkDir(workdir string) string {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return "."
	}

	return filepath.ToSlash(filepath.Clean(workdir))
}

func loadContextSetupState(path string) (contextSetupState, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return contextSetupState{}, err
	}

	var state contextSetupState
	if err := json.Unmarshal(content, &state); err != nil {
		return contextSetupState{}, err
	}

	return state, nil
}

func saveContextSetupState(path string, state contextSetupState) error {
	if err := os.MkdirAll(filepath.Dir(path), defaultDirMode); err != nil {
		return err
	}

	return writeJSONAtomically(path, state, contextFileMode)
}

func (s *Server) ensureOCIVolumes(runtimeDir string, volumes []config.RuntimeVolume) error {
	for _, volume := range volumes {
		path := filepath.Join(runtimeDir, "volumes", volume.Name)
		if err := os.MkdirAll(path, defaultDirMode); err != nil {
			return fmt.Errorf("create runtime volume %s: %w", volume.Name, err)
		}
	}

	return nil
}

func saveArtifactState(
	runtimeDir string,
	image oci.Image,
	key string,
	rootfs string,
	baseRootFS string,
) error {
	path := filepath.Join(runtimeDir, runtimeDirName, artifactStateFile)

	img := artifactImage{
		Reference: image.Reference,
		Digest:    image.ManifestDigest,
		Blobs:     imageBlobDigests(image),
	}

	prepared := artifactPrepared{
		Key:            key,
		Path:           rootfs,
		BasePath:       baseRootFS,
		ImageDigest:    image.ManifestDigest,
		ImageReference: image.Reference,
	}

	state := artifactState{
		Images:    []artifactImage{img},
		Prepared:  []artifactPrepared{prepared},
		UpdatedAt: time.Now().UTC(),
	}

	return writeJSONAtomically(path, state, contextFileMode)
}

func imageBlobDigests(image oci.Image) []string {
	out := []string{image.ManifestDigest}
	if image.ConfigDigest != "" {
		out = append(out, image.ConfigDigest)
	}

	for _, layer := range image.Layers {
		out = append(out, layer.Digest)
	}

	return out
}

func loadArtifactState(path string) (artifactState, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return artifactState{}, err
	}

	var state artifactState
	if err := json.Unmarshal(content, &state); err != nil {
		return artifactState{}, err
	}

	return state, nil
}

func pathJoin(base, rel string) string {
	if rel == "" || rel == "." {
		return base
	}

	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(filepath.ToSlash(rel), "/")
}

func truncateOutput(out []byte) string {
	const limit = 16 * 1024
	if len(out) <= limit {
		return string(out)
	}

	return string(out[:limit]) + "\n... output truncated ..."
}
