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
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/manuel-huez/rmtx/internal/config"
	"github.com/manuel-huez/rmtx/internal/oci"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/version"
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
	defaultOCIWorkdir    = "/workspace"
	pullProgressFields   = 2
)

type preparedRuntime struct {
	RootFS string
	Image  oci.Image
	Key    string
}

type artifactState struct {
	Images    []artifactImage    `json:"images,omitempty"`
	Prepared  []artifactPrepared `json:"prepared,omitempty"`
	UpdatedAt time.Time          `json:"updated_at,omitempty"`
}

type artifactImage struct {
	Reference string   `json:"reference"`
	Digest    string   `json:"digest"`
	Blobs     []string `json:"blobs,omitempty"`
}

type artifactPrepared struct {
	Key            string `json:"key"`
	Path           string `json:"path"`
	ImageDigest    string `json:"image_digest"`
	ImageReference string `json:"image_reference"`
}

type contextSetupState struct {
	Key string `json:"key"`
}

func isOCIRuntime(runtime protocol.RuntimeSpec) bool {
	return strings.EqualFold(strings.TrimSpace(runtime.Type), "oci")
}

func validateRuntimeSpec(runtime protocol.RuntimeSpec) error {
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
		workspace,
		request,
		preparedRuntime,
		runLogs,
	); err != nil {
		input.Stop()
		return 1, err
	}
	runLogs.Flush()

	cmd, cleanup, err := s.newOCICommand(ctx, workspace, workdir, request, preparedRuntime, runLogs)
	if err != nil {
		input.Stop()
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
	s.ociMu.Lock()
	defer s.ociMu.Unlock()

	return s.prepareOCIRuntimeLocked(ctx, handle, request, runLogs)
}

func (s *Server) prepareOCIRuntimeLocked(
	ctx context.Context,
	handle contextHandle,
	request protocol.RunRequest,
	runLogs *hostLogSubscription,
) (preparedRuntime, error) {
	s.logRun(
		runLogs,
		"preparing OCI runtime: context=%s image=%s",
		request.ContextID,
		request.Runtime.Image,
	)

	ref, err := oci.ParseReference(request.Runtime.Image)
	if err != nil {
		return preparedRuntime{}, err
	}

	store := s.ociStore()
	if err := store.Ensure(); err != nil {
		return preparedRuntime{}, err
	}

	image, err := s.pullOCIImage(ctx, ref, request.Runtime, store, request.ContextID, runLogs)
	if err != nil {
		return preparedRuntime{}, err
	}

	key := runtimeCacheKey(image.ManifestDigest, request.Runtime)
	rootfs := filepath.Join(handle.dir, runtimeDirName, runtimeRootFSDirName, key)

	setupMarker := filepath.Join(rootfs, runtimeSetupMarker)
	if _, err := os.Stat(setupMarker); errors.Is(err, os.ErrNotExist) {
		s.logRun(
			runLogs,
			"unpacking OCI image: context=%s rootfs=%s image=%s",
			request.ContextID,
			rootfs,
			request.Runtime.Image,
		)

		if err := store.UnpackImageContext(ctx, rootfs, image); err != nil {
			return preparedRuntime{}, err
		}

		if err := writeRootFSInstanceMarker(rootfs); err != nil {
			_ = os.RemoveAll(rootfs)
			return preparedRuntime{}, err
		}

		if err := s.runImageSetupCommands(
			ctx,
			rootfs,
			request.Runtime,
			image.Env,
			runLogs,
		); err != nil {
			_ = os.RemoveAll(rootfs)
			return preparedRuntime{}, err
		}

		content := []byte(time.Now().UTC().Format(time.RFC3339) + "\n")
		if err := os.WriteFile(setupMarker, content, contextFileMode); err != nil {
			return preparedRuntime{}, err
		}
	} else {
		s.logRun(
			runLogs,
			"using existing OCI runtime setup marker: context=%s image=%s",
			request.ContextID,
			request.Runtime.Image,
		)

		if err := ensureRootFSInstanceMarker(rootfs); err != nil {
			return preparedRuntime{}, err
		}
	}

	if err := s.ensureOCIVolumes(handle.dir, request.Runtime.Volumes); err != nil {
		return preparedRuntime{}, err
	}

	if err := savePreparedRuntimeState(handle.dir, image, key, rootfs); err != nil {
		return preparedRuntime{}, err
	}

	return preparedRuntime{RootFS: rootfs, Image: image, Key: key}, nil
}

func savePreparedRuntimeState(contextDir string, image oci.Image, key string, rootfs string) error {
	if err := saveArtifactState(contextDir, image, key, rootfs); err != nil {
		return err
	}

	_, err := pruneStalePreparedRuntimes(contextDir)

	return err
}

func (s *Server) pullOCIImage(
	ctx context.Context,
	ref oci.Reference,
	runtimeSpec protocol.RuntimeSpec,
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

	client := oci.NewClient(&http.Client{Timeout: 0})
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
	content := []byte(fmt.Sprintf("%d-%d\n", time.Now().UTC().UnixNano(), os.Getpid()))
	if err := os.WriteFile(filepath.Join(rootfs, rootFSInstanceMarker), content, contextFileMode); err != nil {
		return fmt.Errorf("write rootfs instance marker: %w", err)
	}

	return nil
}

func (s *Server) runImageSetupCommands(
	ctx context.Context,
	rootfs string,
	runtimeSpec protocol.RuntimeSpec,
	imageEnv []string,
	runLogs *hostLogSubscription,
) error {
	gpuRuntime, err := nvidiaRuntime(runtimeSpec.GPU)
	if err != nil {
		return nvidiaUnavailableError(err)
	}

	env := append(ociBaseEnv(imageEnv), gpuRuntime.Env...)

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
	}

	return nil
}

func (s *Server) prepareOCIContextSetup(
	ctx context.Context,
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
		dir:       filepath.Join(s.contextsRoot(), request.ContextID),
		workspace: workspace,
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
		s.contextsRoot(),
		request.ContextID,
		runtimeDirName,
		contextSetupFile,
	)
	if contextSetupCacheHit(statePath, key) {
		return nil
	}

	if err := s.runOCIContextSetupCommands(
		ctx,
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
	workspace string,
	workdir string,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
	runLogs *hostLogSubscription,
) (*exec.Cmd, commandCleanup, error) {
	handle := contextHandle{
		dir:       filepath.Join(s.contextsRoot(), request.ContextID),
		workspace: workspace,
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
	env = append(env,
		"RMTX=1",
		"RMTX_WORKSPACE="+runtimeWorkspace,
		"RMTX_CONTEXT_ID="+request.ContextID,
	)

	binds := []ociBind{
		{Source: workspace, Target: runtimeWorkspace},
	}
	for _, volume := range request.Runtime.Volumes {
		binds = append(binds, ociBind{
			Source: filepath.Join(handle.dir, "volumes", volume.Name),
			Target: volume.Target,
		})
	}

	gpuRuntime, err := nvidiaRuntime(request.Runtime.GPU)
	if err != nil {
		return nil, noopCommandCleanup, nvidiaUnavailableError(err)
	}
	binds = append(binds, gpuRuntime.Binds...)
	env = append(env, gpuRuntime.Env...)

	spec := ociChildSpec{
		RootFS:    prepared.RootFS,
		WorkDir:   runtimeCommandWorkdir,
		Command:   append([]string(nil), request.Command...),
		Env:       env,
		Binds:     binds,
		Network:   request.Runtime.Network,
		GPU:       request.Runtime.GPU,
		WSLDistro: request.Runtime.WSLDistro,
	}

	return s.ociChildCommand(ctx, spec, handle.dir, runLogs)
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

func (s *Server) ociStore() *oci.Store {
	return oci.NewStore(filepath.Join(s.opts.StateDir, "cache", "oci"))
}

func runtimeCacheKey(manifestDigest string, runtimeSpec protocol.RuntimeSpec) string {
	network := strings.TrimSpace(runtimeSpec.Network)
	if network == "" {
		network = "host"
	}

	user := strings.TrimSpace(runtimeSpec.User)
	if user == "" {
		user = "root"
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
		RuntimeVersion string   `json:"runtime_version"`
	}{
		ManifestDigest: manifestDigest,
		ImageCommands:  runtimeSpec.Setup.ImageCommands,
		Network:        network,
		User:           user,
		GPU:            gpu,
		RuntimeVersion: version.String(),
	})
	sum := sha256.Sum256(payload)

	return hex.EncodeToString(sum[:])[:24]
}

func contextSetupKey(
	workspace string,
	commandWorkDir string,
	runtimeSpec protocol.RuntimeSpec,
	preparedKey string,
) (string, error) {
	setup := runtimeSpec.Setup
	if len(setup.ContextInputs) == 0 {
		return "", nil
	}

	h := sha256.New()
	enc := json.NewEncoder(h)
	if err := enc.Encode(struct {
		PreparedKey     string                   `json:"prepared_key"`
		CommandWorkDir  string                   `json:"command_workdir"`
		WorkDir         string                   `json:"workdir"`
		Network         string                   `json:"network"`
		User            string                   `json:"user"`
		GPU             string                   `json:"gpu"`
		Volumes         []protocol.RuntimeVolume `json:"volumes"`
		ContextCommands []string                 `json:"context_commands"`
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

	return writeIndentedJSON(path, state)
}

func (s *Server) ensureOCIVolumes(contextDir string, volumes []protocol.RuntimeVolume) error {
	for _, volume := range volumes {
		path := filepath.Join(contextDir, "volumes", volume.Name)
		if err := os.MkdirAll(path, defaultDirMode); err != nil {
			return fmt.Errorf("create runtime volume %s: %w", volume.Name, err)
		}
	}

	return nil
}

func saveArtifactState(contextDir string, image oci.Image, key string, rootfs string) error {
	path := filepath.Join(contextDir, runtimeDirName, artifactStateFile)

	img := artifactImage{
		Reference: image.Reference,
		Digest:    image.ManifestDigest,
		Blobs:     imageBlobDigests(image),
	}

	prepared := artifactPrepared{
		Key:            key,
		Path:           rootfs,
		ImageDigest:    image.ManifestDigest,
		ImageReference: image.Reference,
	}

	state := artifactState{
		Images:    []artifactImage{img},
		Prepared:  []artifactPrepared{prepared},
		UpdatedAt: time.Now().UTC(),
	}

	return writeIndentedJSON(path, state)
}

func writeIndentedJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), defaultDirMode); err != nil {
		return err
	}

	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, append(content, '\n'), contextFileMode)
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
