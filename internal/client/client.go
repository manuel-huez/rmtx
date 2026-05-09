package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/manuel-huez/rmtx/internal/clientstate"

	"github.com/manuel-huez/rmtx/internal/pathutil"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/security"
	"github.com/manuel-huez/rmtx/internal/syncfs"
	"github.com/manuel-huez/rmtx/internal/terminal"
)

type ExecOptions struct {
	Address          string
	DiscoveryService string
	Host             clientstate.HostRecord
	ClientCertPEM    []byte
	ClientKeyPEM     []byte
	Root             string
	CWD              string
	Command          []string
	Mounts           []syncfs.MountSpec
	SyncBack         []string
	Runtime          protocol.RuntimeSpec
	ForwardEnv       []string
	ExtraEnv         map[string]string
	Stdout           io.Writer
	Stderr           io.Writer
	Stdin            io.Reader
	StdinFile        *os.File
	StdoutFile       *os.File
	StderrFile       *os.File
	ForwardStdin     bool
	Session          string
	Project          string
	ContextID        string
	ContextName      string
	TTY              bool
}

type RemoteOptions struct {
	Address          string
	DiscoveryService string
	Host             clientstate.HostRecord
	ClientCertPEM    []byte
	ClientKeyPEM     []byte
	Stderr           io.Writer
}

type PingInfo = protocol.PingResponse
type HostStatsInfo = protocol.HostStatsResponse
type HostUpdateResult = protocol.HostUpdateResponse
type ContextInfo = protocol.ContextSummary
type DeleteContextsResult = protocol.DeleteContextsResponse
type ContextArtifactsResult = protocol.ContextArtifactsResponse
type ContextArtifact = protocol.ContextArtifact
type CachePruneResult = protocol.CachePruneResponse

type DeleteContextsOptions struct {
	Remote    RemoteOptions
	IDs       []string
	All       bool
	OlderThan string
}

type ContextArtifactsOptions struct {
	Remote    RemoteOptions
	ContextID string
	Prune     bool
	Delete    bool
	Volume    string
}

const stdinBufferSize = 32 * 1024
const (
	progressEvery           = 3 * time.Second
	maxBlobTransferParallel = 16
)

var (
	sessionIdleTimeout       = 45 * time.Second
	sessionHeartbeatInterval = 10 * time.Second
)

type runLogger struct {
	out       io.Writer
	mu        sync.Mutex
	lastByID  map[string]time.Time
	started   time.Time
	lastStage time.Time
}

func newRunLogger(out io.Writer) *runLogger {
	if out == nil {
		out = io.Discard
	}

	now := time.Now()
	return &runLogger{
		out:       out,
		lastByID:  map[string]time.Time{},
		started:   now,
		lastStage: now,
	}
}

func (l *runLogger) Printf(format string, args ...any) {
	if l == nil || l.out == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	_, _ = fmt.Fprintf(l.out, "rmtx: "+format+"\n", args...)
}

func (l *runLogger) Stage(format string, args ...any) {
	if l == nil || l.out == nil {
		return
	}

	now := time.Now()
	message := fmt.Sprintf(format, args...)

	l.mu.Lock()
	elapsed := now.Sub(l.lastStage).Round(time.Millisecond)
	total := now.Sub(l.started).Round(time.Millisecond)
	l.lastStage = now
	_, _ = fmt.Fprintf(
		l.out,
		"rmtx: === %s === elapsed=%s total=%s\n",
		message,
		elapsed,
		total,
	)
	l.mu.Unlock()
}

func (l *runLogger) Every(id, format string, args ...any) {
	if l == nil {
		return
	}

	now := time.Now()

	l.mu.Lock()

	last := l.lastByID[id]
	if !last.IsZero() && now.Sub(last) < progressEvery {
		l.mu.Unlock()
		return
	}

	l.lastByID[id] = now
	l.mu.Unlock()

	l.Printf(format, args...)
}

func (l *runLogger) ReportBuildProgress(progress syncfs.BuildProgress) {
	switch progress.Phase {
	case "walk":
		if progress.Done {
			l.Printf(
				"sync scan done: mount=%s scanned=%d files=%d dirs=%d symlinks=%d skipped=%d",
				progress.Mount,
				progress.Scanned,
				progress.Files,
				progress.Dirs,
				progress.Symlinks,
				progress.Skipped,
			)

			return
		}

		l.Printf(
			"sync scan progress: mount=%s scanned=%d files=%d dirs=%d skipped=%d",
			progress.Mount,
			progress.Scanned,
			progress.Files,
			progress.Dirs,
			progress.Skipped,
		)
	case "hash":
		if progress.Done {
			l.Printf(
				"sync hash done: files=%d/%d bytes=%d",
				progress.Hashed,
				progress.TotalFiles,
				progress.Bytes,
			)

			return
		}

		l.Printf(
			"sync hash progress: files=%d/%d bytes=%d",
			progress.Hashed,
			progress.TotalFiles,
			progress.Bytes,
		)
	}
}

func closeQuietly(c io.Closer) {
	if c != nil {
		_ = c.Close()
	}
}

func formatCommand(args []string) string {
	if len(args) == 0 {
		return "(none)"
	}

	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, quoteCommandArg(arg))
	}

	return strings.Join(quoted, " ")
}

func quoteCommandArg(arg string) string {
	if arg == "" {
		return "''"
	}

	if !commandArgNeedsQuote(arg) {
		return arg
	}

	return "'" + strings.ReplaceAll(arg, "'", `'\''`) + "'"
}

func commandArgNeedsQuote(arg string) bool {
	return strings.IndexFunc(arg, func(r rune) bool {
		return unicode.IsSpace(r) || strings.ContainsRune(`'"\\$&;()<>|*?[]{}!#~`, r)
	}) >= 0
}

func startConnectionLiveness(
	ctx context.Context,
	conn *protocol.Conn,
	expectPeerHeartbeat bool,
) func() {
	if conn == nil || conn.Raw() == nil {
		return func() {}
	}

	if idleConn, ok := conn.Raw().(interface{ SetWriteIdleTimeout(time.Duration) }); ok {
		idleConn.SetWriteIdleTimeout(sessionIdleTimeout)
	}
	if expectPeerHeartbeat {
		if idleConn, ok := conn.Raw().(interface{ SetReadIdleTimeout(time.Duration) }); ok {
			idleConn.SetReadIdleTimeout(sessionIdleTimeout)
		}
	}

	stopHeartbeat := protocol.StartHeartbeat(
		ctx,
		conn,
		sessionHeartbeatInterval,
		func(error) { closeQuietly(conn.Raw()) },
	)

	return func() {
		stopHeartbeat()
		if idleConn, ok := conn.Raw().(interface{ SetWriteIdleTimeout(time.Duration) }); ok {
			idleConn.SetWriteIdleTimeout(0)
		}
		if idleConn, ok := conn.Raw().(interface{ SetReadIdleTimeout(time.Duration) }); ok {
			idleConn.SetReadIdleTimeout(0)
		}
	}
}

func Run(ctx context.Context, opts ExecOptions) (int, error) {
	root, workdir, err := resolvePaths(&opts)
	if err != nil {
		return 1, err
	}

	opts.Root = root

	logger := newRunLogger(opts.Stderr)
	logger.Stage("prepare remote run")
	logger.Printf(
		"preparing remote run: host=%s context=%s workdir=%s command=%s",
		opts.Address,
		opts.ContextID,
		workdir,
		formatCommand(opts.Command),
	)

	manifest, request, err := buildRunRequest(ctx, root, workdir, &opts, logger)
	if err != nil {
		return 1, err
	}

	logger.Printf("connecting to host: %s", opts.Address)

	conn, err := updatedRemoteConn(
		ctx,
		RemoteOptions{
			Address:          opts.Address,
			DiscoveryService: opts.DiscoveryService,
			Host:             opts.Host,
			ClientCertPEM:    opts.ClientCertPEM,
			ClientKeyPEM:     opts.ClientKeyPEM,
			Stderr:           opts.Stderr,
		},
	)
	if err != nil {
		return 1, err
	}
	defer closeQuietly(conn.Raw())

	ready, stopRunSession, err := runHandshakeWithLiveness(
		ctx,
		conn,
		request,
		manifest.BlobSources,
		opts,
		logger,
	)
	if err != nil {
		return 1, err
	}
	defer stopRunSession()

	logger.Printf(
		"remote workspace ready: context=%s workspace=%s",
		ready.ContextID,
		ready.Workspace,
	)
	logger.Stage("execute remote command")
	logger.Printf("command: %s", formatCommand(opts.Command))

	code, err := processExecFrames(ctx, conn, root, opts, logger)
	if err == nil {
		logger.Printf("remote run finished: exit_code=%d", code)
	}

	return code, err
}

func resolvePaths(opts *ExecOptions) (string, string, error) {
	if err := validateExecOptions(opts); err != nil {
		return "", "", err
	}

	setDefaultOutputs(opts)

	root, err := resolveRoot(opts.Root)
	if err != nil {
		return "", "", err
	}

	cwd, err := resolveCWD(opts.CWD)
	if err != nil {
		return "", "", err
	}

	workdir, err := computeWorkdir(root, cwd)
	if err != nil {
		return "", "", err
	}

	return root, workdir, nil
}

func validateExecOptions(opts *ExecOptions) error {
	if strings.TrimSpace(opts.Address) == "" {
		return errors.New("host address is required")
	}

	if strings.TrimSpace(opts.Host.Fingerprint) == "" {
		return errors.New("host fingerprint is required")
	}

	if len(opts.Command) == 0 {
		return errors.New("command is required")
	}

	if strings.TrimSpace(opts.ContextID) == "" {
		return errors.New("context id is required")
	}

	return nil
}

func setDefaultOutputs(opts *ExecOptions) {
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}

	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
}

func resolveRoot(root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("get working directory: %w", err)
		}

		root = cwd
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve root: %w", err)
	}

	return absRoot, nil
}

func resolveCWD(cwd string) (string, error) {
	if strings.TrimSpace(cwd) != "" {
		return cwd, nil
	}

	current, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}

	return current, nil
}

func computeWorkdir(root, cwd string) (string, error) {
	workdir, err := filepath.Rel(root, cwd)
	if err != nil {
		return "", fmt.Errorf("compute workdir: %w", err)
	}

	workdir = filepath.ToSlash(filepath.Clean(workdir))
	if strings.HasPrefix(workdir, "../") || workdir == ".." {
		return "", fmt.Errorf("current directory %s is outside project root %s", cwd, root)
	}

	return workdir, nil
}

func buildRunRequest(
	ctx context.Context,
	root string,
	workdir string,
	opts *ExecOptions,
	logger *runLogger,
) (syncfs.BuildResult, protocol.RunRequest, error) {
	logger.Printf("local-to-host sync started: root=%s mounts=%s", root, formatMounts(opts.Mounts))

	if err := syncfs.ValidateSyncBack(root, opts.Mounts, opts.SyncBack); err != nil {
		return syncfs.BuildResult{}, protocol.RunRequest{}, err
	}

	previous := loadManifestCache(root, opts.ContextID, logger)

	manifest, err := syncfs.BuildManifestContextOptions(
		ctx,
		root,
		opts.Mounts,
		syncfs.BuildOptions{
			Progress:        logger.ReportBuildProgress,
			PreviousEntries: previous,
		},
	)
	if err != nil {
		return syncfs.BuildResult{}, protocol.RunRequest{}, err
	}

	saveManifestCache(root, opts.ContextID, manifest.Entries, logger)

	logger.Printf(
		"local manifest ready: entries=%d unique_file_blobs=%d",
		len(manifest.Entries),
		len(manifest.BlobSources),
	)

	if opts.Session == "" {
		opts.Session, err = protocol.RandomNonce()
		if err != nil {
			return syncfs.BuildResult{}, protocol.RunRequest{}, err
		}
	}

	env := collectEnv(opts.ForwardEnv, opts.ExtraEnv)
	if opts.TTY {
		if _, ok := env["TERM"]; !ok {
			if term := strings.TrimSpace(os.Getenv("TERM")); term != "" {
				env["TERM"] = term
			}
		}
	}

	rows, cols := 0, 0

	if opts.TTY {
		sizeFile := opts.StdoutFile
		if !terminal.IsTerminal(sizeFile) {
			sizeFile = opts.StdinFile
		}

		var sizeErr error

		rows, cols, sizeErr = terminal.Size(sizeFile)
		if sizeErr != nil {
			rows, cols = 0, 0
		}
	}

	request := protocol.RunRequest{
		ContextID:   opts.ContextID,
		ContextName: opts.ContextName,
		WorkDir:     workdir,
		Command:     append([]string(nil), opts.Command...),
		Env:         env,
		Runtime:     cloneRuntimeSpec(opts.Runtime),
		Mounts:      append([]syncfs.MountSpec(nil), opts.Mounts...),
		Manifest:    manifest.Entries,
		SyncBack:    cloneStringSlice(opts.SyncBack),
		Session:     opts.Session,
		Project:     opts.Project,
		RootHint:    filepath.Base(root),
		TTY:         opts.TTY,
		TTYRows:     rows,
		TTYCols:     cols,
	}

	return manifest, request, nil
}

func cloneRuntimeSpec(runtime protocol.RuntimeSpec) protocol.RuntimeSpec {
	runtime.Setup.ImageCommands = cloneStringSlice(runtime.Setup.ImageCommands)
	runtime.Setup.ContextCommands = cloneStringSlice(runtime.Setup.ContextCommands)
	runtime.Setup.ContextInputs = cloneStringSlice(runtime.Setup.ContextInputs)
	runtime.Volumes = append([]protocol.RuntimeVolume(nil), runtime.Volumes...)

	return runtime
}

func cloneStringSlice(values []string) []string {
	if values == nil {
		return nil
	}

	return append([]string(nil), values...)
}

func loadManifestCache(root, contextID string, logger *runLogger) []syncfs.Entry {
	previous, err := loadCachedManifest(root, contextID)
	if err != nil {
		logger.Printf("local manifest cache ignored: %v", err)

		return nil
	}

	if len(previous) > 0 {
		logger.Printf("local manifest cache loaded: entries=%d", len(previous))
	}

	return previous
}

func saveManifestCache(root, contextID string, entries []syncfs.Entry, logger *runLogger) {
	if err := saveCachedManifest(root, contextID, entries); err != nil {
		logger.Printf("local manifest cache save failed: %v", err)
	}
}

func runHandshake(
	ctx context.Context,
	conn *protocol.Conn,
	request protocol.RunRequest,
	blobSources map[string]string,
	opts ExecOptions,
	logger *runLogger,
) (protocol.WorkspaceReady, error) {
	logger.Printf("sending run request and manifest: entries=%d", len(request.Manifest))

	if err := conn.WriteJSON(protocol.MsgRunRequest, request); err != nil {
		return protocol.WorkspaceReady{}, err
	}

	ready, stopCancelForwarder, err := finishRunHandshake(
		ctx,
		conn,
		blobSources,
		opts,
		logger,
		nil,
	)
	if stopCancelForwarder != nil {
		stopCancelForwarder()
	}

	return ready, err
}

func runHandshakeWithLiveness(
	ctx context.Context,
	conn *protocol.Conn,
	request protocol.RunRequest,
	blobSources map[string]string,
	opts ExecOptions,
	logger *runLogger,
) (protocol.WorkspaceReady, func(), error) {
	logger.Printf("sending run request and manifest: entries=%d", len(request.Manifest))

	if err := conn.WriteJSON(protocol.MsgRunRequest, request); err != nil {
		return protocol.WorkspaceReady{}, nil, err
	}

	// Before sync_complete, run_cancel would collide with blob/sync frames.
	stopPreSyncCancel := context.AfterFunc(ctx, func() { closeQuietly(conn.Raw()) })
	stopLiveness := startConnectionLiveness(ctx, conn, true)

	ready, stopCancelForwarder, err := finishRunHandshake(
		ctx,
		conn,
		blobSources,
		opts,
		logger,
		func() { stopPreSyncCancel() },
	)
	if err != nil {
		stopPreSyncCancel()
		stopLiveness()
		if stopCancelForwarder != nil {
			stopCancelForwarder()
		}
		return protocol.WorkspaceReady{}, nil, err
	}

	return ready, func() {
		stopPreSyncCancel()
		stopLiveness()
		if stopCancelForwarder != nil {
			stopCancelForwarder()
		}
	}, nil
}

func finishRunHandshake(
	ctx context.Context,
	conn *protocol.Conn,
	blobSources map[string]string,
	opts ExecOptions,
	logger *runLogger,
	onSyncComplete func(),
) (protocol.WorkspaceReady, func(), error) {
	need, err := expectDataFrameWithOutput[protocol.NeedBlobs](
		conn,
		protocol.MsgNeedBlobs,
		opts.Stderr,
	)
	if err != nil {
		return protocol.WorkspaceReady{}, nil, err
	}

	logger.Stage("upload local files")
	if err := sendMissingBlobs(ctx, conn, need, blobSources, opts, logger); err != nil {
		return protocol.WorkspaceReady{}, nil, err
	}

	if err := conn.WriteJSON(protocol.MsgSyncComplete, nil); err != nil {
		return protocol.WorkspaceReady{}, nil, err
	}

	if onSyncComplete != nil {
		onSyncComplete()
	}

	// After sync_complete, the host can queue run_cancel while still streaming logs.
	stopCancelForwarder := startRunCancelForwarder(ctx, conn, logger)

	ready, err := expectDataFrameWithOutput[protocol.WorkspaceReady](
		conn,
		protocol.MsgWorkspaceReady,
		opts.Stderr,
	)
	if err != nil {
		stopCancelForwarder()
		return protocol.WorkspaceReady{}, nil, err
	}

	logger.Printf("local-to-host sync complete")

	return ready, stopCancelForwarder, nil
}

func expectDataFrameWithOutput[T any](
	conn *protocol.Conn,
	wantType string,
	output io.Writer,
) (T, error) {
	var zero T

	if output == nil {
		output = io.Discard
	}

	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return zero, err
		}

		switch head.Type {
		case protocol.MsgHeartbeat:
			if err := conn.DiscardPayload(head); err != nil {
				return zero, err
			}
		case protocol.MsgError:
			return zero, decodeServerError(head)
		case protocol.MsgExecOutput:
			if err := copyExecOutputTo(conn, head, output); err != nil {
				return zero, err
			}
		case wantType:
			return protocol.DecodeData[T](head)
		default:
			if err := conn.DiscardPayload(head); err != nil {
				return zero, err
			}

			return zero, fmt.Errorf("expected %s, got %s", wantType, head.Type)
		}
	}
}

func processExecFrames(
	ctx context.Context,
	conn *protocol.Conn,
	root string,
	opts ExecOptions,
	logger *runLogger,
) (int, error) {
	if opts.TTY {
		return processTTYExecFrames(ctx, conn, root, opts, logger)
	}

	stdinErrCh := make(chan error, 1)

	go func() { stdinErrCh <- sendStdin(conn, opts.Stdin, opts.ForwardStdin) }()

	var (
		exitCode         int
		downloadProgress *transferProgress
	)

	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return exitCode, err
		}

		done, updatedExitCode, err := handleExecFrame(
			ctx,
			conn,
			head,
			root,
			opts,
			exitCode,
			logger,
			&downloadProgress,
		)
		if err != nil {
			return updatedExitCode, err
		}

		exitCode = updatedExitCode

		if done {
			if err := waitForStdinForwarding(ctx, stdinErrCh); err != nil {
				return exitCode, err
			}

			return exitCode, nil
		}
	}
}

func waitForStdinForwarding(ctx context.Context, stdinErrCh <-chan error) error {
	select {
	case err := <-stdinErrCh:
		if ctx.Err() != nil {
			return nil
		}

		return err
	case <-ctx.Done():
		return nil
	}
}

func startRunCancelForwarder(
	ctx context.Context,
	conn *protocol.Conn,
	logger *runLogger,
) func() {
	done := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done():
			logger.Printf("interrupt received; requesting remote command cancel before sync-back")
			if err := conn.WriteJSON(protocol.MsgRunCancel, nil); err != nil {
				logger.Printf("remote cancel request failed: %v", err)
			}
		case <-done:
		}
	}()

	return func() { close(done) }
}

func processTTYExecFrames(
	ctx context.Context,
	conn *protocol.Conn,
	root string,
	opts ExecOptions,
	logger *runLogger,
) (int, error) {
	session, inputErrCh, err := startTTYInput(ctx, conn, opts)
	if err != nil {
		return 1, err
	}

	if session != nil {
		defer session.Close()
	}

	var (
		exitCode         int
		downloadProgress *transferProgress
	)

	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return exitCode, err
		}

		if head.Type == protocol.MsgExecExit && session != nil {
			session.Close()
			session = nil
		}

		done, updatedExitCode, err := handleExecFrame(
			ctx,
			conn,
			head,
			root,
			opts,
			exitCode,
			logger,
			&downloadProgress,
		)
		if err != nil {
			return updatedExitCode, err
		}

		exitCode = updatedExitCode

		if done {
			if err := consumeTTYInputErr(inputErrCh); err != nil {
				return exitCode, err
			}

			return exitCode, nil
		}
	}
}

func consumeTTYInputErr(inputErrCh <-chan error) error {
	if inputErrCh == nil {
		return nil
	}

	select {
	case inputErr := <-inputErrCh:
		if inputErr != nil && !errors.Is(inputErr, io.EOF) {
			return inputErr
		}
	default:
	}

	return nil
}

func handleExecFrame(
	ctx context.Context,
	conn *protocol.Conn,
	head protocol.Header,
	root string,
	opts ExecOptions,
	exitCode int,
	logger *runLogger,
	downloadProgress **transferProgress,
) (bool, int, error) {
	switch head.Type {
	case protocol.MsgHeartbeat:
		return false, exitCode, conn.DiscardPayload(head)
	case protocol.MsgError:
		return false, exitCode, decodeServerError(head)
	case protocol.MsgExecOutput:
		return false, exitCode, copyExecOutput(conn, head, opts)
	case protocol.MsgExecExit:
		code, err := decodeExitCode(head)
		if err == nil {
			logger.Printf("command exited: exit_code=%d", code)
			logger.Stage("download remote changes")
		}

		return false, code, err
	case protocol.MsgChangeSet:
		err := syncWorkspaceChangesFromHost(context.WithoutCancel(ctx), conn, head, root, opts, logger)
		*downloadProgress = nil

		return false, exitCode, err
	case protocol.MsgChangesDone:
		if *downloadProgress != nil {
			(*downloadProgress).Done()
		}

		logger.Printf("host-to-local sync complete")

		return true, exitCode, nil
	default:
		if err := conn.DiscardPayload(head); err != nil {
			return false, exitCode, err
		}

		return false, exitCode, fmt.Errorf("unexpected frame %s", head.Type)
	}
}

func copyExecOutput(conn *protocol.Conn, head protocol.Header, opts ExecOptions) error {
	info, err := protocol.DecodeData[protocol.OutputInfo](head)
	if err != nil {
		return err
	}

	dst := opts.Stdout
	if info.Stream == "stderr" {
		dst = opts.Stderr
	}

	_, err = io.Copy(dst, conn.PayloadReader(head))

	return err
}

func copyExecOutputTo(conn *protocol.Conn, head protocol.Header, stderr io.Writer) error {
	info, err := protocol.DecodeData[protocol.OutputInfo](head)
	if err != nil {
		return err
	}

	dst := stderr
	if info.Stream == "stdout" {
		dst = io.Discard
	}

	if dst == nil {
		dst = io.Discard
	}

	_, err = io.Copy(dst, conn.PayloadReader(head))

	return err
}

func decodeExitCode(head protocol.Header) (int, error) {
	info, err := protocol.DecodeData[protocol.ExitInfo](head)
	if err != nil {
		return 0, err
	}

	return info.Code, nil
}

func syncWorkspaceChangesFromHost(
	ctx context.Context,
	conn *protocol.Conn,
	head protocol.Header,
	root string,
	opts ExecOptions,
	logger *runLogger,
) error {
	changes, err := protocol.DecodeData[protocol.ChangeSet](head)
	if err != nil {
		return err
	}

	files, bytes := changeSetFileTotals(changes.Entries)
	logger.Printf(
		"applying host changes locally: changed=%d deleted=%d file_bytes=%s",
		len(changes.Entries),
		len(changes.Deleted),
		formatBytes(bytes),
	)

	if err := syncfs.DeletePaths(root, changes.Deleted); err != nil {
		return err
	}

	if err := syncfs.ApplyNonFileEntries(root, syncfs.NonFileEntries(changes.Entries)); err != nil {
		return err
	}

	store, err := clientBlobStore()
	if err != nil {
		return err
	}

	missing := store.MissingHashes(changes.Entries)
	sort.Strings(missing)

	if err := conn.WriteJSON(protocol.MsgNeedBlobs, protocol.NeedBlobs{Hashes: missing}); err != nil {
		return err
	}

	need, err := expectDataFrameWithOutput[protocol.NeedBlobs](
		conn,
		protocol.MsgNeedBlobs,
		opts.Stderr,
	)
	if err != nil {
		return err
	}

	if len(need.Hashes) > 0 {
		missingFiles, missingBytes := selectedFileTotals(changes.Entries, need.Hashes)
		transferProgress := newTransferProgress("download-transfer", logger, missingFiles, missingBytes)
		if err := receiveMissingBlobsFromHost(ctx, need, changes.Entries, store, opts, transferProgress); err != nil {
			return err
		}
		transferProgress.Done()
	}

	progress := newTransferProgress("download", logger, files, bytes)
	if err := materializeDownloadedChanges(ctx, root, store, changes.Entries, progress); err != nil {
		return err
	}
	progress.Done()

	updateManifestCacheAfterSync(root, opts.ContextID, changes, logger)

	return conn.WriteJSON(protocol.MsgSyncComplete, nil)
}

func changeSetFileTotals(entries []syncfs.Entry) (int, int64) {
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

func selectedFileTotals(entries []syncfs.Entry, hashes []string) (int, int64) {
	selected := make(map[string]struct{}, len(hashes))
	for _, hash := range hashes {
		selected[hash] = struct{}{}
	}

	seen := map[string]struct{}{}
	var bytes int64
	for _, entry := range entries {
		if entry.Kind != syncfs.KindFile {
			continue
		}
		if _, ok := selected[entry.Hash]; !ok {
			continue
		}
		if _, ok := seen[entry.Hash]; ok {
			continue
		}
		seen[entry.Hash] = struct{}{}
		bytes += entry.Size
	}

	return len(seen), bytes
}

func receiveMissingBlobsFromHost(
	ctx context.Context,
	need protocol.NeedBlobs,
	entries []syncfs.Entry,
	store *syncfs.BlobStore,
	opts ExecOptions,
	progress *transferProgress,
) error {
	if len(need.Hashes) == 0 {
		return nil
	}
	if need.TransferToken == "" {
		return errors.New("host did not provide blob transfer token")
	}

	chunkSize := need.ChunkSize
	if chunkSize <= 0 {
		chunkSize = protocol.DefaultBlobChunkSize
	}

	descriptors, err := descriptorsForHashes(entries, need.Hashes)
	if err != nil {
		return err
	}

	receiver, err := syncfs.NewChunkedBlobReceiver(store, descriptors, chunkSize)
	if err != nil {
		return err
	}

	chunks := protocol.PlanBlobChunks(descriptors, chunkSize)
	parallel := downloadParallelism(need, len(chunks))
	chunkProgress := newBlobChunkProgress(progress, chunks)
	groups := downloadChunkGroups(chunks, parallel)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh, done := startDownloadBlobWorkers(
		ctx,
		cancel,
		groups,
		need.TransferToken,
		chunkSize,
		opts,
		receiver,
		chunkProgress,
	)

	return waitForBlobDownload(ctx, cancel, receiver, errCh, done)
}

func startDownloadBlobWorkers(
	ctx context.Context,
	cancel context.CancelFunc,
	groups [][]protocol.BlobChunkInfo,
	token string,
	chunkSize int64,
	opts ExecOptions,
	receiver *syncfs.ChunkedBlobReceiver,
	chunkProgress *blobChunkProgress,
) (<-chan error, <-chan struct{}) {
	errCh := make(chan error, 1)
	done := make(chan struct{})

	var wg sync.WaitGroup
	for _, group := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := downloadBlobWorker(ctx, group, token, chunkSize, opts, receiver, chunkProgress); err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	return errCh, done
}

func downloadChunkGroups(chunks []protocol.BlobChunkInfo, parallel int) [][]protocol.BlobChunkInfo {
	if parallel < 1 {
		parallel = 1
	}
	if parallel > len(chunks) {
		parallel = len(chunks)
	}
	if parallel < 1 {
		return nil
	}

	groups := make([][]protocol.BlobChunkInfo, parallel)
	for i, chunk := range chunks {
		groups[i%parallel] = append(groups[i%parallel], chunk)
	}

	return groups
}

func waitForBlobDownload(
	ctx context.Context,
	cancel context.CancelFunc,
	receiver *syncfs.ChunkedBlobReceiver,
	errCh <-chan error,
	done <-chan struct{},
) error {
	receiverDone := make(chan error, 1)
	go func() { receiverDone <- receiver.Wait(ctx) }()

	for {
		select {
		case err := <-errCh:
			_ = receiver.Fail(err)
			return err
		case err := <-receiverDone:
			if err != nil {
				cancel()
				return err
			}
			receiverDone = nil
		case <-done:
			if receiverDone != nil {
				continue
			}
			return nil
		case <-ctx.Done():
			_ = receiver.Fail(ctx.Err())
			return ctx.Err()
		}
	}
}

func descriptorsForHashes(
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
			return nil, fmt.Errorf("host requested unknown blob %s", hash)
		}
		descriptors = append(descriptors, protocol.BlobDescriptor{Hash: hash, Size: size})
	}

	return descriptors, nil
}

func downloadParallelism(need protocol.NeedBlobs, totalChunks int) int {
	parallel := need.Parallel
	if parallel <= 0 {
		parallel = transferParallelism(totalChunks)
	}
	if parallel > maxBlobTransferParallel {
		parallel = maxBlobTransferParallel
	}
	if parallel > totalChunks {
		parallel = totalChunks
	}
	if parallel < 1 {
		return 1
	}

	return parallel
}

func downloadBlobWorker(
	ctx context.Context,
	chunks []protocol.BlobChunkInfo,
	token string,
	chunkSize int64,
	opts ExecOptions,
	receiver *syncfs.ChunkedBlobReceiver,
	progress *blobChunkProgress,
) error {
	var conn *blobTransferConn
	defer closeBlobTransferConn(conn)

	var err error
	conn, err = downloadBlobChunksPipelined(ctx, conn, token, chunkSize, opts, receiver, progress, chunks)
	if err != nil {
		return err
	}

	if conn != nil {
		_ = conn.conn.WriteJSON(protocol.MsgSyncComplete, nil)
	}

	return nil
}

func downloadBlobChunksPipelined(
	ctx context.Context,
	conn *blobTransferConn,
	token string,
	chunkSize int64,
	opts ExecOptions,
	receiver *syncfs.ChunkedBlobReceiver,
	progress *blobChunkProgress,
	chunks []protocol.BlobChunkInfo,
) (*blobTransferConn, error) {
	pending := append([]protocol.BlobChunkInfo(nil), chunks...)
	inFlight := map[blobChunkKey]protocol.BlobChunkInfo{}
	inFlightOrder := make([]protocol.BlobChunkInfo, 0, downloadPipelineWindow)
	attempts := map[blobChunkKey]int{}

	for len(pending) > 0 || len(inFlight) > 0 {
		nextConn, err := fillDownloadPipeline(ctx, conn, token, opts, &pending, inFlight, &inFlightOrder)
		conn = nextConn
		if err != nil {
			failed := downloadInflight(inFlight, inFlightOrder)
			pending = append(failed, pending...)
			inFlight = map[blobChunkKey]protocol.BlobChunkInfo{}
			inFlightOrder = inFlightOrder[:0]
			if err := retryDownloadPipeline(ctx, conn, attempts, failed, blobProgressLogger(progress), err); err != nil {
				return nil, err
			}
			conn = nil
			continue
		}
		if len(inFlight) == 0 {
			continue
		}

		head, err := conn.conn.ReadHeader()
		if err != nil {
			failed := downloadInflight(inFlight, inFlightOrder)
			pending = append(failed, pending...)
			inFlight = map[blobChunkKey]protocol.BlobChunkInfo{}
			inFlightOrder = inFlightOrder[:0]
			if err := retryDownloadPipeline(ctx, conn, attempts, failed, blobProgressLogger(progress), err); err != nil {
				return nil, err
			}
			conn = nil
			continue
		}

		done, err := handleDownloadBlobTransferFrame(conn.conn, receiver, progress, inFlight, head)
		if err != nil {
			failed := downloadInflight(inFlight, inFlightOrder)
			pending = append(failed, pending...)
			inFlight = map[blobChunkKey]protocol.BlobChunkInfo{}
			inFlightOrder = inFlightOrder[:0]
			if err := retryDownloadPipeline(ctx, conn, attempts, failed, blobProgressLogger(progress), err); err != nil {
				return nil, err
			}
			conn = nil
			continue
		}
		if done {
			inFlight = map[blobChunkKey]protocol.BlobChunkInfo{}
			inFlightOrder = inFlightOrder[:0]
		}
	}

	return conn, nil
}

func fillDownloadPipeline(
	ctx context.Context,
	conn *blobTransferConn,
	token string,
	opts ExecOptions,
	pending *[]protocol.BlobChunkInfo,
	inFlight map[blobChunkKey]protocol.BlobChunkInfo,
	inFlightOrder *[]protocol.BlobChunkInfo,
) (*blobTransferConn, error) {
	if conn == nil {
		var err error
		conn, err = dialBlobTransferConn(ctx, opts)
		if err != nil {
			return nil, err
		}
	}

	for len(*pending) > 0 && len(inFlight) < downloadPipelineWindow {
		chunk := (*pending)[0]
		*pending = (*pending)[1:]
		key := keyBlobChunk(chunk)
		inFlight[key] = chunk
		*inFlightOrder = append(*inFlightOrder, chunk)
		if err := writeDownloadBlobRequest(conn.conn, token, opts, chunk); err != nil {
			return conn, err
		}
	}

	return conn, nil
}

func writeDownloadBlobRequest(
	conn *protocol.Conn,
	token string,
	opts ExecOptions,
	chunk protocol.BlobChunkInfo,
) error {
	req := protocol.BlobTransferRequest{
		ContextID: opts.ContextID,
		Session:   opts.Session,
		Token:     token,
		Direction: protocol.BlobTransferDownload,
		Chunk:     &chunk,
	}

	return conn.WriteJSON(protocol.MsgBlobTransferRequest, req)
}

func retryDownloadPipeline(
	ctx context.Context,
	conn *blobTransferConn,
	attempts map[blobChunkKey]int,
	failed []protocol.BlobChunkInfo,
	logger *runLogger,
	err error,
) error {
	closeBlobTransferConn(conn)
	if !isRetryableBlobTransferError(ctx, err) {
		return err
	}
	if len(failed) == 0 {
		return err
	}

	maxAttempt := 0
	for _, chunk := range failed {
		key := keyBlobChunk(chunk)
		attempts[key]++
		if attempts[key] > maxAttempt {
			maxAttempt = attempts[key]
		}
	}
	if maxAttempt >= blobTransferMaxAttempts {
		first := failed[0]
		return fmt.Errorf(
			"download blob chunk %s offset %d failed after %d attempts: %w",
			first.Hash,
			first.Offset,
			blobTransferMaxAttempts,
			err,
		)
	}
	if logger != nil {
		first := failed[0]
		logger.Printf(
			"retrying blob download pipeline: chunks=%d first_hash=%s first_offset=%d attempt=%d/%d error=%v",
			len(failed),
			first.Hash,
			first.Offset,
			maxAttempt+1,
			blobTransferMaxAttempts,
			err,
		)
	}
	if waitErr := waitBlobTransferRetry(ctx, maxAttempt); waitErr != nil {
		return waitErr
	}

	return nil
}

func downloadInflight(
	inFlight map[blobChunkKey]protocol.BlobChunkInfo,
	order []protocol.BlobChunkInfo,
) []protocol.BlobChunkInfo {
	chunks := make([]protocol.BlobChunkInfo, 0, len(inFlight))
	for _, chunk := range order {
		if _, ok := inFlight[keyBlobChunk(chunk)]; ok {
			chunks = append(chunks, chunk)
		}
	}

	return chunks
}

func handleDownloadBlobTransferFrame(
	conn *protocol.Conn,
	receiver *syncfs.ChunkedBlobReceiver,
	progress *blobChunkProgress,
	inFlight map[blobChunkKey]protocol.BlobChunkInfo,
	head protocol.Header,
) (bool, error) {
	switch head.Type {
	case protocol.MsgHeartbeat:
		return false, conn.DiscardPayload(head)
	case protocol.MsgError:
		return false, decodeServerError(head)
	case protocol.MsgBlobChunk:
		return receiveDownloadBlobChunk(conn, receiver, progress, inFlight, head)
	case protocol.MsgSyncComplete:
		return true, nil
	default:
		if err := conn.DiscardPayload(head); err != nil {
			return false, err
		}

		return false, fmt.Errorf("unexpected blob transfer frame: %s", head.Type)
	}
}

func receiveDownloadBlobChunk(
	conn *protocol.Conn,
	receiver *syncfs.ChunkedBlobReceiver,
	progress *blobChunkProgress,
	inFlight map[blobChunkKey]protocol.BlobChunkInfo,
	head protocol.Header,
) (bool, error) {
	info, err := protocol.DecodeData[protocol.BlobChunkInfo](head)
	if err != nil {
		return false, err
	}
	key := keyBlobChunk(info)
	if _, ok := inFlight[key]; !ok {
		if err := conn.DiscardPayload(head); err != nil {
			return false, err
		}

		return false, fmt.Errorf("unexpected blob chunk %s offset %d", info.Hash, info.Offset)
	}
	if err := receiver.ReceiveChunk(info, conn.PayloadReader(head), head.PayloadLen); err != nil {
		return false, err
	}
	delete(inFlight, key)
	progress.AddBytes(int(head.PayloadLen))
	progress.CompleteChunk(info.Hash)

	return false, nil
}

func materializeDownloadedChanges(
	ctx context.Context,
	root string,
	store *syncfs.BlobStore,
	entries []syncfs.Entry,
	progress *transferProgress,
) error {
	files := make([]syncfs.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Kind == syncfs.KindFile {
			files = append(files, entry)
		}
	}
	if len(files) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := transferParallelism(len(files))
	jobs := make(chan syncfs.Entry)
	errCh := make(chan error, 1)

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
					if err := materializeDownloadedFile(root, store, entry, progress); err != nil {
						select {
						case errCh <- err:
							cancel()
						default:
						}
						return
					}
					progress.CompleteFile()
				}
			}
		}()
	}

sendJobs:
	for _, entry := range files {
		select {
		case <-ctx.Done():
			break sendJobs
		case jobs <- entry:
		}
	}
	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}
	return ctx.Err()
}

func materializeDownloadedFile(
	root string,
	store *syncfs.BlobStore,
	entry syncfs.Entry,
	progress *transferProgress,
) error {
	target, err := pathutil.SecureJoin(root, filepath.FromSlash(entry.Path))
	if err != nil {
		return err
	}

	mode := os.FileMode(entry.Mode)
	if mode == 0 {
		mode = 0o644
	}

	return store.MaterializeWithProgress(
		entry.Hash,
		target,
		mode,
		entry.ModTime,
		func(n int) {
			progress.AddBytes(n)
		},
	)
}

func updateManifestCacheAfterSync(
	root string,
	contextID string,
	changes protocol.ChangeSet,
	logger *runLogger,
) {
	previous, err := loadCachedManifest(root, contextID)
	if err != nil {
		logger.Printf("local manifest cache sync-back update skipped: %v", err)
		return
	}

	updated := applyManifestChanges(previous, changes)
	if err := saveCachedManifest(root, contextID, updated); err != nil {
		logger.Printf("local manifest cache sync-back update failed: %v", err)
	}
}

func applyManifestChanges(previous []syncfs.Entry, changes protocol.ChangeSet) []syncfs.Entry {
	byPath := make(map[string]syncfs.Entry, len(previous)+len(changes.Entries))
	for _, entry := range previous {
		byPath[entry.Path] = entry
	}
	for _, path := range changes.Deleted {
		delete(byPath, path)
	}
	for _, entry := range changes.Entries {
		byPath[entry.Path] = entry
	}

	updated := make([]syncfs.Entry, 0, len(byPath))
	for _, entry := range byPath {
		updated = append(updated, entry)
	}
	sort.Slice(updated, func(i, j int) bool {
		return updated[i].Path < updated[j].Path
	})

	return updated
}

type PairOptions struct {
	Address             string
	DiscoveryService    string
	Host                clientstate.HostRecord
	Code                string
	ClientLabel         string
	PreviousFingerprint string
	CSRPEM              []byte
	Stderr              io.Writer
}

type PairResult = protocol.PairResponse
type PairCodeResult = protocol.PairCodeResponse

func RequestPairCode(ctx context.Context, opts PairOptions) (PairCodeResult, error) {
	conn, err := dialTLS(ctx, opts.Address, opts.DiscoveryService, opts.Host.Fingerprint, nil, nil)
	if err != nil {
		return PairCodeResult{}, err
	}
	defer closeQuietly(conn.Raw())

	req := protocol.PairCodeRequest{ClientLabel: opts.ClientLabel}
	if err := conn.WriteJSON(protocol.MsgPairCodeRequest, req); err != nil {
		return PairCodeResult{}, err
	}

	return expectDataFrameWithOutput[protocol.PairCodeResponse](
		conn,
		protocol.MsgPairCodeResponse,
		opts.Stderr,
	)
}

func PairHost(ctx context.Context, opts PairOptions) (PairResult, error) {
	conn, err := dialTLS(ctx, opts.Address, opts.DiscoveryService, opts.Host.Fingerprint, nil, nil)
	if err != nil {
		return PairResult{}, err
	}
	defer closeQuietly(conn.Raw())

	req := protocol.PairRequest{
		Code:                opts.Code,
		ClientLabel:         opts.ClientLabel,
		PreviousFingerprint: opts.PreviousFingerprint,
		CSRPEM:              string(opts.CSRPEM),
	}
	if err := conn.WriteJSON(protocol.MsgPairRequest, req); err != nil {
		return PairResult{}, err
	}

	return expectDataFrameWithOutput[protocol.PairResponse](
		conn,
		protocol.MsgPairResponse,
		opts.Stderr,
	)
}

func GenerateClientIdentity(label string) ([]byte, []byte, []byte, error) {
	return security.GenerateClientIdentity(label)
}

func GenerateCSR(keyPEM []byte, label string) ([]byte, error) {
	return security.GenerateCSRFromKey(keyPEM, label)
}

type blobUploadItem struct {
	hash        string
	path        string
	displayPath string
	size        int64
}

type transferProgress struct {
	label          string
	logger         *runLogger
	totalFiles     int
	totalBytes     int64
	completedFiles int
	bytes          int64
	start          time.Time
	mu             sync.Mutex
}

func newTransferProgress(
	label string,
	logger *runLogger,
	totalFiles int,
	totalBytes int64,
) *transferProgress {
	return &transferProgress{
		label:      label,
		logger:     logger,
		totalFiles: totalFiles,
		totalBytes: totalBytes,
		start:      time.Now(),
	}
}

func (p *transferProgress) AddBytes(n int) {
	if p == nil || n <= 0 {
		return
	}

	p.mu.Lock()
	p.bytes += int64(n)
	filesDone := p.completedFiles
	bytesDone := p.bytes
	p.mu.Unlock()

	p.logger.Every(
		p.label,
		"%s progress: files=%d/%d bytes=%s/%s speed=%s/s",
		p.label,
		filesDone,
		p.totalFiles,
		formatBytes(bytesDone),
		formatBytes(p.totalBytes),
		formatBytes(p.rate(bytesDone)),
	)
}

func (p *transferProgress) CompleteFile() {
	if p == nil {
		return
	}

	p.mu.Lock()
	p.completedFiles++
	filesDone := p.completedFiles
	bytesDone := p.bytes
	p.mu.Unlock()

	p.logger.Every(
		p.label,
		"%s progress: files=%d/%d bytes=%s/%s speed=%s/s",
		p.label,
		filesDone,
		p.totalFiles,
		formatBytes(bytesDone),
		formatBytes(p.totalBytes),
		formatBytes(p.rate(bytesDone)),
	)
}

func (p *transferProgress) Done() {
	if p == nil {
		return
	}

	p.mu.Lock()
	filesDone := p.completedFiles
	bytesDone := p.bytes
	p.mu.Unlock()

	p.logger.Printf(
		"%s done: files=%d/%d bytes=%s/%s avg_speed=%s/s",
		p.label,
		filesDone,
		p.totalFiles,
		formatBytes(bytesDone),
		formatBytes(p.totalBytes),
		formatBytes(p.rate(bytesDone)),
	)
}

func (p *transferProgress) rate(bytesDone int64) int64 {
	elapsed := time.Since(p.start).Seconds()
	if elapsed <= 0 {
		return 0
	}

	return int64(float64(bytesDone) / elapsed)
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}

	value := float64(n)
	units := []string{"KiB", "MiB", "GiB", "TiB"}

	for _, suffix := range units {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.1f %s", value, suffix)
		}
	}

	return fmt.Sprintf("%.1f PiB", value/unit)
}

func transferParallelism(totalChunks int) int {
	if totalChunks <= 1 {
		return 1
	}

	parallel := runtime.NumCPU()
	if parallel > 8 {
		parallel = 8
	}
	if parallel > maxBlobTransferParallel {
		parallel = maxBlobTransferParallel
	}
	if parallel > totalChunks {
		parallel = totalChunks
	}
	if parallel < 1 {
		return 1
	}

	return parallel
}

func sendMissingBlobs(
	ctx context.Context,
	conn *protocol.Conn,
	need protocol.NeedBlobs,
	blobSources map[string]string,
	opts ExecOptions,
	logger *runLogger,
) error {
	ordered, totalBytes, err := prepareUploadItems(need.Hashes, blobSources, opts.Root)
	if err != nil {
		return err
	}

	if len(ordered) == 0 {
		logger.Printf("host already has all file blobs")

		return nil
	}

	if need.TransferToken == "" {
		return errors.New("host did not provide blob transfer token")
	}

	chunkSize := need.ChunkSize
	if chunkSize <= 0 {
		chunkSize = protocol.DefaultBlobChunkSize
	}
	descriptors := uploadBlobDescriptors(ordered)
	chunks := protocol.PlanBlobChunks(descriptors, chunkSize)
	parallel := uploadParallelism(need, len(chunks))

	progress := newTransferProgress("upload", logger, len(ordered), totalBytes)
	logger.Printf(
		"uploading missing file blobs: files=%d chunks=%d bytes=%s parallel=%d",
		len(ordered),
		len(chunks),
		formatBytes(totalBytes),
		parallel,
	)

	if err := sendMissingBlobsParallel(
		ctx,
		ordered,
		chunks,
		parallel,
		need.TransferToken,
		chunkSize,
		opts,
		progress,
		logger,
	); err != nil {
		return err
	}

	progress.Done()

	return nil
}

func prepareUploadItems(
	hashes []string,
	blobSources map[string]string,
	root string,
) ([]blobUploadItem, int64, error) {
	ordered := append([]string(nil), hashes...)
	sort.Strings(ordered)

	items := make([]blobUploadItem, 0, len(ordered))

	var totalBytes int64

	for _, hash := range ordered {
		srcPath, ok := blobSources[hash]
		if !ok {
			return nil, 0, fmt.Errorf("host requested unknown blob %s", hash)
		}

		info, err := os.Stat(srcPath)
		if err != nil {
			return nil, 0, fmt.Errorf("stat blob source %s: %w", srcPath, err)
		}

		items = append(items, blobUploadItem{
			hash:        hash,
			path:        srcPath,
			displayPath: uploadDisplayPath(root, srcPath),
			size:        info.Size(),
		})
		totalBytes += info.Size()
	}

	return items, totalBytes, nil
}

func uploadDisplayPath(root, srcPath string) string {
	if strings.TrimSpace(root) != "" {
		rel, err := filepath.Rel(root, srcPath)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return filepath.ToSlash(filepath.Clean(rel))
		}
	}

	return filepath.ToSlash(srcPath)
}

func uploadBlobDescriptors(items []blobUploadItem) []protocol.BlobDescriptor {
	descriptors := make([]protocol.BlobDescriptor, 0, len(items))
	for _, item := range items {
		descriptors = append(descriptors, protocol.BlobDescriptor{Hash: item.hash, Size: item.size})
	}

	return descriptors
}

func uploadParallelism(need protocol.NeedBlobs, totalChunks int) int {
	parallel := need.Parallel
	if parallel <= 0 {
		parallel = transferParallelism(totalChunks)
	}

	if parallel > maxBlobTransferParallel {
		parallel = maxBlobTransferParallel
	}

	if parallel > totalChunks {
		parallel = totalChunks
	}

	if parallel < 1 {
		return 1
	}

	return parallel
}

func sendMissingBlobsParallel(
	ctx context.Context,
	items []blobUploadItem,
	chunks []protocol.BlobChunkInfo,
	parallel int,
	token string,
	chunkSize int64,
	opts ExecOptions,
	progress *transferProgress,
	logger *runLogger,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan blobUploadChunk)
	errCh := make(chan error, 1)
	chunkProgress := newBlobChunkProgress(progress, chunks)
	itemsByHash := uploadItemsByHash(items)

	var wg sync.WaitGroup
	for range parallel {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := uploadBlobWorker(
				ctx,
				jobs,
				token,
				chunkSize,
				opts,
				chunkProgress,
				logger,
			); err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
			}
		}()
	}

	go sendUploadJobs(ctx, chunks, itemsByHash, jobs)

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type blobUploadChunk struct {
	item blobUploadItem
	info protocol.BlobChunkInfo
}

func uploadItemsByHash(items []blobUploadItem) map[string]blobUploadItem {
	byHash := make(map[string]blobUploadItem, len(items))
	for _, item := range items {
		byHash[item.hash] = item
	}

	return byHash
}

func sendUploadJobs(
	ctx context.Context,
	chunks []protocol.BlobChunkInfo,
	items map[string]blobUploadItem,
	jobs chan<- blobUploadChunk,
) {
	defer close(jobs)

	for _, chunk := range chunks {
		item := items[chunk.Hash]
		select {
		case <-ctx.Done():
			return
		case jobs <- blobUploadChunk{item: item, info: chunk}:
		}
	}
}

func uploadBlobWorker(
	ctx context.Context,
	jobs <-chan blobUploadChunk,
	token string,
	chunkSize int64,
	opts ExecOptions,
	progress *blobChunkProgress,
	logger *runLogger,
) error {
	var conn *blobTransferConn
	defer closeBlobTransferConn(conn)

	for job := range jobs {
		if err := ctx.Err(); err != nil {
			return err
		}

		var err error
		conn, err = retryBlobTransferChunk(
			ctx,
			conn,
			"upload",
			job.info.Hash,
			job.info.Offset,
			logger,
			func(conn *blobTransferConn) (*blobTransferConn, error) {
				return uploadBlobChunk(ctx, conn, token, opts, job, chunkSize, progress)
			},
		)
		if err != nil {
			return err
		}
		progress.CompleteChunk(job.info.Hash)
	}

	if conn != nil {
		_ = conn.conn.WriteJSON(protocol.MsgSyncComplete, nil)
	}

	return nil
}

func uploadBlobChunk(
	ctx context.Context,
	conn *blobTransferConn,
	token string,
	opts ExecOptions,
	job blobUploadChunk,
	chunkSize int64,
	progress *blobChunkProgress,
) (*blobTransferConn, error) {
	if conn == nil {
		var err error
		conn, err = dialBlobTransferConn(ctx, opts)
		if err != nil {
			return nil, err
		}

		req := protocol.BlobTransferRequest{
			ContextID: opts.ContextID,
			Session:   opts.Session,
			Token:     token,
			Direction: protocol.BlobTransferUpload,
		}
		if err := conn.conn.WriteJSON(protocol.MsgBlobTransferRequest, req); err != nil {
			return conn, err
		}
	}

	if err := sendUploadChunk(conn.conn, job, chunkSize, progress); err != nil {
		return conn, err
	}

	return conn, nil
}

func sendUploadChunk(
	conn *protocol.Conn,
	job blobUploadChunk,
	chunkSize int64,
	progress *blobChunkProgress,
) error {
	payloadLen := protocol.BlobChunkPayloadLen(job.info, chunkSize)
	f, err := os.Open(job.item.path)
	if err != nil {
		return fmt.Errorf("open blob source %s: %w", job.item.path, err)
	}

	defer closeQuietly(f)

	if err := conn.WriteFrom(
		protocol.MsgBlobChunk,
		job.info,
		io.NewSectionReader(f, job.info.Offset, payloadLen),
		payloadLen,
	); err != nil {
		return err
	}

	progress.AddBytes(int(payloadLen))

	return nil
}

type blobChunkProgress struct {
	progress  *transferProgress
	remaining map[string]int
	mu        sync.Mutex
}

func newBlobChunkProgress(
	progress *transferProgress,
	chunks []protocol.BlobChunkInfo,
) *blobChunkProgress {
	remaining := map[string]int{}
	for _, chunk := range chunks {
		remaining[chunk.Hash]++
	}

	return &blobChunkProgress{progress: progress, remaining: remaining}
}

func blobProgressLogger(progress *blobChunkProgress) *runLogger {
	if progress == nil || progress.progress == nil {
		return nil
	}

	return progress.progress.logger
}

func (p *blobChunkProgress) AddBytes(n int) {
	if p != nil && p.progress != nil {
		p.progress.AddBytes(n)
	}
}

func (p *blobChunkProgress) CompleteChunk(hash string) {
	if p == nil || p.progress == nil {
		return
	}

	p.mu.Lock()
	remaining := p.remaining[hash] - 1
	p.remaining[hash] = remaining
	p.mu.Unlock()

	if remaining == 0 {
		p.progress.CompleteFile()
	}
}

func formatMounts(mounts []syncfs.MountSpec) string {
	if len(mounts) == 0 {
		return "."
	}

	parts := make([]string, 0, len(mounts))
	for _, mount := range mounts {
		path := strings.TrimSpace(mount.Path)
		if path == "" {
			path = "."
		}

		if len(mount.Exclude) > 0 {
			path += fmt.Sprintf("(ignore=%d)", len(mount.Exclude))
		}

		parts = append(parts, path)
	}

	return strings.Join(parts, ",")
}

func sendStdin(conn *protocol.Conn, src io.Reader, enabled bool) error {
	if !enabled || src == nil {
		return conn.WriteJSON(protocol.MsgStdinClose, nil)
	}

	buf := make([]byte, stdinBufferSize)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if writeErr := conn.WriteBytes(protocol.MsgStdinData, nil, buf[:n]); writeErr != nil {
				return writeErr
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return conn.WriteJSON(protocol.MsgStdinClose, nil)
			}

			return err
		}
	}
}

func collectEnv(names []string, extra map[string]string) map[string]string {
	out := make(map[string]string, len(names)+len(extra))
	for _, name := range names {
		if value, ok := os.LookupEnv(name); ok {
			out[name] = value
		}
	}

	maps.Copy(out, extra)

	return out
}

func decodeServerError(head protocol.Header) error {
	msg, err := protocol.DecodeData[protocol.ErrorMessage](head)
	if err != nil {
		return err
	}

	return errors.New(msg.Message)
}
