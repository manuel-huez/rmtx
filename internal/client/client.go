package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/manuel-huez/rmtx/internal/clientstate"

	"github.com/manuel-huez/rmtx/internal/discovery"
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
	directDialTimeout   = 1500 * time.Millisecond
	reverseDialTimeout  = 5 * time.Second
	defaultDiscoverySvc = "rmtx"
	progressEvery       = 3 * time.Second
	parallelBlobUploads = 4
)

type runLogger struct {
	out      io.Writer
	mu       sync.Mutex
	lastByID map[string]time.Time
}

func newRunLogger(out io.Writer) *runLogger {
	if out == nil {
		out = io.Discard
	}

	return &runLogger{out: out, lastByID: map[string]time.Time{}}
}

func (l *runLogger) Printf(format string, args ...any) {
	if l == nil || l.out == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	_, _ = fmt.Fprintf(l.out, "rmtx: "+format+"\n", args...)
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

func Run(ctx context.Context, opts ExecOptions) (int, error) {
	root, workdir, err := resolvePaths(&opts)
	if err != nil {
		return 1, err
	}

	logger := newRunLogger(opts.Stderr)
	logger.Printf(
		"preparing remote run: host=%s context=%s workdir=%s command=%q",
		opts.Address,
		opts.ContextID,
		workdir,
		strings.Join(opts.Command, " "),
	)

	manifest, request, err := buildRunRequest(ctx, root, workdir, &opts, logger)
	if err != nil {
		return 1, err
	}

	logger.Printf("connecting to host: %s", opts.Address)

	conn, err := dialTLS(
		ctx,
		opts.Address,
		opts.DiscoveryService,
		opts.Host.Fingerprint,
		opts.ClientCertPEM,
		opts.ClientKeyPEM,
	)
	if err != nil {
		return 1, err
	}
	defer closeQuietly(conn.Raw())

	stopContextClose := context.AfterFunc(ctx, func() { closeQuietly(conn.Raw()) })
	defer stopContextClose()

	ready, err := runHandshake(ctx, conn, request, manifest.BlobSources, opts, logger)
	if err != nil {
		return 1, err
	}

	logger.Printf(
		"remote workspace ready: context=%s workspace=%s",
		ready.ContextID,
		ready.Workspace,
	)
	logger.Printf("running remote command now")

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
	logger.Printf("synchronizing local files: root=%s mounts=%s", root, formatMounts(opts.Mounts))

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

func dialTLS(
	ctx context.Context,
	address, discoveryService, fingerprint string,
	clientCertPEM, clientKeyPEM []byte,
) (*protocol.Conn, error) {
	dialer := net.Dialer{Timeout: directDialTimeout}

	tlsConfig, err := security.ClientTLSConfig(clientCertPEM, clientKeyPEM, fingerprint)
	if err != nil {
		return nil, err
	}

	raw, err := tls.DialWithDialer(&dialer, "tcp", address, tlsConfig)
	if err != nil {
		reverse, reverseErr := dialReverseTLS(
			ctx,
			address,
			discoveryService,
			fingerprint,
			clientCertPEM,
			clientKeyPEM,
		)
		if reverseErr == nil {
			return reverse, nil
		}

		return nil, fmt.Errorf(
			"dial host %s: %w; reverse connect failed: %w",
			address,
			err,
			reverseErr,
		)
	}

	return protocol.NewConn(raw), nil
}

func dialReverseTLS(
	ctx context.Context,
	address, discoveryService, fingerprint string,
	clientCertPEM, clientKeyPEM []byte,
) (*protocol.Conn, error) {
	ln, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		return nil, fmt.Errorf("listen for reverse connection: %w", err)
	}
	defer closeQuietly(ln)

	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok || tcpAddr.Port == 0 {
		return nil, fmt.Errorf("listen for reverse connection: unexpected address %s", ln.Addr())
	}

	ctx, cancel := context.WithTimeout(ctx, reverseDialTimeout)
	defer cancel()

	requestCtx, requestCancel := context.WithCancel(ctx)
	defer requestCancel()

	requestErrCh := startReverseConnectRequests(
		requestCtx,
		nonEmpty(discoveryService, defaultDiscoverySvc),
		address,
		tcpAddr.Port,
		fingerprint,
	)

	raw, err := acceptReverse(ctx, ln, requestErrCh)
	if err != nil {
		return nil, err
	}

	requestCancel()

	tlsConfig, err := security.ClientTLSConfig(clientCertPEM, clientKeyPEM, fingerprint)
	if err != nil {
		closeQuietly(raw)

		return nil, err
	}

	conn := tls.Client(raw, tlsConfig)
	if err := conn.HandshakeContext(ctx); err != nil {
		closeQuietly(conn)

		return nil, fmt.Errorf("reverse tls handshake: %w", err)
	}

	return protocol.NewConn(conn), nil
}

func startReverseConnectRequests(
	ctx context.Context,
	discoveryService string,
	address string,
	callbackPort int,
	fingerprint string,
) <-chan error {
	errCh := make(chan error, 1)

	go func() {
		errCh <- discovery.RequestReverseConnect(
			ctx,
			discoveryService,
			address,
			callbackPort,
			fingerprint,
		)
	}()

	return errCh
}

func nonEmpty(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}

	return value
}

func acceptReverse(
	ctx context.Context,
	ln net.Listener,
	requestErrCh <-chan error,
) (net.Conn, error) {
	type acceptResult struct {
		conn net.Conn
		err  error
	}

	resultCh := make(chan acceptResult, 1)

	go func() {
		conn, err := ln.Accept()
		resultCh <- acceptResult{conn: conn, err: err}
	}()

	for {
		select {
		case <-ctx.Done():
			_ = ln.Close()

			return nil, fmt.Errorf("accept reverse connection: %w", ctx.Err())
		case err := <-requestErrCh:
			requestErrCh = nil

			if err != nil {
				_ = ln.Close()

				return nil, err
			}
		case result := <-resultCh:
			if result.err != nil {
				return nil, fmt.Errorf("accept reverse connection: %w", result.err)
			}

			return result.conn, nil
		}
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

	need, err := expectDataFrameWithOutput[protocol.NeedBlobs](
		conn,
		protocol.MsgNeedBlobs,
		opts.Stderr,
	)
	if err != nil {
		return protocol.WorkspaceReady{}, err
	}

	if err := sendMissingBlobs(ctx, conn, need, blobSources, opts, logger); err != nil {
		return protocol.WorkspaceReady{}, err
	}

	logger.Printf("local-to-host sync complete")

	if err := conn.WriteJSON(protocol.MsgSyncComplete, nil); err != nil {
		return protocol.WorkspaceReady{}, err
	}

	return expectDataFrameWithOutput[protocol.WorkspaceReady](
		conn,
		protocol.MsgWorkspaceReady,
		opts.Stderr,
	)
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
			if err := <-stdinErrCh; err != nil {
				return exitCode, err
			}

			return exitCode, nil
		}
	}
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
	conn *protocol.Conn,
	head protocol.Header,
	root string,
	opts ExecOptions,
	exitCode int,
	logger *runLogger,
	downloadProgress **transferProgress,
) (bool, int, error) {
	switch head.Type {
	case protocol.MsgError:
		return false, exitCode, decodeServerError(head)
	case protocol.MsgExecOutput:
		return false, exitCode, copyExecOutput(conn, head, opts)
	case protocol.MsgExecExit:
		code, err := decodeExitCode(head)
		if err == nil {
			logger.Printf("command exited: exit_code=%d; syncing host changes back", code)
		}

		return false, code, err
	case protocol.MsgSyncCompressionStart:
		return false, exitCode, enableSyncCompression(conn, head)
	case protocol.MsgChangeSet:
		progress, err := applyChangeSet(conn, head, root, logger)
		*downloadProgress = progress

		return false, exitCode, err
	case protocol.MsgChangeBlob:
		return false, exitCode, applyChangeBlob(conn, head, root, logger, *downloadProgress)
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

func enableSyncCompression(conn *protocol.Conn, head protocol.Header) error {
	info, err := protocol.DecodeData[protocol.CompressionInfo](head)
	if err != nil {
		return err
	}

	switch info.Algorithm {
	case protocol.CompressionZstd:
		_, err := conn.EnableZstdReader()
		return err
	default:
		return fmt.Errorf("unsupported sync compression: %s", info.Algorithm)
	}
}

func applyChangeSet(
	conn *protocol.Conn,
	head protocol.Header,
	root string,
	logger *runLogger,
) (*transferProgress, error) {
	changes, err := protocol.DecodeData[protocol.ChangeSet](head)
	if err != nil {
		return nil, err
	}

	files, bytes := changeSetFileTotals(changes.Entries)
	logger.Printf(
		"applying host changes locally: changed=%d deleted=%d file_bytes=%s",
		len(changes.Entries),
		len(changes.Deleted),
		formatBytes(bytes),
	)

	if err := syncfs.DeletePaths(root, changes.Deleted); err != nil {
		return nil, err
	}

	if err := syncfs.ApplyNonFileEntries(root, syncfs.NonFileEntries(changes.Entries)); err != nil {
		return nil, err
	}

	return newTransferProgress("download", logger, files, bytes), nil
}

func applyChangeBlob(
	conn *protocol.Conn,
	head protocol.Header,
	root string,
	logger *runLogger,
	progress *transferProgress,
) error {
	info, err := protocol.DecodeData[protocol.BlobInfo](head)
	if err != nil {
		return err
	}

	entry := syncfs.Entry{
		Path:    info.Path,
		Kind:    syncfs.KindFile,
		Hash:    info.Hash,
		Size:    head.PayloadLen,
		Mode:    info.Mode,
		ModTime: info.ModTime,
	}

	var received int64

	reader := &progressReader{
		src: conn.PayloadReader(head),
		onRead: func(n int) {
			received += int64(n)
			if progress != nil {
				progress.AddBytes(n)
			}

			logger.Every(
				"download-file",
				"download file: current_file=%s bytes=%s/%s",
				info.Path,
				formatBytes(received),
				formatBytes(head.PayloadLen),
			)
		},
	}

	if err := syncfs.WriteFile(root, entry, reader); err != nil {
		return err
	}

	if progress != nil {
		progress.CompleteFile()
	}

	return nil
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
	hash string
	path string
	size int64
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

func sendMissingBlobs(
	ctx context.Context,
	conn *protocol.Conn,
	need protocol.NeedBlobs,
	blobSources map[string]string,
	opts ExecOptions,
	logger *runLogger,
) error {
	ordered, totalBytes, err := prepareUploadItems(need.Hashes, blobSources)
	if err != nil {
		return err
	}

	if len(ordered) == 0 {
		logger.Printf("host already has all file blobs")

		return nil
	}

	parallel := uploadParallelism(need, len(ordered))
	compression := chooseBlobUploadCompression(ordered)
	progress := newTransferProgress("upload", logger, len(ordered), totalBytes)
	logger.Printf(
		"uploading missing file blobs: files=%d bytes=%s parallel=%d compression=%s",
		len(ordered),
		formatBytes(totalBytes),
		parallel,
		compression,
	)

	if need.UploadToken != "" && (parallel > 1 || compression != "") {
		if err := sendMissingBlobsParallel(
			ctx,
			ordered,
			parallel,
			need.UploadToken,
			compression,
			opts,
			progress,
		); err != nil {
			return err
		}

		progress.Done()

		return nil
	}

	if err := sendMissingBlobsSequential(conn, ordered, progress); err != nil {
		return err
	}

	progress.Done()

	return nil
}

func prepareUploadItems(
	hashes []string,
	blobSources map[string]string,
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

		items = append(items, blobUploadItem{hash: hash, path: srcPath, size: info.Size()})
		totalBytes += info.Size()
	}

	return items, totalBytes, nil
}

func chooseBlobUploadCompression(items []blobUploadItem) string {
	candidates := make([]syncfs.CompressionCandidate, 0, len(items))
	for _, item := range items {
		candidates = append(candidates, syncfs.CompressionCandidate{
			Path: item.path,
			Size: item.size,
		})
	}

	if syncfs.ShouldCompressTransfer(candidates) {
		return protocol.CompressionZstd
	}

	return ""
}

func uploadParallelism(need protocol.NeedBlobs, total int) int {
	parallel := need.Parallel
	if parallel <= 0 {
		parallel = parallelBlobUploads
	}

	if parallel > parallelBlobUploads {
		parallel = parallelBlobUploads
	}

	if parallel > total {
		parallel = total
	}

	if parallel < 1 {
		return 1
	}

	return parallel
}

func sendMissingBlobsSequential(
	conn *protocol.Conn,
	items []blobUploadItem,
	progress *transferProgress,
) error {
	for _, item := range items {
		if err := sendBlob(conn, item, progress); err != nil {
			return err
		}

		progress.CompleteFile()
	}

	return nil
}

func sendMissingBlobsParallel(
	ctx context.Context,
	items []blobUploadItem,
	parallel int,
	token string,
	compression string,
	opts ExecOptions,
	progress *transferProgress,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan blobUploadItem)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	for range parallel {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := uploadBlobWorker(ctx, jobs, token, compression, opts, progress); err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
			}
		}()
	}

	go sendUploadJobs(ctx, items, jobs)

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

func sendUploadJobs(ctx context.Context, items []blobUploadItem, jobs chan<- blobUploadItem) {
	defer close(jobs)

	for _, item := range items {
		select {
		case <-ctx.Done():
			return
		case jobs <- item:
		}
	}
}

func uploadBlobWorker(
	ctx context.Context,
	jobs <-chan blobUploadItem,
	token string,
	compression string,
	opts ExecOptions,
	progress *transferProgress,
) error {
	conn, err := dialTLS(
		ctx,
		opts.Address,
		opts.DiscoveryService,
		opts.Host.Fingerprint,
		opts.ClientCertPEM,
		opts.ClientKeyPEM,
	)
	if err != nil {
		return err
	}

	defer closeQuietly(conn.Raw())

	req := protocol.BlobUploadRequest{
		ContextID:   opts.ContextID,
		Session:     opts.Session,
		Token:       token,
		Compression: compression,
	}
	if err := conn.WriteJSON(protocol.MsgBlobUploadRequest, req); err != nil {
		return err
	}

	var closeCompressed func() error
	if compression == protocol.CompressionZstd {
		closeCompressed, err = conn.EnableZstdWriter()
		if err != nil {
			return err
		}

		defer func() { _ = closeCompressed() }()
	}

	for item := range jobs {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := sendBlob(conn, item, progress); err != nil {
			return err
		}

		progress.CompleteFile()
	}

	if err := conn.WriteJSON(protocol.MsgSyncComplete, nil); err != nil {
		return err
	}

	if closeCompressed != nil {
		return closeCompressed()
	}

	return nil
}

func sendBlob(
	conn *protocol.Conn,
	item blobUploadItem,
	progress *transferProgress,
) error {
	f, err := os.Open(item.path)
	if err != nil {
		return fmt.Errorf("open blob source %s: %w", item.path, err)
	}

	defer closeQuietly(f)

	reader := &progressReader{
		src:    f,
		onRead: progress.AddBytes,
	}

	return conn.WriteFrom(
		protocol.MsgBlob,
		protocol.BlobInfo{Hash: item.hash, Size: item.size},
		reader,
		item.size,
	)
}

type progressReader struct {
	src    io.Reader
	onRead func(int)
}

func (r *progressReader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 && r.onRead != nil {
		r.onRead(n)
	}

	return n, err
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
