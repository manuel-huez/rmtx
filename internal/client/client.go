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
}

type PingInfo = protocol.PingResponse
type ContextInfo = protocol.ContextSummary
type DeleteContextsResult = protocol.DeleteContextsResponse

type DeleteContextsOptions struct {
	Remote    RemoteOptions
	IDs       []string
	All       bool
	OlderThan string
}

const stdinBufferSize = 32 * 1024
const (
	directDialTimeout   = 1500 * time.Millisecond
	reverseDialTimeout  = 5 * time.Second
	defaultDiscoverySvc = "rmtx"
	progressEvery       = 3 * time.Second
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

	ready, err := runHandshake(conn, request, manifest.BlobSources, logger)
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

	manifest, err := syncfs.BuildManifestContextOptions(
		ctx,
		root,
		opts.Mounts,
		syncfs.BuildOptions{Progress: logger.ReportBuildProgress},
	)
	if err != nil {
		return syncfs.BuildResult{}, protocol.RunRequest{}, err
	}

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
		Mounts:      append([]syncfs.MountSpec(nil), opts.Mounts...),
		Manifest:    manifest.Entries,
		Session:     opts.Session,
		Project:     opts.Project,
		RootHint:    filepath.Base(root),
		TTY:         opts.TTY,
		TTYRows:     rows,
		TTYCols:     cols,
	}

	return manifest, request, nil
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

	if err := discovery.RequestReverseConnect(
		ctx,
		nonEmpty(discoveryService, defaultDiscoverySvc),
		address,
		tcpAddr.Port,
		fingerprint,
	); err != nil {
		return nil, err
	}

	raw, err := acceptReverse(ctx, ln)
	if err != nil {
		return nil, err
	}

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

func nonEmpty(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}

	return value
}

func acceptReverse(ctx context.Context, ln net.Listener) (net.Conn, error) {
	type acceptResult struct {
		conn net.Conn
		err  error
	}

	resultCh := make(chan acceptResult, 1)

	go func() {
		conn, err := ln.Accept()
		resultCh <- acceptResult{conn: conn, err: err}
	}()

	select {
	case <-ctx.Done():
		_ = ln.Close()

		return nil, fmt.Errorf("accept reverse connection: %w", ctx.Err())
	case result := <-resultCh:
		if result.err != nil {
			return nil, fmt.Errorf("accept reverse connection: %w", result.err)
		}

		return result.conn, nil
	}
}

func runHandshake(
	conn *protocol.Conn,
	request protocol.RunRequest,
	blobSources map[string]string,
	logger *runLogger,
) (protocol.WorkspaceReady, error) {
	logger.Printf("sending run request and manifest: entries=%d", len(request.Manifest))

	if err := conn.WriteJSON(protocol.MsgRunRequest, request); err != nil {
		return protocol.WorkspaceReady{}, err
	}

	need, err := expectDataFrame[protocol.NeedBlobs](conn, protocol.MsgNeedBlobs)
	if err != nil {
		return protocol.WorkspaceReady{}, err
	}

	if err := sendMissingBlobs(conn, need.Hashes, blobSources, logger); err != nil {
		return protocol.WorkspaceReady{}, err
	}

	logger.Printf("local-to-host sync complete")

	if err := conn.WriteJSON(protocol.MsgSyncComplete, nil); err != nil {
		return protocol.WorkspaceReady{}, err
	}

	return expectDataFrame[protocol.WorkspaceReady](conn, protocol.MsgWorkspaceReady)
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

	exitCode := 0

	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return exitCode, err
		}

		done, updatedExitCode, err := handleExecFrame(conn, head, root, opts, exitCode, logger)
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

	exitCode := 0

	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return exitCode, err
		}

		if head.Type == protocol.MsgExecExit && session != nil {
			session.Close()
			session = nil
		}

		done, updatedExitCode, err := handleExecFrame(conn, head, root, opts, exitCode, logger)
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
	case protocol.MsgChangeSet:
		return false, exitCode, applyChangeSet(conn, head, root, logger)
	case protocol.MsgChangeBlob:
		return false, exitCode, applyChangeBlob(conn, head, root, logger)
	case protocol.MsgChangesDone:
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

func decodeExitCode(head protocol.Header) (int, error) {
	info, err := protocol.DecodeData[protocol.ExitInfo](head)
	if err != nil {
		return 0, err
	}

	return info.Code, nil
}

func applyChangeSet(
	conn *protocol.Conn,
	head protocol.Header,
	root string,
	logger *runLogger,
) error {
	changes, err := protocol.DecodeData[protocol.ChangeSet](head)
	if err != nil {
		return err
	}

	logger.Printf(
		"applying host changes locally: changed=%d deleted=%d",
		len(changes.Entries),
		len(changes.Deleted),
	)

	if err := syncfs.DeletePaths(root, changes.Deleted); err != nil {
		return err
	}

	return syncfs.ApplyNonFileEntries(root, syncfs.NonFileEntries(changes.Entries))
}

func applyChangeBlob(
	conn *protocol.Conn,
	head protocol.Header,
	root string,
	logger *runLogger,
) error {
	info, err := protocol.DecodeData[protocol.BlobInfo](head)
	if err != nil {
		return err
	}

	entry := syncfs.Entry{
		Path: info.Path,
		Kind: syncfs.KindFile,
		Hash: info.Hash,
		Size: head.PayloadLen,
		Mode: info.Mode,
	}

	var received int64

	reader := &progressReader{
		src: conn.PayloadReader(head),
		onRead: func(n int) {
			received += int64(n)
			logger.Every(
				"download",
				"download progress: current_file=%s bytes=%d/%d",
				info.Path,
				received,
				head.PayloadLen,
			)
		},
	}

	return syncfs.WriteFile(root, entry, reader)
}

func expectFrame(conn *protocol.Conn, wantType string) (protocol.Header, error) {
	head, err := conn.ReadHeader()
	if err != nil {
		return protocol.Header{}, err
	}

	if head.Type == protocol.MsgError {
		return protocol.Header{}, decodeServerError(head)
	}

	if head.Type != wantType {
		return protocol.Header{}, fmt.Errorf("expected %s, got %s", wantType, head.Type)
	}

	return head, nil
}

func expectDataFrame[T any](conn *protocol.Conn, wantType string) (T, error) {
	head, err := expectFrame(conn, wantType)
	if err != nil {
		var zero T
		return zero, err
	}

	return protocol.DecodeData[T](head)
}

type PairOptions struct {
	Address             string
	DiscoveryService    string
	Host                clientstate.HostRecord
	Code                string
	ClientLabel         string
	PreviousFingerprint string
	CSRPEM              []byte
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

	return expectDataFrame[protocol.PairCodeResponse](conn, protocol.MsgPairCodeResponse)
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

	return expectDataFrame[protocol.PairResponse](conn, protocol.MsgPairResponse)
}

func GenerateClientIdentity(label string) ([]byte, []byte, []byte, error) {
	return security.GenerateClientIdentity(label)
}

func GenerateCSR(keyPEM []byte, label string) ([]byte, error) {
	return security.GenerateCSRFromKey(keyPEM, label)
}

func sendMissingBlobs(
	conn *protocol.Conn,
	hashes []string,
	blobSources map[string]string,
	logger *runLogger,
) error {
	ordered := append([]string(nil), hashes...)
	sort.Strings(ordered)

	if len(ordered) == 0 {
		logger.Printf("host already has all file blobs")

		return nil
	}

	logger.Printf("uploading missing file blobs: files=%d", len(ordered))

	sent := 0

	var bytesSent int64

	for _, hash := range ordered {
		srcPath, ok := blobSources[hash]
		if !ok {
			return fmt.Errorf("host requested unknown blob %s", hash)
		}

		f, err := os.Open(srcPath)
		if err != nil {
			return fmt.Errorf("open blob source %s: %w", srcPath, err)
		}

		info, err := f.Stat()
		if err != nil {
			closeQuietly(f)
			return fmt.Errorf("stat blob source %s: %w", srcPath, err)
		}

		reader := &progressReader{
			src: f,
			onRead: func(n int) {
				bytesSent += int64(n)
				logger.Every(
					"upload",
					"upload progress: files=%d/%d bytes=%d",
					sent,
					len(ordered),
					bytesSent,
				)
			},
		}

		if err := conn.WriteFrom(
			protocol.MsgBlob,
			protocol.BlobInfo{Hash: hash, Size: info.Size()},
			reader,
			info.Size(),
		); err != nil {
			closeQuietly(f)
			return err
		}

		closeQuietly(f)

		sent++
		logger.Every(
			"upload",
			"upload progress: files=%d/%d bytes=%d",
			sent,
			len(ordered),
			bytesSent,
		)
	}

	logger.Printf("upload done: files=%d bytes=%d", sent, bytesSent)

	return nil
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
