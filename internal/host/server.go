package host

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/manuel-huez/rmtx/internal/discovery"
	"github.com/manuel-huez/rmtx/internal/pathutil"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/security"
	"github.com/manuel-huez/rmtx/internal/syncfs"
	"github.com/manuel-huez/rmtx/internal/version"
)

const (
	defaultDirMode     = 0o755
	streamBufferSize   = 32 * 1024
	splitNEquals       = 2
	pipeCount          = 2
	exitCodeNotFound   = 127
	defaultFileMode    = 0o644
	reverseDialTimeout = 5 * time.Second
	progressEvery      = 3 * time.Second
	blobUploadParallel = 4
)

type Options struct {
	ListenAddr       string
	StateDir         string
	AdvertiseName    string
	DiscoveryService string
	DisableDiscovery bool
	Logger           *log.Logger
}

type Server struct {
	opts        Options
	listener    net.Listener
	blobStore   *syncfs.BlobStore
	logger      *log.Logger
	advertiser  *discovery.Responder
	tlsConfig   *tls.Config
	hostPKI     security.HostPKI
	fingerprint string
	listenerMu  sync.RWMutex

	contextLocksMu sync.Mutex
	contextLocks   map[string]*sync.Mutex
	activeMu       sync.Mutex
	activeContexts map[string]int
	uploadsMu      sync.Mutex
	uploads        map[string]*blobUploadSession
	ociMu          sync.Mutex
}

func New(opts Options) (*Server, error) {
	opts = withDefaultOptions(opts)

	if err := os.MkdirAll(opts.StateDir, defaultDirMode); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}

	if err := os.MkdirAll(
		filepath.Join(opts.StateDir, contextDirName),
		defaultDirMode,
	); err != nil {
		return nil, fmt.Errorf("create context dir: %w", err)
	}

	store := syncfs.NewBlobStore(filepath.Join(opts.StateDir, "blobs"))
	if err := store.Ensure(); err != nil {
		return nil, fmt.Errorf("prepare blob store: %w", err)
	}

	serverName := strings.TrimSpace(opts.AdvertiseName)
	if serverName == "" {
		if hostName, err := os.Hostname(); err == nil && strings.TrimSpace(hostName) != "" {
			serverName = hostName
		} else {
			serverName = "rmtx-host"
		}
	}

	hostPKI, err := security.EnsureHostPKI(opts.StateDir, serverName)
	if err != nil {
		return nil, err
	}

	tlsConfig, fingerprint, err := security.ServerTLSConfig(hostPKI)
	if err != nil {
		return nil, err
	}

	return &Server{
		opts:           opts,
		blobStore:      store,
		logger:         opts.Logger,
		tlsConfig:      tlsConfig,
		hostPKI:        hostPKI,
		fingerprint:    fingerprint,
		contextLocks:   map[string]*sync.Mutex{},
		activeContexts: map[string]int{},
		uploads:        map[string]*blobUploadSession{},
	}, nil
}

func withDefaultOptions(opts Options) Options {
	if strings.TrimSpace(opts.ListenAddr) == "" {
		opts.ListenAddr = ":33221"
	}

	if strings.TrimSpace(opts.StateDir) == "" {
		opts.StateDir = defaultStateDir()
	}

	if strings.TrimSpace(opts.DiscoveryService) == "" {
		opts.DiscoveryService = "rmtx"
	}

	if opts.Logger == nil {
		opts.Logger = log.New(io.Discard, "", 0)
	}

	return opts
}

func defaultStateDir() string {
	home, _ := os.UserHomeDir()
	if home == "" {
		home = "."
	}

	return filepath.Join(home, ".local", "state", "rmtx")
}

func (s *Server) Addr() string {
	s.listenerMu.RLock()
	defer s.listenerMu.RUnlock()

	if s.listener == nil {
		return ""
	}

	return s.listener.Addr().String()
}

func (s *Server) Fingerprint() string {
	return s.fingerprint
}

func (s *Server) hostName() string {
	return effectiveHostName(s.opts.AdvertiseName)
}

func (s *Server) Serve(ctx context.Context) error {
	base, err := net.Listen("tcp", s.opts.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.opts.ListenAddr, err)
	}

	ln := tls.NewListener(base, s.tlsConfig)

	defer func() { _ = ln.Close() }()

	s.listenerMu.Lock()
	s.listener = ln
	s.listenerMu.Unlock()

	defer func() {
		s.listenerMu.Lock()
		if s.listener == ln {
			s.listener = nil
		}
		s.listenerMu.Unlock()
	}()

	s.logger.Printf("listening on %s", ln.Addr().String())

	if !s.opts.DisableDiscovery {
		if tcpAddr, ok := ln.Addr().(*net.TCPAddr); ok {
			adv, err := discovery.Advertise(
				ctx,
				s.opts.DiscoveryService,
				s.opts.AdvertiseName,
				tcpAddr.Port,
				discovery.AdvertiseOptions{
					OS:              runtime.GOOS,
					HostFingerprint: s.fingerprint,
					PairingEnabled:  true,
					OnReverseConnect: func(address string) {
						s.handleReverseConnect(ctx, address)
					},
				},
			)
			if err != nil {
				s.logger.Printf("discovery advertise failed: %v", err)
			} else {
				s.advertiser = adv

				defer func() { _ = adv.Close() }()
			}
		}
	}

	go func() { <-ctx.Done(); _ = ln.Close() }()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			var ne net.Error
			if errors.As(err, &ne) {
				continue
			}

			return fmt.Errorf("accept connection: %w", err)
		}

		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleReverseConnect(parent context.Context, address string) {
	ctx, cancel := context.WithTimeout(parent, reverseDialTimeout)
	defer cancel()

	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		s.logger.Printf("reverse connect to %s failed: %v", address, err)

		return
	}

	s.handleConn(parent, tls.Server(raw, s.tlsConfig))
}

func (s *Server) handleConn(parent context.Context, raw net.Conn) {
	defer func() { _ = raw.Close() }()

	conn := protocol.NewConn(raw)
	if err := s.handleConnSession(parent, conn); err != nil {
		s.logger.Printf("request failed: remote=%s error=%v", raw.RemoteAddr(), err)
		_ = conn.WriteJSON(protocol.MsgError, protocol.ErrorMessage{Message: err.Error()})
	}
}

func isDisconnectError(err error) bool {
	var errno syscall.Errno

	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, windowsConnectionReset) ||
		(errors.As(err, &errno) && isDisconnectErrno(errno))
}

const windowsConnectionReset syscall.Errno = 10054

func isDisconnectErrno(errno syscall.Errno) bool {
	return errno == syscall.ECONNABORTED ||
		errno == syscall.ECONNRESET ||
		errno == syscall.EPIPE ||
		errno == windowsConnectionReset
}

func (s *Server) handleConnSession(parent context.Context, conn *protocol.Conn) error {
	head, err := conn.ReadHeader()
	if err != nil {
		if isDisconnectError(err) {
			return nil
		}

		return err
	}

	s.logger.Printf("request received: remote=%s type=%s", conn.Raw().RemoteAddr(), head.Type)

	if err := s.dispatchSessionRequest(parent, conn, head); err != nil {
		if isDisconnectError(err) {
			return nil
		}

		return err
	}

	return nil
}

func (s *Server) handleRunRequest(
	parent context.Context,
	conn *protocol.Conn,
	request protocol.RunRequest,
) error {
	if err := validateRunRequest(request); err != nil {
		return err
	}

	s.logger.Printf(
		"run request received: context=%s session=%s workdir=%s command=%q mounts=%d entries=%d",
		request.ContextID,
		request.Session,
		request.WorkDir,
		strings.Join(request.Command, " "),
		len(request.Mounts),
		len(request.Manifest),
	)

	release := s.acquireContext(request.ContextID)
	defer release()

	handle, err := s.ensureContext(request.ContextID, request.ContextName, request.RootHint)
	if err != nil {
		return err
	}

	runtimePrep, hasPreparedRuntime, err := s.prepareRuntimeBeforeSync(parent, handle, request)
	if err != nil {
		return err
	}

	var preparedRuntimeRef *preparedRuntime
	if hasPreparedRuntime {
		preparedRuntimeRef = &runtimePrep
	}

	currentManifest, err := s.loadTrackedManifest(request.ContextID)
	if err != nil {
		return err
	}

	if err := s.syncContextFromClient(
		parent,
		conn,
		request.ContextID,
		request.Session,
		handle.workspace,
		currentManifest,
		request.Manifest,
	); err != nil {
		return err
	}

	if err := s.saveTrackedManifest(request.ContextID, request.Manifest); err != nil {
		return err
	}

	if err := writeWorkspaceReady(conn, request.ContextID, handle); err != nil {
		return err
	}

	return s.executeAndSyncRun(parent, conn, handle, request, preparedRuntimeRef)
}

//nolint:cyclop // Protocol dispatch is deliberately centralized.
func (s *Server) dispatchSessionRequest(
	parent context.Context,
	conn *protocol.Conn,
	head protocol.Header,
) error {
	if head.Type == protocol.MsgPairCodeRequest {
		return s.dispatchPairCodeRequest(conn, head)
	}

	if head.Type == protocol.MsgPairRequest {
		return s.dispatchPairRequest(conn, head)
	}

	if _, err := s.requireTrustedClient(conn); err != nil {
		return err
	}

	switch head.Type {
	case protocol.MsgRunRequest:
		return s.dispatchRunRequest(parent, conn, head)
	case protocol.MsgBlobUploadRequest:
		return s.dispatchBlobUploadRequest(conn, head)
	case protocol.MsgPingRequest:
		return s.discardAndHandle(head, conn, s.handlePing)
	case protocol.MsgListContextsRequest:
		return s.discardAndHandle(head, conn, s.handleListContexts)
	case protocol.MsgDeleteContextsRequest:
		return s.dispatchDeleteContexts(conn, head)
	case protocol.MsgContextArtifactsRequest:
		return s.dispatchContextArtifacts(conn, head)
	case protocol.MsgCachePruneRequest:
		return s.discardAndHandle(head, conn, s.handleCachePrune)
	default:
		if err := conn.DiscardPayload(head); err != nil {
			return err
		}

		return fmt.Errorf("unexpected request %s", head.Type)
	}
}

func (s *Server) dispatchPairCodeRequest(conn *protocol.Conn, head protocol.Header) error {
	req, err := protocol.DecodeData[protocol.PairCodeRequest](head)
	if err != nil {
		return err
	}

	return s.handlePairCodeRequest(conn, req)
}

func (s *Server) dispatchPairRequest(conn *protocol.Conn, head protocol.Header) error {
	req, err := protocol.DecodeData[protocol.PairRequest](head)
	if err != nil {
		return err
	}

	return s.handlePairRequest(conn, req)
}

func (s *Server) dispatchRunRequest(
	parent context.Context,
	conn *protocol.Conn,
	head protocol.Header,
) error {
	req, err := protocol.DecodeData[protocol.RunRequest](head)
	if err != nil {
		return err
	}

	return s.handleRunRequest(parent, conn, req)
}

func (s *Server) dispatchBlobUploadRequest(conn *protocol.Conn, head protocol.Header) error {
	req, err := protocol.DecodeData[protocol.BlobUploadRequest](head)
	if err != nil {
		return err
	}

	return s.handleBlobUploadRequest(conn, req)
}

func (s *Server) dispatchDeleteContexts(conn *protocol.Conn, head protocol.Header) error {
	req, err := protocol.DecodeData[protocol.DeleteContextsRequest](head)
	if err != nil {
		return err
	}

	return s.handleDeleteContexts(conn, req)
}

func (s *Server) dispatchContextArtifacts(conn *protocol.Conn, head protocol.Header) error {
	req, err := protocol.DecodeData[protocol.ContextArtifactsRequest](head)
	if err != nil {
		return err
	}

	return s.handleContextArtifacts(conn, req)
}

func (s *Server) discardAndHandle(
	head protocol.Header,
	conn *protocol.Conn,
	handler func(*protocol.Conn) error,
) error {
	if err := conn.DiscardPayload(head); err != nil {
		return err
	}

	return handler(conn)
}

func validateRunRequest(request protocol.RunRequest) error {
	if len(request.Command) == 0 {
		return errors.New("missing command")
	}

	if strings.TrimSpace(request.ContextID) == "" {
		return errors.New("missing context id")
	}

	if err := validateRuntimeSpec(request.Runtime); err != nil {
		return err
	}

	return nil
}

func writeWorkspaceReady(conn *protocol.Conn, contextID string, handle contextHandle) error {
	return conn.WriteJSON(
		protocol.MsgWorkspaceReady,
		protocol.WorkspaceReady{
			ContextID: contextID,
			Created:   handle.created,
			Workspace: handle.workspace,
		},
	)
}

func (s *Server) executeAndSyncRun(
	parent context.Context,
	conn *protocol.Conn,
	handle contextHandle,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
) error {
	code, runErr := s.executeRequest(parent, conn, handle.workspace, request, preparedRuntime)
	if err := writeUserVisibleRunError(conn, runErr); err != nil {
		return err
	}

	if err := conn.WriteJSON(protocol.MsgExecExit, protocol.ExitInfo{Code: code}); err != nil {
		return err
	}

	s.logger.Printf(
		"scanning workspace changes after command: context=%s session=%s",
		request.ContextID,
		request.Session,
	)

	post, err := syncfs.BuildManifestContextOptions(
		parent,
		handle.workspace,
		request.Mounts,
		syncfs.BuildOptions{
			Progress: s.logBuildProgress(request.ContextID, request.Session, "post-run"),
		},
	)
	if err != nil {
		return fmt.Errorf("scan workspace changes: %w", err)
	}

	postEntries := post.Entries
	ignoreMode := false

	if hostIsWindows() {
		ignoreMode = true
		postEntries = syncfs.NormalizeModes(postEntries, request.Manifest)
		postEntries = syncfs.PreserveMissingEntries(
			postEntries,
			request.Manifest,
			syncfs.KindSymlink,
		)
	}

	closeCompressed, err := s.sendWorkspaceChanges(
		conn,
		handle.workspace,
		request.Manifest,
		postEntries,
		request.SyncBack,
		ignoreMode,
	)
	if err != nil {
		return err
	}
	defer func() {
		if closeCompressed != nil {
			_ = closeCompressed()
		}
	}()

	if err := s.saveTrackedManifest(request.ContextID, postEntries); err != nil {
		return err
	}

	handle.meta.UpdatedAt = time.Now().UTC()
	if err := saveContextMetadata(handle.dir, handle.meta); err != nil {
		return err
	}

	if err := conn.WriteJSON(protocol.MsgChangesDone, nil); err != nil {
		return err
	}

	if closeCompressed != nil {
		return closeCompressed()
	}

	return nil
}

func (s *Server) handlePing(conn *protocol.Conn) error {
	contexts, err := s.listContexts()
	if err != nil {
		return err
	}

	s.logger.Printf(
		"ping request handled: remote=%s contexts=%d",
		conn.Raw().RemoteAddr(),
		len(contexts),
	)

	return conn.WriteJSON(protocol.MsgPingResponse, protocol.PingResponse{
		Online:       true,
		Version:      version.String(),
		Name:         s.hostName(),
		Address:      s.Addr(),
		Fingerprint:  s.fingerprint,
		Now:          time.Now().UTC(),
		ContextCount: len(contexts),
	})
}

func (s *Server) handleListContexts(conn *protocol.Conn) error {
	contexts, err := s.listContexts()
	if err != nil {
		return err
	}

	s.logger.Printf(
		"context list handled: remote=%s contexts=%d ids=%s",
		conn.Raw().RemoteAddr(),
		len(contexts),
		formatContextSummaryIDs(contexts),
	)

	return conn.WriteJSON(
		protocol.MsgListContextsResponse,
		protocol.ListContextsResponse{Contexts: contexts},
	)
}

func (s *Server) handleDeleteContexts(
	conn *protocol.Conn,
	req protocol.DeleteContextsRequest,
) error {
	s.logger.Printf(
		"context delete requested: remote=%s ids=%s all=%t older_than=%q",
		conn.Raw().RemoteAddr(),
		formatStrings(req.IDs),
		req.All,
		req.OlderThan,
	)

	resp, err := s.deleteContexts(req)
	if err != nil {
		return err
	}

	s.logger.Printf(
		"context delete handled: remote=%s deleted=%s not_found=%s",
		conn.Raw().RemoteAddr(),
		formatContextSummaryIDs(resp.Deleted),
		formatStrings(resp.NotFound),
	)

	return conn.WriteJSON(protocol.MsgDeleteContextsResponse, resp)
}

func (s *Server) executeRequest(
	parent context.Context,
	conn *protocol.Conn,
	workspace string,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
) (int, error) {
	sessionCtx, cancel := context.WithCancel(parent)
	defer cancel()

	code, runErr := s.runCommand(sessionCtx, cancel, conn, workspace, request, preparedRuntime)
	if runErr != nil {
		s.logger.Printf(
			"session %s context %s exited with error: %v",
			request.Session,
			request.ContextID,
			runErr,
		)
	}

	return code, runErr
}

func (s *Server) sendWorkspaceChanges(
	conn *protocol.Conn,
	workspace string,
	before []syncfs.Entry,
	after []syncfs.Entry,
	syncBack []string,
	ignoreMode bool,
) (func() error, error) {
	changed, deleted := diffWorkspaceChanges(before, after, syncBack, ignoreMode)
	compression := chooseWorkspaceChangeCompression(workspace, changed)
	s.logger.Printf(
		"sending workspace changes: sync_back=%s changed=%d deleted=%d compression=%s",
		formatSyncBack(syncBack),
		len(changed),
		len(deleted),
		compression,
	)

	var closeCompressed func() error
	if compression != "" {
		if err := conn.WriteJSON(
			protocol.MsgSyncCompressionStart,
			protocol.CompressionInfo{Algorithm: compression},
		); err != nil {
			return nil, err
		}

		var err error
		closeCompressed, err = conn.EnableZstdWriter()
		if err != nil {
			return nil, err
		}
	}

	if err := conn.WriteJSON(
		protocol.MsgChangeSet,
		protocol.ChangeSet{Entries: changed, Deleted: deleted},
	); err != nil {
		if closeCompressed != nil {
			_ = closeCompressed()
		}

		return nil, err
	}

	if err := s.sendChangedFileBlobs(conn, workspace, changed); err != nil {
		if closeCompressed != nil {
			_ = closeCompressed()
		}

		return nil, err
	}

	return closeCompressed, nil
}

func chooseWorkspaceChangeCompression(workspace string, entries []syncfs.Entry) string {
	candidates := make([]syncfs.CompressionCandidate, 0, len(entries))
	for _, entry := range entries {
		if entry.Kind != syncfs.KindFile {
			continue
		}

		candidates = append(candidates, syncfs.CompressionCandidate{
			Path: filepath.Join(workspace, filepath.FromSlash(entry.Path)),
			Size: entry.Size,
		})
	}

	if syncfs.ShouldCompressTransfer(candidates) {
		return protocol.CompressionZstd
	}

	return ""
}

func diffWorkspaceChanges(
	before,
	after []syncfs.Entry,
	syncBack []string,
	ignoreMode bool,
) ([]syncfs.Entry, []string) {
	return syncfs.Diff(
		syncfs.FilterEntriesByPath(before, syncBack),
		syncfs.FilterEntriesByPath(after, syncBack),
		syncfs.DiffOptions{IgnoreMode: ignoreMode},
	)
}

func formatSyncBack(paths []string) string {
	if paths == nil {
		return "all"
	}

	if len(paths) == 0 {
		return "none"
	}

	return formatStrings(paths)
}

func (s *Server) logBuildProgress(contextID, session, label string) func(syncfs.BuildProgress) {
	return func(progress syncfs.BuildProgress) {
		switch progress.Phase {
		case "walk":
			if progress.Done {
				s.logger.Printf(
					"%s scan done: context=%s session=%s mount=%s scanned=%d files=%d dirs=%d skipped=%d",
					label,
					contextID,
					session,
					progress.Mount,
					progress.Scanned,
					progress.Files,
					progress.Dirs,
					progress.Skipped,
				)

				return
			}

			s.logger.Printf(
				"%s scan progress: context=%s session=%s mount=%s scanned=%d files=%d dirs=%d skipped=%d",
				label,
				contextID,
				session,
				progress.Mount,
				progress.Scanned,
				progress.Files,
				progress.Dirs,
				progress.Skipped,
			)
		case "hash":
			if progress.Done {
				s.logger.Printf(
					"%s hash done: context=%s session=%s files=%d/%d bytes=%d",
					label,
					contextID,
					session,
					progress.Hashed,
					progress.TotalFiles,
					progress.Bytes,
				)

				return
			}

			s.logger.Printf(
				"%s hash progress: context=%s session=%s files=%d/%d bytes=%d",
				label,
				contextID,
				session,
				progress.Hashed,
				progress.TotalFiles,
				progress.Bytes,
			)
		}
	}
}

func (s *Server) sendChangedFileBlobs(
	conn *protocol.Conn,
	workspace string,
	changed []syncfs.Entry,
) error {
	total := 0

	for _, entry := range changed {
		if entry.Kind == syncfs.KindFile {
			total++
		}
	}

	s.logger.Printf("sending changed file blobs to client: files=%d", total)

	sent := 0

	var bytesSent int64

	lastProgress := time.Time{}

	for _, entry := range changed {
		if entry.Kind != syncfs.KindFile {
			continue
		}

		onRead := func(n int) {
			bytesSent += int64(n)

			now := time.Now()
			if lastProgress.IsZero() || now.Sub(lastProgress) >= progressEvery {
				lastProgress = now

				s.logger.Printf(
					"send changed blob progress: files=%d/%d bytes=%d",
					sent,
					total,
					bytesSent,
				)
			}
		}

		if err := sendChangedFileBlob(conn, workspace, entry, onRead); err != nil {
			return err
		}

		sent++
	}

	s.logger.Printf("sending changed file blobs done: files=%d bytes=%d", sent, bytesSent)

	return nil
}

func sendChangedFileBlob(
	conn *protocol.Conn,
	workspace string,
	entry syncfs.Entry,
	onRead func(int),
) error {
	path := filepath.Join(workspace, filepath.FromSlash(entry.Path))

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open change blob %s: %w", entry.Path, err)
	}

	defer func() { _ = f.Close() }()

	return conn.WriteFrom(
		protocol.MsgChangeBlob,
		protocol.BlobInfo{
			Path:    entry.Path,
			Hash:    entry.Hash,
			Size:    entry.Size,
			Mode:    entry.Mode,
			ModTime: entry.ModTime,
		},
		&logReader{src: f, onRead: onRead},
		entry.Size,
	)
}

func (s *Server) requireTrustedClient(conn *protocol.Conn) (*x509.Certificate, error) {
	tlsConn, ok := conn.Raw().(*tls.Conn)
	if !ok {
		return nil, errors.New("connection is not tls")
	}

	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return nil, errors.New("client certificate required; run `rmtx pair`")
	}

	clientCert := state.PeerCertificates[0]
	fingerprint := security.FingerprintCert(clientCert)

	trusted, err := s.clientTrusted(fingerprint)
	if err != nil {
		return nil, err
	}

	if !trusted {
		return nil, errors.New("client certificate not trusted; run `rmtx pair`")
	}

	return clientCert, nil
}

func (s *Server) handlePairCodeRequest(conn *protocol.Conn, req protocol.PairCodeRequest) error {
	info, err := createPairCodeInfo(s.opts.StateDir, s.hostName(), 0)
	if err != nil {
		return err
	}

	clientLabel := strings.TrimSpace(req.ClientLabel)
	if clientLabel == "" {
		clientLabel = "unknown-client"
	}

	remoteAddr := "unknown"
	if raw := conn.Raw(); raw != nil && raw.RemoteAddr() != nil {
		remoteAddr = raw.RemoteAddr().String()
	}

	s.logger.Printf(
		"pair request from %s client=%q code=%s expires=%s",
		remoteAddr,
		clientLabel,
		info.Code,
		info.ExpiresAt.Format(time.RFC3339),
	)

	return conn.WriteJSON(protocol.MsgPairCodeResponse, protocol.PairCodeResponse{
		HostName:  info.HostName,
		ExpiresAt: info.ExpiresAt,
	})
}

func (s *Server) handlePairRequest(conn *protocol.Conn, req protocol.PairRequest) error {
	return withPairCode(s.opts.StateDir, req.Code, func() error {
		if strings.TrimSpace(req.CSRPEM) == "" {
			return errors.New("csr is required")
		}

		clientCertPEM, fingerprint, err := security.SignClientCSR(
			s.hostPKI.CACertPEM,
			s.hostPKI.CAKeyPEM,
			[]byte(req.CSRPEM),
			req.ClientLabel,
		)
		if err != nil {
			return err
		}

		if err := s.trustClient(fingerprint, req.PreviousFingerprint, req.ClientLabel); err != nil {
			return err
		}

		return conn.WriteJSON(protocol.MsgPairResponse, protocol.PairResponse{
			ClientCertPEM: string(clientCertPEM),
			Fingerprint:   fingerprint,
		})
	})
}

func (s *Server) receiveBlobs(
	ctx context.Context,
	conn *protocol.Conn,
	contextID string,
	session string,
	total int,
	upload *blobUploadSession,
) error {
	s.logger.Printf(
		"receiving client blobs: context=%s session=%s files=%d",
		contextID,
		session,
		total,
	)

	received := 0

	var bytesReceived int64

	lastProgress := time.Time{}

	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return fmt.Errorf("read sync frame: %w", err)
		}

		switch head.Type {
		case protocol.MsgBlob:
			if err := s.receiveControlBlob(
				conn,
				head,
				contextID,
				session,
				total,
				&received,
				&bytesReceived,
				&lastProgress,
				upload,
			); err != nil {
				return err
			}
		case protocol.MsgSyncComplete:
			if upload != nil {
				if err := waitForBlobUpload(ctx, upload); err != nil {
					return err
				}
			}

			s.logger.Printf(
				"receiving client blobs done: context=%s session=%s files=%d/%d bytes=%d",
				contextID,
				session,
				received,
				total,
				bytesReceived,
			)

			return nil
		default:
			if err := conn.DiscardPayload(head); err != nil {
				return err
			}

			return fmt.Errorf("unexpected sync frame: %s", head.Type)
		}
	}
}

func (s *Server) receiveControlBlob(
	conn *protocol.Conn,
	head protocol.Header,
	contextID string,
	session string,
	total int,
	received *int,
	bytesReceived *int64,
	lastProgress *time.Time,
	upload *blobUploadSession,
) error {
	info, err := protocol.DecodeData[protocol.BlobInfo](head)
	if err != nil {
		return err
	}

	if info.Hash == "" {
		return errors.New("blob hash is required")
	}

	reader := &logReader{
		src: conn.PayloadReader(head),
		onRead: func(n int) {
			*bytesReceived += int64(n)

			now := time.Now()
			if lastProgress.IsZero() || now.Sub(*lastProgress) >= progressEvery {
				*lastProgress = now

				s.logger.Printf(
					"receive blob progress: context=%s session=%s files=%d/%d bytes=%d",
					contextID,
					session,
					*received,
					total,
					*bytesReceived,
				)
			}
		},
	}

	if err := s.blobStore.Store(info.Hash, head.PayloadLen, reader); err != nil {
		return err
	}

	if upload != nil {
		if err := upload.complete(info.Hash); err != nil {
			return err
		}
	}

	(*received)++

	return nil
}

func waitForBlobUpload(ctx context.Context, upload *blobUploadSession) error {
	done := make(chan error, 1)

	go func() { done <- upload.wait() }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type logReader struct {
	src    io.Reader
	onRead func(int)
}

func (r *logReader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 && r.onRead != nil {
		r.onRead(n)
	}

	return n, err
}

func (s *Server) runCommand(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	workspace string,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
) (int, error) {
	workdir, err := secureJoin(workspace, request.WorkDir)
	if err != nil {
		return 1, err
	}

	s.logger.Printf(
		"starting command: context=%s session=%s workdir=%s tty=%t command=%q",
		request.ContextID,
		request.Session,
		request.WorkDir,
		request.TTY,
		strings.Join(request.Command, " "),
	)

	var (
		code   int
		runErr error
	)

	switch {
	case request.TTY:
		code, runErr = s.runTTYCommand(
			ctx,
			cancel,
			conn,
			workspace,
			workdir,
			request,
			preparedRuntime,
		)
	case isOCIRuntime(request.Runtime):
		code, runErr = s.runOCIPipeCommand(
			ctx,
			cancel,
			conn,
			workspace,
			workdir,
			request,
			preparedRuntime,
		)
	default:
		code, runErr = s.runPipeCommand(ctx, cancel, conn, workspace, workdir, request)
	}

	s.logger.Printf(
		"command finished: context=%s session=%s exit_code=%d err=%v",
		request.ContextID,
		request.Session,
		code,
		runErr,
	)

	return code, runErr
}

func (s *Server) runPipeCommand(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	workspace string,
	workdir string,
	request protocol.RunRequest,
) (int, error) {
	cmd := s.newSessionCommand(ctx, workspace, workdir, request)

	return s.runPipeExecCommand(ctx, cancel, conn, cmd)
}

func (s *Server) runPipeExecCommand(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	cmd *exec.Cmd,
) (int, error) {
	cancelRun := s.commandCancel(cmd, cancel)
	defer cancelRun()

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return 1, fmt.Errorf("open stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 1, fmt.Errorf("open stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return 1, fmt.Errorf("open stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return exitCode(err), fmt.Errorf("start command: %w", err)
	}

	go func() {
		<-ctx.Done()
		cancelRun()
	}()

	var outWG sync.WaitGroup
	outWG.Add(pipeCount)

	go func() {
		defer outWG.Done()

		if err := streamPipe(conn, stdout, "stdout"); err != nil {
			cancelRun()
		}
	}()

	go func() {
		defer outWG.Done()

		if err := streamPipe(conn, stderr, "stderr"); err != nil {
			cancelRun()
		}
	}()

	stdinDone := make(chan error, 1)

	go func() {
		err := s.consumePipeInput(conn, stdin)
		if err != nil {
			cancelRun()
		}

		stdinDone <- err
	}()

	waitErr := cmd.Wait()
	_ = stdin.Close()

	outWG.Wait()

	select {
	case err := <-stdinDone:
		if err != nil && !errors.Is(err, io.EOF) && !isDisconnectError(err) {
			s.logger.Printf("stdin forwarding ended: %v", err)
		}
	default:
	}

	return exitCode(waitErr), waitErr
}

func (s *Server) consumePipeInput(conn *protocol.Conn, stdin io.WriteCloser) error {
	var stdinClosed bool

	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return err
		}

		if stdinClosed {
			if err := conn.DiscardPayload(head); err != nil {
				return err
			}

			continue
		}

		done, err := s.handleInputFrame(conn, head, stdin, false)
		if err != nil {
			return err
		}

		if done {
			stdinClosed = true
			_ = stdin.Close()
		}
	}
}

func (s *Server) newSessionCommand(
	ctx context.Context,
	workspace string,
	workdir string,
	request protocol.RunRequest,
) *exec.Cmd {
	cmd := exec.CommandContext(ctx, request.Command[0], request.Command[1:]...)
	configureCommandProcessGroup(cmd)
	cmd.Dir = workdir
	cmd.Env = mergeEnv(os.Environ(), request.Env)
	cmd.Env = append(
		cmd.Env,
		"RMTX=1",
		"RMTX_WORKSPACE="+workspace,
		"RMTX_CONTEXT_ID="+request.ContextID,
	)

	return cmd
}

func (s *Server) handleInputFrame(
	conn *protocol.Conn,
	head protocol.Header,
	writer io.Writer,
	allowResize bool,
) (bool, error) {
	switch head.Type {
	case protocol.MsgStdinData:
		_, err := io.Copy(writer, conn.PayloadReader(head))
		return false, err
	case protocol.MsgStdinClose:
		return true, nil
	case protocol.MsgResizeTTY:
		if !allowResize {
			if err := conn.DiscardPayload(head); err != nil {
				return false, err
			}

			return false, fmt.Errorf("unexpected frame during stdin phase: %s", head.Type)
		}

		return false, handleTTYResize(head, writer)
	default:
		if err := conn.DiscardPayload(head); err != nil {
			return false, err
		}

		phase := "stdin"
		if allowResize {
			phase = "TTY"
		}

		return false, fmt.Errorf("unexpected frame during %s phase: %s", phase, head.Type)
	}
}

func handleTTYResize(head protocol.Header, writer io.Writer) error {
	size, err := protocol.DecodeData[protocol.TTYSize](head)
	if err != nil {
		return err
	}

	if resizer, ok := writer.(interface {
		ResizeTTY(rows, cols int) error
	}); ok {
		return resizer.ResizeTTY(size.Rows, size.Cols)
	}

	file, ok := writer.(*os.File)
	if !ok {
		return nil
	}

	return resizePTY(file, size.Rows, size.Cols)
}

func streamPipe(conn *protocol.Conn, src io.Reader, stream string) error {
	buf := make([]byte, streamBufferSize)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if writeErr := conn.WriteBytes(
				protocol.MsgExecOutput,
				protocol.OutputInfo{Stream: stream},
				buf[:n],
			); writeErr != nil {
				return writeErr
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}
	}
}

func writeUserVisibleRunError(conn *protocol.Conn, err error) error {
	message := userVisibleRunError(err)
	if message == "" {
		return nil
	}

	if !strings.HasSuffix(message, "\n") {
		message += "\n"
	}

	return conn.WriteBytes(
		protocol.MsgExecOutput,
		protocol.OutputInfo{Stream: "stderr"},
		[]byte(message),
	)
}

func userVisibleRunError(err error) string {
	if err == nil {
		return ""
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return ""
	}

	return err.Error()
}

func mergeEnv(base []string, overrides map[string]string) []string {
	if len(overrides) == 0 {
		return append([]string(nil), base...)
	}

	merged := map[string]string{}
	order := make([]string, 0, len(base)+len(overrides))

	for _, entry := range base {
		parts := strings.SplitN(entry, "=", splitNEquals)
		k := parts[0]

		v := ""
		if len(parts) == splitNEquals {
			v = parts[1]
		}

		if _, ok := merged[k]; !ok {
			order = append(order, k)
		}

		merged[k] = v
	}

	for k, v := range overrides {
		if _, ok := merged[k]; !ok {
			order = append(order, k)
		}

		merged[k] = v
	}

	out := make([]string, 0, len(order))
	for _, k := range order {
		out = append(out, k+"="+merged[k])
	}

	return out
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}

	return exitCodeNotFound
}

func secureJoin(root, rel string) (string, error) {
	if strings.TrimSpace(rel) == "" {
		rel = "."
	}

	joined, err := pathutil.SecureJoin(root, filepath.FromSlash(rel))
	if err != nil {
		return "", fmt.Errorf("workdir %s escapes workspace", rel)
	}

	return joined, nil
}
