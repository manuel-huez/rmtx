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
	"time"

	"github.com/manuel-huez/rmtx/internal/discovery"
	"github.com/manuel-huez/rmtx/internal/pathutil"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/security"
	"github.com/manuel-huez/rmtx/internal/syncfs"
	"github.com/manuel-huez/rmtx/internal/version"
)

const (
	defaultDirMode          = 0o755
	streamBufferSize        = 32 * 1024
	splitNEquals            = 2
	pipeCount               = 2
	exitCodeNotFound        = 127
	defaultFileMode         = 0o644
	noneValue               = "none"
	reverseDialTimeout      = 5 * time.Second
	progressEvery           = 3 * time.Second
	maxBlobTransferParallel = 16
	materializeParallel     = 4
	hostUpdateTimeout       = 5 * time.Minute
	startupCleanupTimeout   = 15 * time.Second
	restartPollEvery        = 100 * time.Millisecond
	tcpKeepAliveEvery       = 15 * time.Second
)

var (
	ErrRestartRequested = errors.New("host restart requested")
	errRestartPending   = errors.New("host restart in progress; retry shortly")

	requestHeaderIdleTimeout = 30 * time.Second
	sessionIdleTimeout       = 45 * time.Second
	sessionHeartbeatInterval = 10 * time.Second
)

type RestartRequestedError struct {
	Executable string
}

func (e *RestartRequestedError) Error() string {
	return ErrRestartRequested.Error()
}

func (e *RestartRequestedError) Unwrap() error {
	return ErrRestartRequested
}

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
	logHub      *hostLogHub
	advertiser  *discovery.Responder
	tlsConfig   *tls.Config
	hostPKI     security.HostPKI
	fingerprint string
	listenerMu  sync.RWMutex
	restartMu   sync.Mutex
	restarting  bool

	activeRuns           int
	restartExecutable    string
	restartVersion       string
	restartInstallTarget string

	contextLocksMu  sync.Mutex
	contextLocks    map[string]*sync.Mutex
	activeMu        sync.Mutex
	activeContexts  map[string]int
	blobTransfersMu sync.Mutex
	blobTransfers   map[string]*blobTransferSession
	// Hold during pre-run sync so blob GC cannot remove hashes before manifest commit.
	blobGCMu     sync.RWMutex
	ociMu        sync.Mutex
	updateMu     sync.Mutex
	updateRunner updateRunner
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

	logHub := newHostLogHub(opts.Logger.Writer())
	logger := log.New(logHub, opts.Logger.Prefix(), opts.Logger.Flags())

	cleanupStartupCaches(context.Background(), opts.StateDir, logger)

	return &Server{
		opts:           opts,
		blobStore:      store,
		logger:         logger,
		logHub:         logHub,
		tlsConfig:      tlsConfig,
		hostPKI:        hostPKI,
		fingerprint:    fingerprint,
		contextLocks:   map[string]*sync.Mutex{},
		activeContexts: map[string]int{},
		blobTransfers:  map[string]*blobTransferSession{},
		updateRunner:   defaultUpdateRunner,
	}, nil
}

func cleanupStartupCaches(ctx context.Context, stateDir string, logger *log.Logger) {
	updates, err := pruneOldUpdateDirs(stateDir)
	logCleanupResult(logger, "update", "dirs", updates, err)

	temps, err := pruneStartupTempFiles(stateDir)
	logCleanupResult(logger, "temp", "files", temps, err)

	// WSL can stall while distros initialize or update; startup cleanup is best-effort.
	cleanupCtx, cancel := context.WithTimeout(ctx, startupCleanupTimeout)
	defer cancel()

	removed, _, err := pruneWSLStagedRootFS(cleanupCtx, stateDir)
	logArtifactCleanupResult(logger, "WSL rootfs", "dirs", removed, err)
}

func logCleanupResult(logger *log.Logger, name string, unit string, removed []string, err error) {
	if err != nil {
		logger.Printf("%s cleanup failed: %v", name, err)

		return
	}

	if len(removed) > 0 {
		logger.Printf("%s cleanup removed: %s=%d", name, unit, len(removed))
	}
}

func logArtifactCleanupResult(
	logger *log.Logger,
	name string,
	unit string,
	removed []protocol.ContextArtifact,
	err error,
) {
	if err != nil {
		logger.Printf("%s cleanup failed: %v", name, err)

		return
	}

	if len(removed) > 0 {
		logger.Printf("%s cleanup removed: %s=%d", name, unit, len(removed))
	}
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

func (s *Server) acquireRun() (func(), error) {
	s.restartMu.Lock()
	defer s.restartMu.Unlock()

	if s.restarting {
		return nil, errRestartPending
	}

	s.activeRuns++

	return func() {
		s.restartMu.Lock()
		defer s.restartMu.Unlock()

		if s.activeRuns > 0 {
			s.activeRuns--
		}
	}, nil
}

type restartState struct {
	Executable    string
	Version       string
	InstallTarget string
}

func (s *Server) beginRestart(executable, targetVersion, installTarget string) bool {
	s.restartMu.Lock()
	defer s.restartMu.Unlock()

	if s.restarting {
		return false
	}

	s.restarting = true
	s.restartExecutable = executable
	s.restartVersion = targetVersion
	s.restartInstallTarget = installTarget

	return true
}

func (s *Server) cancelRestart() {
	s.restartMu.Lock()
	defer s.restartMu.Unlock()

	s.restarting = false
	s.restartExecutable = ""
	s.restartVersion = ""
	s.restartInstallTarget = ""
}

func (s *Server) waitForActiveRuns(ctx context.Context) error {
	ticker := time.NewTicker(restartPollEvery)
	defer ticker.Stop()

	for {
		s.restartMu.Lock()
		activeRuns := s.activeRuns
		s.restartMu.Unlock()

		if activeRuns == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for active runs before restart: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *Server) finishRestart() {
	s.listenerMu.RLock()
	ln := s.listener
	s.listenerMu.RUnlock()

	if ln != nil {
		_ = ln.Close()
	}
}

func (s *Server) restartRequest() (restartState, bool) {
	s.restartMu.Lock()
	defer s.restartMu.Unlock()

	return restartState{
		Executable:    s.restartExecutable,
		Version:       s.restartVersion,
		InstallTarget: s.restartInstallTarget,
	}, s.restarting
}

func (s *Server) restartWasRequested() bool {
	s.restartMu.Lock()
	defer s.restartMu.Unlock()

	return s.restarting
}

func (s *Server) hostName() string {
	return effectiveHostName(s.opts.AdvertiseName)
}

func (s *Server) Serve(ctx context.Context) error {
	listenConfig := net.ListenConfig{KeepAlive: tcpKeepAliveEvery}
	base, err := listenConfig.Listen(ctx, "tcp", s.opts.ListenAddr)
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

			if restart, ok := s.restartRequest(); ok {
				return &RestartRequestedError{Executable: restart.Executable}
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

	raw, err := (&net.Dialer{KeepAlive: tcpKeepAliveEvery}).DialContext(ctx, "tcp", address)
	if err != nil {
		s.logger.Printf("reverse connect to %s failed: %v", address, err)

		return
	}

	s.handleConn(parent, tls.Server(raw, s.tlsConfig))
}

func (s *Server) handleConn(parent context.Context, raw net.Conn) {
	raw = protocol.NewIdleDeadlineConn(raw)
	defer func() { _ = raw.Close() }()

	sessionCtx, cancel := context.WithCancel(parent)
	defer cancel()

	stopContextClose := context.AfterFunc(sessionCtx, func() { _ = raw.Close() })
	defer stopContextClose()

	conn := protocol.NewConn(raw)
	if err := s.handleConnSession(sessionCtx, cancel, conn); err != nil {
		s.logger.Printf("request failed: remote=%s error=%v", raw.RemoteAddr(), err)
		_ = conn.WriteJSON(protocol.MsgError, protocol.ErrorMessage{Message: err.Error()})
	}
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

func (s *Server) handleConnSession(
	parent context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
) error {
	for {
		setConnectionIdleTimeout(conn, requestHeaderIdleTimeout)

		head, err := conn.ReadHeader()
		if err != nil {
			if protocol.IsDisconnectError(err) {
				return nil
			}

			return err
		}

		keepOpen, err := s.handleConnRequest(parent, cancel, conn, head)
		if err != nil || !keepOpen {
			return err
		}
	}
}

func (s *Server) handleConnRequest(
	parent context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	head protocol.Header,
) (bool, error) {
	var requestLogs *hostLogSubscription
	if streamsHostLogs(head.Type) {
		requestLogs = s.logHub.Subscribe(conn)
		defer requestLogs.Close()
	}

	s.logger.Printf("request received: remote=%s type=%s", conn.Raw().RemoteAddr(), head.Type)

	stopLiveness := func() { setConnectionIdleTimeout(conn, 0) }
	if requestUsesSessionLiveness(head.Type) {
		stopLiveness = startSessionLiveness(
			parent,
			cancel,
			conn,
			requestUsesOutboundHeartbeat(head.Type),
		)
	}
	defer stopLiveness()

	if err := s.dispatchSessionRequest(parent, conn, head, requestLogs); err != nil {
		if protocol.IsDisconnectError(err) {
			return false, nil
		}

		s.logger.Printf("request failed: remote=%s error=%v", conn.Raw().RemoteAddr(), err)
		requestLogs.Flush()

		return false, conn.WriteJSON(protocol.MsgError, protocol.ErrorMessage{Message: err.Error()})
	}

	return true, nil
}

func streamsHostLogs(messageType string) bool {
	return messageType == protocol.MsgRunRequest ||
		messageType == protocol.MsgHostUpdateRequest
}

func (s *Server) handleRunRequest(
	parent context.Context,
	conn *protocol.Conn,
	request protocol.RunRequest,
	runLogs *hostLogSubscription,
) error {
	if err := validateRunRequest(request); err != nil {
		return err
	}

	releaseRun, err := s.acquireRun()
	if err != nil {
		return err
	}
	defer releaseRun()

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

	runtimePrep, hasPreparedRuntime, err := s.prepareRuntimeBeforeSync(
		parent,
		handle,
		request,
		runLogs,
	)
	if err != nil {
		return err
	}

	var preparedRuntimeRef *preparedRuntime
	if hasPreparedRuntime {
		preparedRuntimeRef = &runtimePrep
	}

	if err := s.syncClientManifestAndSave(parent, conn, request, handle, runLogs); err != nil {
		return err
	}

	s.pruneUnreferencedBlobsAfterRunSave(request, "pre-run", runLogs)

	runLogs.Flush()

	if err := writeWorkspaceReady(conn, request.ContextID, handle); err != nil {
		return err
	}

	if err := s.executeAndSyncRun(parent, conn, handle, request, preparedRuntimeRef, runLogs); err != nil {
		return err
	}

	return nil
}

func (s *Server) cleanRunWorkspace(
	contextID string,
	workspace string,
	runLogs *hostLogSubscription,
) error {
	// Marker first makes interrupted cleanup recoverable on the next sync.
	if err := s.markWorkspaceCleaned(contextID); err != nil {
		return err
	}

	if err := removeContextSetupCache(filepath.Dir(workspace)); err != nil {
		return err
	}

	if err := cleanWorkspace(workspace); err != nil {
		return err
	}

	s.logRun(runLogs, "workspace cleaned: context=%s", contextID)

	return nil
}

func (s *Server) syncClientManifestAndSave(
	parent context.Context,
	conn *protocol.Conn,
	request protocol.RunRequest,
	handle contextHandle,
	runLogs *hostLogSubscription,
) error {
	s.blobGCMu.RLock()
	defer s.blobGCMu.RUnlock()

	currentManifest, err := s.loadTrackedManifest(request.ContextID)
	if err != nil {
		return err
	}

	if s.workspaceWasCleaned(request.ContextID) {
		// Cleanup marker may mean cleanup completed or was interrupted.
		if err := removeContextSetupCache(handle.dir); err != nil {
			return err
		}

		if err := cleanWorkspace(handle.workspace); err != nil {
			return err
		}

		currentManifest = nil
	}

	if err := s.syncContextFromClient(
		parent,
		conn,
		request.ContextID,
		request.Session,
		handle.workspace,
		currentManifest,
		request.Manifest,
		runLogs,
	); err != nil {
		return err
	}

	if err := s.saveTrackedManifest(request.ContextID, request.Manifest); err != nil {
		return err
	}

	return s.clearWorkspaceCleaned(request.ContextID)
}

//nolint:cyclop // Protocol dispatch is deliberately centralized.
func (s *Server) dispatchSessionRequest(
	parent context.Context,
	conn *protocol.Conn,
	head protocol.Header,
	requestLogs *hostLogSubscription,
) error {
	if head.Type == protocol.MsgPairCodeRequest {
		return s.dispatchPairCodeRequest(conn, head, requestLogs)
	}

	if head.Type == protocol.MsgPairRequest {
		return s.dispatchPairRequest(conn, head, requestLogs)
	}

	if _, err := s.requireTrustedClient(conn); err != nil {
		return err
	}

	switch head.Type {
	case protocol.MsgRunRequest:
		return s.dispatchRunRequest(parent, conn, head, requestLogs)
	case protocol.MsgBlobTransferRequest:
		return s.dispatchBlobTransferRequest(parent, conn, head)
	case protocol.MsgPingRequest:
		return s.discardAndHandle(head, conn, func(conn *protocol.Conn) error {
			return s.handlePing(conn, requestLogs)
		})
	case protocol.MsgHostStatsRequest:
		return s.discardAndHandle(head, conn, func(conn *protocol.Conn) error {
			return s.handleHostStats(parent, conn, requestLogs)
		})
	case protocol.MsgHostUpdateRequest:
		return s.dispatchHostUpdateRequest(parent, conn, head, requestLogs)
	case protocol.MsgListContextsRequest:
		return s.discardAndHandle(head, conn, func(conn *protocol.Conn) error {
			return s.handleListContexts(conn, requestLogs)
		})
	case protocol.MsgDeleteContextsRequest:
		return s.dispatchDeleteContexts(parent, conn, head, requestLogs)
	case protocol.MsgContextArtifactsRequest:
		return s.dispatchContextArtifacts(conn, head, requestLogs)
	case protocol.MsgCachePruneRequest:
		return s.discardAndHandle(head, conn, func(conn *protocol.Conn) error {
			return s.handleCachePrune(parent, conn, requestLogs)
		})
	default:
		if err := conn.DiscardPayload(head); err != nil {
			return err
		}

		return fmt.Errorf("unexpected request %s", head.Type)
	}
}

func (s *Server) dispatchPairCodeRequest(
	conn *protocol.Conn,
	head protocol.Header,
	requestLogs *hostLogSubscription,
) error {
	req, err := protocol.DecodeData[protocol.PairCodeRequest](head)
	if err != nil {
		return err
	}

	return s.handlePairCodeRequest(conn, req, requestLogs)
}

func (s *Server) dispatchPairRequest(
	conn *protocol.Conn,
	head protocol.Header,
	requestLogs *hostLogSubscription,
) error {
	req, err := protocol.DecodeData[protocol.PairRequest](head)
	if err != nil {
		return err
	}

	return s.handlePairRequest(conn, req, requestLogs)
}

func (s *Server) dispatchRunRequest(
	parent context.Context,
	conn *protocol.Conn,
	head protocol.Header,
	runLogs *hostLogSubscription,
) error {
	req, err := protocol.DecodeData[protocol.RunRequest](head)
	if err != nil {
		return err
	}

	return s.handleRunRequest(parent, conn, req, runLogs)
}

func (s *Server) dispatchBlobTransferRequest(
	ctx context.Context,
	conn *protocol.Conn,
	head protocol.Header,
) error {
	req, err := protocol.DecodeData[protocol.BlobTransferRequest](head)
	if err != nil {
		return err
	}

	return s.handleBlobTransferRequest(ctx, conn, req)
}

func (s *Server) dispatchHostUpdateRequest(
	parent context.Context,
	conn *protocol.Conn,
	head protocol.Header,
	requestLogs *hostLogSubscription,
) error {
	req, err := protocol.DecodeData[protocol.HostUpdateRequest](head)
	if err != nil {
		return err
	}

	return s.handleHostUpdateRequest(parent, conn, req, requestLogs)
}

func (s *Server) dispatchDeleteContexts(
	parent context.Context,
	conn *protocol.Conn,
	head protocol.Header,
	requestLogs *hostLogSubscription,
) error {
	req, err := protocol.DecodeData[protocol.DeleteContextsRequest](head)
	if err != nil {
		return err
	}

	return s.handleDeleteContexts(parent, conn, req, requestLogs)
}

func (s *Server) dispatchContextArtifacts(
	conn *protocol.Conn,
	head protocol.Header,
	requestLogs *hostLogSubscription,
) error {
	req, err := protocol.DecodeData[protocol.ContextArtifactsRequest](head)
	if err != nil {
		return err
	}

	return s.handleContextArtifacts(conn, req, requestLogs)
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

func writeJSONAfterLogs(
	conn *protocol.Conn,
	requestLogs *hostLogSubscription,
	msgType string,
	payload any,
) error {
	requestLogs.Flush()

	return conn.WriteJSON(msgType, payload)
}

func (s *Server) executeAndSyncRun(
	parent context.Context,
	conn *protocol.Conn,
	handle contextHandle,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
	runLogs *hostLogSubscription,
) error {
	code, runErr := s.executeRequest(
		parent,
		conn,
		handle.workspace,
		request,
		preparedRuntime,
		runLogs,
	)
	runLogs.Flush()

	if err := writeUserVisibleRunError(conn, runErr); err != nil {
		return err
	}

	runLogs.Flush()

	if err := conn.WriteJSON(protocol.MsgExecExit, protocol.ExitInfo{Code: code}); err != nil {
		return err
	}

	s.logRun(
		runLogs,
		"scanning workspace changes after command: context=%s session=%s",
		request.ContextID,
		request.Session,
	)

	post, err := syncfs.BuildManifestContextOptions(
		parent,
		handle.workspace,
		request.Mounts,
		syncfs.BuildOptions{
			Progress: s.logBuildProgress(request.ContextID, request.Session, "post-run", runLogs),
		},
	)
	if err != nil {
		return fmt.Errorf("scan workspace changes: %w", err)
	}

	runLogs.Flush()

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

	if err := s.sendWorkspaceChanges(
		parent,
		conn,
		request.ContextID,
		request.Session,
		handle.workspace,
		post.BlobSources,
		request.Manifest,
		postEntries,
		request.SyncBack,
		ignoreMode,
	); err != nil {
		return err
	}

	if err := s.saveTrackedManifest(request.ContextID, postEntries); err != nil {
		return err
	}

	s.pruneUnreferencedBlobsAfterRunSave(request, "post-run", runLogs)

	handle.meta.UpdatedAt = time.Now().UTC()
	if err := saveContextMetadata(handle.dir, handle.meta); err != nil {
		return err
	}

	runLogs.Flush()

	if err := s.cleanRunWorkspace(request.ContextID, handle.workspace, runLogs); err != nil {
		return err
	}

	runLogs.Flush()

	if err := conn.WriteJSON(protocol.MsgChangesDone, nil); err != nil {
		return err
	}

	return nil
}

func (s *Server) pruneUnreferencedBlobsAfterRunSave(
	request protocol.RunRequest,
	stage string,
	runLogs *hostLogSubscription,
) {
	deleted, bytes, err := s.pruneUnreferencedBlobs()
	if err != nil {
		s.logRun(
			runLogs,
			"blob cache prune failed after %s manifest save: context=%s session=%s error=%v",
			stage,
			request.ContextID,
			request.Session,
			err,
		)

		return
	}

	if len(deleted) == 0 {
		return
	}

	s.logRun(
		runLogs,
		"blob cache pruned after %s manifest save: context=%s session=%s deleted=%d bytes=%d",
		stage,
		request.ContextID,
		request.Session,
		len(deleted),
		bytes,
	)
}

func (s *Server) handlePing(
	conn *protocol.Conn,
	requestLogs *hostLogSubscription,
) error {
	contexts, err := s.listContexts()
	if err != nil {
		return err
	}

	s.logger.Printf(
		"ping request handled: remote=%s contexts=%d",
		conn.Raw().RemoteAddr(),
		len(contexts),
	)

	return writeJSONAfterLogs(conn, requestLogs, protocol.MsgPingResponse, protocol.PingResponse{
		Online:       true,
		Version:      version.String(),
		Name:         s.hostName(),
		Address:      s.Addr(),
		Fingerprint:  s.fingerprint,
		Now:          time.Now().UTC(),
		ContextCount: len(contexts),
	})
}

func (s *Server) handleListContexts(
	conn *protocol.Conn,
	requestLogs *hostLogSubscription,
) error {
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

	return writeJSONAfterLogs(
		conn,
		requestLogs,
		protocol.MsgListContextsResponse,
		protocol.ListContextsResponse{Contexts: contexts},
	)
}

func (s *Server) handleDeleteContexts(
	ctx context.Context,
	conn *protocol.Conn,
	req protocol.DeleteContextsRequest,
	requestLogs *hostLogSubscription,
) error {
	s.logger.Printf(
		"context delete requested: remote=%s ids=%s all=%t older_than=%q",
		conn.Raw().RemoteAddr(),
		formatStrings(req.IDs),
		req.All,
		req.OlderThan,
	)

	resp, err := s.deleteContexts(ctx, req)
	if err != nil {
		return err
	}

	s.logger.Printf(
		"context delete handled: remote=%s deleted=%s not_found=%s",
		conn.Raw().RemoteAddr(),
		formatContextSummaryIDs(resp.Deleted),
		formatStrings(resp.NotFound),
	)

	return writeJSONAfterLogs(conn, requestLogs, protocol.MsgDeleteContextsResponse, resp)
}

func (s *Server) executeRequest(
	parent context.Context,
	conn *protocol.Conn,
	workspace string,
	request protocol.RunRequest,
	preparedRuntime *preparedRuntime,
	runLogs *hostLogSubscription,
) (int, error) {
	sessionCtx, cancel := context.WithCancel(parent)
	defer cancel()

	code, runErr := s.runCommand(
		sessionCtx,
		cancel,
		conn,
		workspace,
		request,
		preparedRuntime,
		runLogs,
	)
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
	ctx context.Context,
	conn *protocol.Conn,
	contextID string,
	session string,
	workspace string,
	blobSources map[string]string,
	before []syncfs.Entry,
	after []syncfs.Entry,
	syncBack []string,
	ignoreMode bool,
) error {
	changed, deleted := diffWorkspaceChanges(before, after, syncBack, ignoreMode)
	s.logger.Printf(
		"sending workspace changes: sync_back=%s changed=%d deleted=%d",
		formatSyncBack(syncBack),
		len(changed),
		len(deleted),
	)

	if err := conn.WriteJSON(
		protocol.MsgChangeSet,
		protocol.ChangeSet{Entries: changed, Deleted: deleted},
	); err != nil {
		return err
	}

	need, err := readClientNeedBlobs(conn)
	if err != nil {
		return err
	}

	transfer, err := s.sendChangedFileBlobs(
		conn,
		contextID,
		session,
		workspace,
		blobSources,
		changed,
		need,
	)
	if transfer != nil {
		defer s.unregisterBlobTransferSession(transfer.token)
	}
	if err != nil {
		return err
	}

	if err := s.storeChangedFileBlobs(workspace, blobSources, changed); err != nil {
		return err
	}

	if err := s.waitForClientSyncCompleteAndBlobTransfer(ctx, conn, transfer); err != nil {
		return err
	}

	return nil
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
		return noneValue
	}

	return formatStrings(paths)
}

func (s *Server) logBuildProgress(
	contextID,
	session,
	label string,
	runLogs io.Writer,
) func(syncfs.BuildProgress) {
	return func(progress syncfs.BuildProgress) {
		switch progress.Phase {
		case "walk":
			if progress.Done {
				s.logRun(
					runLogs,
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

			s.logRun(
				runLogs,
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
				s.logRun(
					runLogs,
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

			s.logRun(
				runLogs,
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
	contextID string,
	session string,
	workspace string,
	blobSources map[string]string,
	changed []syncfs.Entry,
	need protocol.NeedBlobs,
) (*blobTransferSession, error) {
	items, descriptors, err := prepareDownloadBlobItems(workspace, blobSources, changed, need.Hashes)
	if err != nil {
		return nil, err
	}

	chunkSize := protocol.DefaultBlobChunkSize
	chunks := protocol.PlanBlobChunks(descriptors, chunkSize)
	parallel := transferParallelism(len(chunks))

	s.logger.Printf(
		"sending changed file blobs to client: blobs=%d chunks=%d parallel=%d",
		len(items),
		len(chunks),
		parallel,
	)

	if len(items) == 0 {
		return nil, conn.WriteJSON(protocol.MsgNeedBlobs, protocol.NeedBlobs{})
	}

	token, err := protocol.RandomNonce()
	if err != nil {
		return nil, err
	}

	transfer := newBlobSendSession(
		contextID,
		session,
		token,
		items,
		len(chunks),
		chunkSize,
		parallel,
	)
	s.registerBlobTransferSession(transfer)

	if err := conn.WriteJSON(
		protocol.MsgNeedBlobs,
		protocol.NeedBlobs{
			Hashes:        need.Hashes,
			TransferToken: token,
			Parallel:      parallel,
			ChunkSize:     chunkSize,
		},
	); err != nil {
		return transfer, err
	}

	return transfer, nil
}

type downloadBlobItem struct {
	hash string
	path string
	size int64
}

func prepareDownloadBlobItems(
	workspace string,
	blobSources map[string]string,
	changed []syncfs.Entry,
	hashes []string,
) (map[string]downloadBlobItem, []protocol.BlobDescriptor, error) {
	entriesByHash := map[string]syncfs.Entry{}
	for _, entry := range changed {
		if entry.Kind != syncfs.KindFile || entry.Hash == "" {
			continue
		}
		if _, ok := entriesByHash[entry.Hash]; !ok {
			entriesByHash[entry.Hash] = entry
		}
	}

	items := make(map[string]downloadBlobItem, len(hashes))
	descriptors := make([]protocol.BlobDescriptor, 0, len(hashes))
	for _, hash := range hashes {
		entry, ok := entriesByHash[hash]
		if !ok {
			return nil, nil, fmt.Errorf("client requested unknown changed blob %s", hash)
		}

		source := blobSources[hash]
		if source == "" {
			source = filepath.Join(workspace, filepath.FromSlash(entry.Path))
		}

		items[hash] = downloadBlobItem{hash: hash, path: source, size: entry.Size}
		descriptors = append(descriptors, protocol.BlobDescriptor{Hash: hash, Size: entry.Size})
	}

	return items, descriptors, nil
}

func (s *Server) storeChangedFileBlobs(
	workspace string,
	blobSources map[string]string,
	changed []syncfs.Entry,
) error {
	stored := map[string]struct{}{}
	for _, entry := range changed {
		if entry.Kind != syncfs.KindFile || entry.Hash == "" {
			continue
		}
		if _, ok := stored[entry.Hash]; ok {
			continue
		}

		source := blobSources[entry.Hash]
		if source == "" {
			source = filepath.Join(workspace, filepath.FromSlash(entry.Path))
		}
		if err := s.blobStore.StorePath(entry.Hash, entry.Size, source); err != nil {
			return fmt.Errorf("store changed blob %s: %w", entry.Path, err)
		}
		stored[entry.Hash] = struct{}{}
	}

	return nil
}

func readClientNeedBlobs(conn *protocol.Conn) (protocol.NeedBlobs, error) {
	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return protocol.NeedBlobs{}, err
		}

		switch head.Type {
		case protocol.MsgHeartbeat, protocol.MsgStdinData, protocol.MsgStdinClose:
			if err := conn.DiscardPayload(head); err != nil {
				return protocol.NeedBlobs{}, err
			}
		case protocol.MsgRunCancel:
			if err := conn.DiscardPayload(head); err != nil {
				return protocol.NeedBlobs{}, err
			}
			return protocol.NeedBlobs{}, context.Canceled
		case protocol.MsgNeedBlobs:
			return protocol.DecodeData[protocol.NeedBlobs](head)
		default:
			if err := conn.DiscardPayload(head); err != nil {
				return protocol.NeedBlobs{}, err
			}
			return protocol.NeedBlobs{}, fmt.Errorf("expected %s, got %s", protocol.MsgNeedBlobs, head.Type)
		}
	}
}

func readClientSyncComplete(conn *protocol.Conn) error {
	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return err
		}

		switch head.Type {
		case protocol.MsgHeartbeat, protocol.MsgStdinData, protocol.MsgStdinClose:
			if err := conn.DiscardPayload(head); err != nil {
				return err
			}
		case protocol.MsgRunCancel:
			if err := conn.DiscardPayload(head); err != nil {
				return err
			}
			return context.Canceled
		case protocol.MsgSyncComplete:
			return nil
		default:
			if err := conn.DiscardPayload(head); err != nil {
				return err
			}
			return fmt.Errorf("expected %s, got %s", protocol.MsgSyncComplete, head.Type)
		}
	}
}

func (s *Server) waitForClientSyncCompleteAndBlobTransfer(
	ctx context.Context,
	conn *protocol.Conn,
	transfer *blobTransferSession,
) error {
	if transfer == nil {
		return readClientSyncComplete(conn)
	}

	syncDone := make(chan error, 1)
	go func() { syncDone <- readClientSyncComplete(conn) }()

	transferDone := make(chan error, 1)
	go func() { transferDone <- transfer.wait(ctx) }()

	for syncDone != nil || transferDone != nil {
		select {
		case err := <-syncDone:
			syncDone = nil
			if err != nil {
				_ = transfer.fail(err)
				return err
			}
		case err := <-transferDone:
			transferDone = nil
			if err != nil {
				if conn != nil && conn.Raw() != nil {
					_ = conn.Raw().SetReadDeadline(time.Now())
				}
				if syncDone != nil {
					<-syncDone
				}
				return err
			}
		}
	}

	s.logger.Printf("sending changed file blobs done")
	return nil
}

func (s *Server) requireTrustedClient(conn *protocol.Conn) (*x509.Certificate, error) {
	raw := conn.Raw()
	if wrapped, ok := raw.(interface{ Underlying() net.Conn }); ok {
		raw = wrapped.Underlying()
	}

	tlsConn, ok := raw.(*tls.Conn)
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

func (s *Server) handlePairCodeRequest(
	conn *protocol.Conn,
	req protocol.PairCodeRequest,
	requestLogs *hostLogSubscription,
) error {
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

	return writeJSONAfterLogs(
		conn,
		requestLogs,
		protocol.MsgPairCodeResponse,
		protocol.PairCodeResponse{
			HostName:  info.HostName,
			ExpiresAt: info.ExpiresAt,
		},
	)
}

func (s *Server) handlePairRequest(
	conn *protocol.Conn,
	req protocol.PairRequest,
	requestLogs *hostLogSubscription,
) error {
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

		return writeJSONAfterLogs(
			conn,
			requestLogs,
			protocol.MsgPairResponse,
			protocol.PairResponse{
				ClientCertPEM: string(clientCertPEM),
				Fingerprint:   fingerprint,
			},
		)
	})
}

func (s *Server) waitForClientBlobTransfer(
	ctx context.Context,
	conn *protocol.Conn,
	contextID string,
	session string,
	total int,
	transfer *blobTransferSession,
) error {
	s.logger.Printf(
		"receiving client blobs: context=%s session=%s files=%d",
		contextID,
		session,
		total,
	)

	for {
		head, err := conn.ReadHeader()
		if err != nil {
			if transfer != nil {
				_ = transfer.fail(err)
			}
			return fmt.Errorf("read sync frame: %w", err)
		}

		switch head.Type {
		case protocol.MsgHeartbeat:
			if err := conn.DiscardPayload(head); err != nil {
				if transfer != nil {
					_ = transfer.fail(err)
				}
				return err
			}
		case protocol.MsgSyncComplete:
			if transfer != nil {
				if err := transfer.wait(ctx); err != nil {
					return err
				}
			}

			s.logger.Printf(
				"receiving client blobs done: context=%s session=%s files=%d",
				contextID,
				session,
				total,
			)

			return nil
		default:
			if err := conn.DiscardPayload(head); err != nil {
				if transfer != nil {
					_ = transfer.fail(err)
				}
				return err
			}

			err := fmt.Errorf("unexpected sync frame: %s", head.Type)
			if transfer != nil {
				_ = transfer.fail(err)
			}
			return err
		}
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
	runLogs *hostLogSubscription,
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
			runLogs,
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
			runLogs,
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
	cancelRun := newRunCancelHandle(cancel)
	input := s.startPipeInputForwarding(conn, cancelRun.Cancel)

	return s.runPipeExecCommandWithInput(ctx, cancel, conn, cmd, input, cancelRun)
}

func (s *Server) runPipeExecCommandWithInput(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	cmd *exec.Cmd,
	input *pipeInputForwarding,
	cancelRunHandle *runCancelHandle,
) (int, error) {
	cancelRun := s.commandCancel(cmd, cancel)
	if cancelRunHandle != nil {
		cancelRunHandle.Set(cancelRun)
	}
	defer cancelRun()

	inputStopped := false
	stopInput := func() {
		if inputStopped {
			return
		}
		inputStopped = true
		if err := stopPipeInputReader(conn, input); err != nil && !errors.Is(err, io.EOF) {
			s.logger.Printf("stdin forwarding ended: %v", err)
		}
	}
	defer stopInput()

	stdout, stderr, err := commandOutputPipes(cmd)
	if err != nil {
		return 1, err
	}
	cmd.Stdin = input.Reader()

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

	waitErr := cmd.Wait()

	outWG.Wait()
	stopInput()

	return exitCode(waitErr), waitErr
}

func commandOutputPipes(cmd *exec.Cmd) (io.ReadCloser, io.ReadCloser, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("open stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("open stderr pipe: %w", err)
	}

	return stdout, stderr, nil
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
	cmd.Env = mergeEnvEntries(cmd.Env, rmtxRunEnv(ctx, workspace, request.ContextID))

	return cmd
}

func handleTTYResize(head protocol.Header, writer io.Writer) error {
	size, err := protocol.DecodeData[protocol.TTYSize](head)
	if err != nil {
		return err
	}

	return resizeTTYWriter(writer, size.Rows, size.Cols)
}

func resizeTTYWriter(writer io.Writer, rows, cols int) error {
	if resizer, ok := writer.(interface {
		ResizeTTY(rows, cols int) error
	}); ok {
		return resizer.ResizeTTY(rows, cols)
	}

	file, ok := writer.(*os.File)
	if !ok {
		return nil
	}

	return resizePTY(file, rows, cols)
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
