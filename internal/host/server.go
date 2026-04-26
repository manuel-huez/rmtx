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
)

const Version = "0.6.4"

const (
	defaultDirMode     = 0o755
	streamBufferSize   = 32 * 1024
	splitNEquals       = 2
	pipeCount          = 2
	exitCodeNotFound   = 127
	defaultFileMode    = 0o644
	reverseDialTimeout = 5 * time.Second
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
		_ = conn.WriteJSON(protocol.MsgError, protocol.ErrorMessage{Message: err.Error()})
	}
}

func (s *Server) handleConnSession(parent context.Context, conn *protocol.Conn) error {
	head, err := conn.ReadHeader()
	if err != nil {
		return err
	}

	return s.dispatchSessionRequest(parent, conn, head)
}

func (s *Server) handleRunRequest(
	parent context.Context,
	conn *protocol.Conn,
	request protocol.RunRequest,
) error {
	if err := validateRunRequest(request); err != nil {
		return err
	}

	release := s.acquireContext(request.ContextID)
	defer release()

	handle, err := s.ensureContext(request.ContextID, request.ContextName, request.RootHint)
	if err != nil {
		return err
	}

	currentManifest, err := s.loadTrackedManifest(request.ContextID)
	if err != nil {
		return err
	}

	if err := s.syncContextFromClient(
		conn,
		request.ContextID,
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

	if err := s.executeAndSyncRun(parent, conn, handle, request); err != nil {
		return err
	}

	return conn.WriteJSON(protocol.MsgChangesDone, nil)
}

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
	case protocol.MsgPingRequest:
		return s.discardAndHandle(head, conn, s.handlePing)
	case protocol.MsgListContextsRequest:
		return s.discardAndHandle(head, conn, s.handleListContexts)
	case protocol.MsgDeleteContextsRequest:
		return s.dispatchDeleteContexts(conn, head)
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

func (s *Server) dispatchDeleteContexts(conn *protocol.Conn, head protocol.Header) error {
	req, err := protocol.DecodeData[protocol.DeleteContextsRequest](head)
	if err != nil {
		return err
	}

	return s.handleDeleteContexts(conn, req)
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
) error {
	code := s.executeRequest(parent, conn, handle.workspace, request)
	if err := conn.WriteJSON(protocol.MsgExecExit, protocol.ExitInfo{Code: code}); err != nil {
		return err
	}

	post, err := syncfs.BuildManifest(handle.workspace, request.Mounts)
	if err != nil {
		return fmt.Errorf("scan workspace changes: %w", err)
	}

	if err := s.sendWorkspaceChanges(
		conn,
		handle.workspace,
		request.Manifest,
		post.Entries,
	); err != nil {
		return err
	}

	if err := s.saveTrackedManifest(request.ContextID, post.Entries); err != nil {
		return err
	}

	handle.meta.UpdatedAt = time.Now().UTC()
	if err := saveContextMetadata(handle.dir, handle.meta); err != nil {
		return err
	}

	return nil
}

func (s *Server) handlePing(conn *protocol.Conn) error {
	contexts, err := s.listContexts()
	if err != nil {
		return err
	}

	return conn.WriteJSON(protocol.MsgPingResponse, protocol.PingResponse{
		Online:       true,
		Version:      Version,
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

	return conn.WriteJSON(
		protocol.MsgListContextsResponse,
		protocol.ListContextsResponse{Contexts: contexts},
	)
}

func (s *Server) handleDeleteContexts(
	conn *protocol.Conn,
	req protocol.DeleteContextsRequest,
) error {
	resp, err := s.deleteContexts(req)
	if err != nil {
		return err
	}

	return conn.WriteJSON(protocol.MsgDeleteContextsResponse, resp)
}

func (s *Server) executeRequest(
	parent context.Context,
	conn *protocol.Conn,
	workspace string,
	request protocol.RunRequest,
) int {
	sessionCtx, cancel := context.WithCancel(parent)
	defer cancel()

	code, runErr := s.runCommand(sessionCtx, cancel, conn, workspace, request)
	if runErr != nil {
		s.logger.Printf(
			"session %s context %s exited with error: %v",
			request.Session,
			request.ContextID,
			runErr,
		)
	}

	return code
}

func (s *Server) sendWorkspaceChanges(
	conn *protocol.Conn,
	workspace string,
	before []syncfs.Entry,
	after []syncfs.Entry,
) error {
	changed, deleted := syncfs.Diff(before, after)
	if err := conn.WriteJSON(
		protocol.MsgChangeSet,
		protocol.ChangeSet{Entries: changed, Deleted: deleted},
	); err != nil {
		return err
	}

	return sendChangedFileBlobs(conn, workspace, changed)
}

func sendChangedFileBlobs(conn *protocol.Conn, workspace string, changed []syncfs.Entry) error {
	for _, entry := range changed {
		if entry.Kind != syncfs.KindFile {
			continue
		}

		if err := sendChangedFileBlob(conn, workspace, entry); err != nil {
			return err
		}
	}

	return nil
}

func sendChangedFileBlob(conn *protocol.Conn, workspace string, entry syncfs.Entry) error {
	path := filepath.Join(workspace, filepath.FromSlash(entry.Path))

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open change blob %s: %w", entry.Path, err)
	}

	defer func() { _ = f.Close() }()

	return conn.WriteFrom(
		protocol.MsgChangeBlob,
		protocol.BlobInfo{Path: entry.Path, Hash: entry.Hash, Size: entry.Size, Mode: entry.Mode},
		f,
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

func (s *Server) receiveBlobs(conn *protocol.Conn) error {
	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return fmt.Errorf("read sync frame: %w", err)
		}

		switch head.Type {
		case protocol.MsgBlob:
			info, err := protocol.DecodeData[protocol.BlobInfo](head)
			if err != nil {
				return err
			}

			if info.Hash == "" {
				return errors.New("blob hash is required")
			}

			if err := s.blobStore.Store(
				info.Hash,
				head.PayloadLen,
				conn.PayloadReader(head),
			); err != nil {
				return err
			}
		case protocol.MsgSyncComplete:
			return nil
		default:
			if err := conn.DiscardPayload(head); err != nil {
				return err
			}

			return fmt.Errorf("unexpected sync frame: %s", head.Type)
		}
	}
}

func (s *Server) runCommand(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	workspace string,
	request protocol.RunRequest,
) (int, error) {
	workdir, err := secureJoin(workspace, request.WorkDir)
	if err != nil {
		return 1, err
	}

	if request.TTY {
		return s.runTTYCommand(ctx, cancel, conn, workspace, workdir, request)
	}

	return s.runPipeCommand(ctx, cancel, conn, workspace, workdir, request)
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

	var outWG sync.WaitGroup
	outWG.Add(pipeCount)

	go func() {
		defer outWG.Done()

		if err := streamPipe(conn, stdout, "stdout"); err != nil {
			cancel()
		}
	}()

	go func() {
		defer outWG.Done()

		if err := streamPipe(conn, stderr, "stderr"); err != nil {
			cancel()
		}
	}()

	stdinDone := make(chan error, 1)

	go func() { stdinDone <- s.consumePipeInput(conn, stdin) }()

	waitErr := cmd.Wait()
	_ = stdin.Close()

	outWG.Wait()

	if err := <-stdinDone; err != nil && !errors.Is(err, io.EOF) {
		s.logger.Printf("stdin forwarding ended: %v", err)
	}

	return exitCode(waitErr), waitErr
}

func (s *Server) consumePipeInput(conn *protocol.Conn, stdin io.WriteCloser) error {
	defer func() { _ = stdin.Close() }()

	for {
		head, err := conn.ReadHeader()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		done, err := s.handleInputFrame(conn, head, stdin, false)
		if err != nil {
			return err
		}

		if done {
			return nil
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
