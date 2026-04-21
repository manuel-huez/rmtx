package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/manuel-huez/rmtx/internal/discovery"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/syncfs"
)

const Version = "0.1.0"

type Options struct {
	ListenAddr       string
	Token            string
	StateDir         string
	AdvertiseName    string
	DiscoveryService string
	DisableDiscovery bool
	Logger           *log.Logger
}

type Server struct {
	opts       Options
	listener   net.Listener
	blobStore  *syncfs.BlobStore
	logger     *log.Logger
	advertiser *discovery.Responder
}

func New(opts Options) (*Server, error) {
	if strings.TrimSpace(opts.Token) == "" {
		return nil, errors.New("host token is required")
	}

	if strings.TrimSpace(opts.ListenAddr) == "" {
		opts.ListenAddr = ":33221"
	}

	if strings.TrimSpace(opts.StateDir) == "" {
		home, _ := os.UserHomeDir()
		if home == "" {
			home = "."
		}

		opts.StateDir = filepath.Join(home, ".local", "state", "rmtx")
	}

	if strings.TrimSpace(opts.DiscoveryService) == "" {
		opts.DiscoveryService = "rmtx"
	}

	if opts.Logger == nil {
		opts.Logger = log.New(io.Discard, "", 0)
	}

	if err := os.MkdirAll(opts.StateDir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}

	if err := os.MkdirAll(filepath.Join(opts.StateDir, "sessions"), 0o755); err != nil {
		return nil, fmt.Errorf("create session dir: %w", err)
	}

	store := syncfs.NewBlobStore(filepath.Join(opts.StateDir, "blobs"))
	if err := store.Ensure(); err != nil {
		return nil, fmt.Errorf("prepare blob store: %w", err)
	}

	return &Server{opts: opts, blobStore: store, logger: opts.Logger}, nil
}

func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}

	return s.listener.Addr().String()
}

func (s *Server) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.opts.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.opts.ListenAddr, err)
	}
	defer ln.Close()

	s.listener = ln
	s.logger.Printf("listening on %s", ln.Addr().String())

	if !s.opts.DisableDiscovery {
		if tcpAddr, ok := ln.Addr().(*net.TCPAddr); ok {
			adv, err := discovery.Advertise(
				ctx,
				s.opts.DiscoveryService,
				s.opts.AdvertiseName,
				tcpAddr.Port,
				nil,
			)
			if err != nil {
				s.logger.Printf("discovery advertise failed: %v", err)
			} else {
				s.advertiser = adv
				defer adv.Close()
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

func (s *Server) handleConn(parent context.Context, raw net.Conn) {
	defer raw.Close()

	conn := protocol.NewConn(raw)
	if err := s.authenticate(conn); err != nil {
		_ = conn.WriteJSON(protocol.MsgError, protocol.ErrorMessage{Message: err.Error()})
		return
	}

	head, err := conn.ReadHeader()
	if err != nil {
		return
	}

	if head.Type != protocol.MsgRunRequest {
		_ = conn.WriteJSON(
			protocol.MsgError,
			protocol.ErrorMessage{
				Message: fmt.Sprintf("expected %s, got %s", protocol.MsgRunRequest, head.Type),
			},
		)

		return
	}

	request, err := protocol.DecodeData[protocol.RunRequest](head)
	if err != nil {
		_ = conn.WriteJSON(protocol.MsgError, protocol.ErrorMessage{Message: err.Error()})
		return
	}

	if len(request.Command) == 0 {
		_ = conn.WriteJSON(protocol.MsgError, protocol.ErrorMessage{Message: "missing command"})
		return
	}

	missing := s.blobStore.MissingHashes(request.Manifest)
	if err := conn.WriteJSON(
		protocol.MsgNeedBlobs,
		protocol.NeedBlobs{Hashes: missing},
	); err != nil {
		return
	}

	if err := s.receiveBlobs(conn); err != nil {
		_ = conn.WriteJSON(protocol.MsgError, protocol.ErrorMessage{Message: err.Error()})
		return
	}

	workspace, err := os.MkdirTemp(filepath.Join(s.opts.StateDir, "sessions"), "session-")
	if err != nil {
		_ = conn.WriteJSON(
			protocol.MsgError,
			protocol.ErrorMessage{Message: fmt.Sprintf("create workspace: %v", err)},
		)

		return
	}
	defer os.RemoveAll(workspace)

	if err := syncfs.MaterializeWorkspace(workspace, request.Manifest, s.blobStore); err != nil {
		_ = conn.WriteJSON(protocol.MsgError, protocol.ErrorMessage{Message: err.Error()})
		return
	}

	if err := conn.WriteJSON(protocol.MsgWorkspaceReady, nil); err != nil {
		return
	}

	sessionCtx, cancel := context.WithCancel(parent)
	defer cancel()

	code, runErr := s.runCommand(sessionCtx, cancel, conn, workspace, request)
	if runErr != nil {
		s.logger.Printf("session %s exited with error: %v", request.Session, runErr)
	}

	if err := conn.WriteJSON(protocol.MsgExecExit, protocol.ExitInfo{Code: code}); err != nil {
		return
	}

	post, err := syncfs.BuildManifest(workspace, request.Mounts)
	if err != nil {
		_ = conn.WriteJSON(
			protocol.MsgError,
			protocol.ErrorMessage{Message: fmt.Sprintf("scan workspace changes: %v", err)},
		)

		return
	}

	changed, deleted := syncfs.Diff(request.Manifest, post.Entries)
	if err := conn.WriteJSON(
		protocol.MsgChangeSet,
		protocol.ChangeSet{Entries: changed, Deleted: deleted},
	); err != nil {
		return
	}

	for _, entry := range changed {
		if entry.Kind != syncfs.KindFile {
			continue
		}

		path := filepath.Join(workspace, filepath.FromSlash(entry.Path))

		f, err := os.Open(path)
		if err != nil {
			_ = conn.WriteJSON(
				protocol.MsgError,
				protocol.ErrorMessage{
					Message: fmt.Sprintf("open change blob %s: %v", entry.Path, err),
				},
			)

			return
		}

		if err := conn.WriteFrom(
			protocol.MsgChangeBlob,
			protocol.BlobInfo{
				Path: entry.Path,
				Hash: entry.Hash,
				Size: entry.Size,
				Mode: entry.Mode,
			},
			f,
			entry.Size,
		); err != nil {
			f.Close()
			return
		}

		_ = f.Close()
	}

	_ = conn.WriteJSON(protocol.MsgChangesDone, nil)
}

func (s *Server) authenticate(conn *protocol.Conn) error {
	nonce, err := protocol.RandomNonce()
	if err != nil {
		return err
	}

	if err := conn.WriteJSON(
		protocol.MsgAuthHello,
		protocol.AuthHello{Nonce: nonce, Version: Version},
	); err != nil {
		return err
	}

	head, err := conn.ReadHeader()
	if err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}

	if head.Type != protocol.MsgAuthResponse {
		return fmt.Errorf("expected %s, got %s", protocol.MsgAuthResponse, head.Type)
	}

	resp, err := protocol.DecodeData[protocol.AuthResponse](head)
	if err != nil {
		return err
	}

	if !protocol.VerifyToken(s.opts.Token, nonce, resp.MAC) {
		return errors.New("authentication failed")
	}

	return conn.WriteJSON(protocol.MsgAuthOK, nil)
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

	cmd := exec.CommandContext(ctx, request.Command[0], request.Command[1:]...)
	cmd.Dir = workdir
	cmd.Env = mergeEnv(os.Environ(), request.Env)
	cmd.Env = append(cmd.Env, "RMTX=1", "RMTX_WORKSPACE="+workspace)

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
	outWG.Add(2)

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

	go func() { stdinDone <- s.consumeStdin(conn, stdin) }()

	waitErr := cmd.Wait()
	_ = stdin.Close()

	outWG.Wait()

	if err := <-stdinDone; err != nil && !errors.Is(err, io.EOF) {
		s.logger.Printf("stdin forwarding ended: %v", err)
	}

	return exitCode(waitErr), waitErr
}

func (s *Server) consumeStdin(conn *protocol.Conn, stdin io.WriteCloser) error {
	defer stdin.Close()

	for {
		head, err := conn.ReadHeader()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		switch head.Type {
		case protocol.MsgStdinData:
			if _, err := io.Copy(stdin, conn.PayloadReader(head)); err != nil {
				return err
			}
		case protocol.MsgStdinClose:
			return nil
		default:
			if err := conn.DiscardPayload(head); err != nil {
				return err
			}

			return fmt.Errorf("unexpected frame during stdin phase: %s", head.Type)
		}
	}
}

func streamPipe(conn *protocol.Conn, src io.Reader, stream string) error {
	buf := make([]byte, 32*1024)
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
		parts := strings.SplitN(entry, "=", 2)
		k := parts[0]

		v := ""
		if len(parts) == 2 {
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

	return 127
}

func secureJoin(root, rel string) (string, error) {
	if strings.TrimSpace(rel) == "" {
		rel = "."
	}

	joined := filepath.Join(root, filepath.FromSlash(rel))

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}

	absJoined, err := filepath.Abs(joined)
	if err != nil {
		return "", err
	}

	if absJoined != absRoot && !strings.HasPrefix(absJoined, absRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("workdir %s escapes workspace", rel)
	}

	return absJoined, nil
}
