package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/syncfs"
	"github.com/manuel-huez/rmtx/internal/terminal"
)

type ExecOptions struct {
	Address      string
	Token        string
	Root         string
	CWD          string
	Command      []string
	Mounts       []syncfs.MountSpec
	ForwardEnv   []string
	ExtraEnv     map[string]string
	Stdout       io.Writer
	Stderr       io.Writer
	Stdin        io.Reader
	StdinFile    *os.File
	StdoutFile   *os.File
	StderrFile   *os.File
	ForwardStdin bool
	Session      string
	Project      string
	ContextID    string
	ContextName  string
	TTY          bool
}

type RemoteOptions struct {
	Address string
	Token   string
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

	manifest, request, err := buildRunRequest(root, workdir, &opts)
	if err != nil {
		return 1, err
	}

	conn, err := dialAndAuthenticate(ctx, opts.Address, opts.Token)
	if err != nil {
		return 1, err
	}
	defer closeQuietly(conn.Raw())

	if err := runHandshake(conn, request, manifest.BlobSources); err != nil {
		return 1, err
	}

	return processExecFrames(ctx, conn, root, opts)
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

	if strings.TrimSpace(opts.Token) == "" {
		return errors.New("client token is required")
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
	root string,
	workdir string,
	opts *ExecOptions,
) (syncfs.BuildResult, protocol.RunRequest, error) {
	manifest, err := syncfs.BuildManifest(root, opts.Mounts)
	if err != nil {
		return syncfs.BuildResult{}, protocol.RunRequest{}, err
	}

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

func dialAndAuthenticate(ctx context.Context, address, token string) (*protocol.Conn, error) {
	dialer := net.Dialer{}

	raw, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("dial host %s: %w", address, err)
	}

	conn := protocol.NewConn(raw)
	if err := authenticate(conn, token); err != nil {
		closeQuietly(raw)
		return nil, err
	}

	return conn, nil
}

func runHandshake(
	conn *protocol.Conn,
	request protocol.RunRequest,
	blobSources map[string]string,
) error {
	if err := conn.WriteJSON(protocol.MsgRunRequest, request); err != nil {
		return err
	}

	need, err := expectDataFrame[protocol.NeedBlobs](conn, protocol.MsgNeedBlobs)
	if err != nil {
		return err
	}

	if err := sendMissingBlobs(conn, need.Hashes, blobSources); err != nil {
		return err
	}

	if err := conn.WriteJSON(protocol.MsgSyncComplete, nil); err != nil {
		return err
	}

	_, err = expectDataFrame[protocol.WorkspaceReady](conn, protocol.MsgWorkspaceReady)

	return err
}

func processExecFrames(
	ctx context.Context,
	conn *protocol.Conn,
	root string,
	opts ExecOptions,
) (int, error) {
	if opts.TTY {
		return processTTYExecFrames(ctx, conn, root, opts)
	}

	stdinErrCh := make(chan error, 1)
	go func() { stdinErrCh <- sendStdin(conn, opts.Stdin, opts.ForwardStdin) }()

	exitCode := 0

	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return exitCode, err
		}

		done, updatedExitCode, err := handleExecFrame(conn, head, root, opts, exitCode)
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

		done, updatedExitCode, err := handleExecFrame(conn, head, root, opts, exitCode)
		if err != nil {
			return updatedExitCode, err
		}

		exitCode = updatedExitCode

		if done {
			if inputErrCh != nil {
				select {
				case inputErr := <-inputErrCh:
					if inputErr != nil && !errors.Is(inputErr, io.EOF) {
						return exitCode, inputErr
					}
				default:
				}
			}

			return exitCode, nil
		}
	}
}

func handleExecFrame(
	conn *protocol.Conn,
	head protocol.Header,
	root string,
	opts ExecOptions,
	exitCode int,
) (bool, int, error) {
	switch head.Type {
	case protocol.MsgError:
		return false, exitCode, decodeServerError(head)
	case protocol.MsgExecOutput:
		return false, exitCode, copyExecOutput(conn, head, opts)
	case protocol.MsgExecExit:
		code, err := decodeExitCode(head)
		return false, code, err
	case protocol.MsgChangeSet:
		return false, exitCode, applyChangeSet(conn, head, root)
	case protocol.MsgChangeBlob:
		return false, exitCode, applyChangeBlob(conn, head, root)
	case protocol.MsgChangesDone:
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

func applyChangeSet(conn *protocol.Conn, head protocol.Header, root string) error {
	changes, err := protocol.DecodeData[protocol.ChangeSet](head)
	if err != nil {
		return err
	}

	if err := syncfs.DeletePaths(root, changes.Deleted); err != nil {
		return err
	}

	return syncfs.ApplyNonFileEntries(root, filterNonFileEntries(changes.Entries))
}

func applyChangeBlob(conn *protocol.Conn, head protocol.Header, root string) error {
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

	return syncfs.WriteFile(root, entry, conn.PayloadReader(head))
}

func filterNonFileEntries(entries []syncfs.Entry) []syncfs.Entry {
	nonFiles := make([]syncfs.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Kind != syncfs.KindFile {
			nonFiles = append(nonFiles, entry)
		}
	}

	return nonFiles
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

func authenticate(conn *protocol.Conn, token string) error {
	head, err := conn.ReadHeader()
	if err != nil {
		return err
	}

	if head.Type == protocol.MsgError {
		return decodeServerError(head)
	}

	if head.Type != protocol.MsgAuthHello {
		return fmt.Errorf("expected %s, got %s", protocol.MsgAuthHello, head.Type)
	}

	hello, err := protocol.DecodeData[protocol.AuthHello](head)
	if err != nil {
		return err
	}

	resp := protocol.AuthResponse{MAC: protocol.SignToken(token, hello.Nonce)}
	if err := conn.WriteJSON(protocol.MsgAuthResponse, resp); err != nil {
		return err
	}

	head, err = conn.ReadHeader()
	if err != nil {
		return err
	}

	if head.Type == protocol.MsgError {
		return decodeServerError(head)
	}

	if head.Type != protocol.MsgAuthOK {
		return fmt.Errorf("expected %s, got %s", protocol.MsgAuthOK, head.Type)
	}

	return nil
}

func sendMissingBlobs(conn *protocol.Conn, hashes []string, blobSources map[string]string) error {
	ordered := append([]string(nil), hashes...)
	sort.Strings(ordered)

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

		if err := conn.WriteFrom(
			protocol.MsgBlob,
			protocol.BlobInfo{Hash: hash, Size: info.Size()},
			f,
			info.Size(),
		); err != nil {
			closeQuietly(f)
			return err
		}

		closeQuietly(f)
	}

	return nil
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
