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
	ForwardStdin bool
	Session      string
	Project      string
}

func Run(ctx context.Context, opts ExecOptions) (int, error) {
	if strings.TrimSpace(opts.Address) == "" {
		return 1, errors.New("host address is required")
	}

	if strings.TrimSpace(opts.Token) == "" {
		return 1, errors.New("client token is required")
	}

	if len(opts.Command) == 0 {
		return 1, errors.New("command is required")
	}

	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}

	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}

	if strings.TrimSpace(opts.Root) == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return 1, fmt.Errorf("get working directory: %w", err)
		}

		opts.Root = cwd
	}

	root, err := filepath.Abs(opts.Root)
	if err != nil {
		return 1, fmt.Errorf("resolve root: %w", err)
	}

	cwd := opts.CWD
	if strings.TrimSpace(cwd) == "" {
		cwd, err = os.Getwd()
		if err != nil {
			return 1, fmt.Errorf("get working directory: %w", err)
		}
	}

	workdir, err := filepath.Rel(root, cwd)
	if err != nil {
		return 1, fmt.Errorf("compute workdir: %w", err)
	}

	workdir = filepath.ToSlash(filepath.Clean(workdir))
	if strings.HasPrefix(workdir, "../") || workdir == ".." {
		return 1, fmt.Errorf("current directory %s is outside project root %s", cwd, root)
	}

	manifest, err := syncfs.BuildManifest(root, opts.Mounts)
	if err != nil {
		return 1, err
	}

	if opts.Session == "" {
		opts.Session, err = protocol.RandomNonce()
		if err != nil {
			return 1, err
		}
	}

	request := protocol.RunRequest{
		WorkDir:  workdir,
		Command:  append([]string(nil), opts.Command...),
		Env:      collectEnv(opts.ForwardEnv, opts.ExtraEnv),
		Mounts:   append([]syncfs.MountSpec(nil), opts.Mounts...),
		Manifest: manifest.Entries,
		Session:  opts.Session,
		Project:  opts.Project,
		RootHint: filepath.Base(root),
	}
	dialer := net.Dialer{}

	raw, err := dialer.DialContext(ctx, "tcp", opts.Address)
	if err != nil {
		return 1, fmt.Errorf("dial host %s: %w", opts.Address, err)
	}
	defer raw.Close()

	conn := protocol.NewConn(raw)
	if err := authenticate(conn, opts.Token); err != nil {
		return 1, err
	}

	if err := conn.WriteJSON(protocol.MsgRunRequest, request); err != nil {
		return 1, err
	}

	head, err := conn.ReadHeader()
	if err != nil {
		return 1, err
	}

	if head.Type == protocol.MsgError {
		return 1, decodeServerError(head)
	}

	if head.Type != protocol.MsgNeedBlobs {
		return 1, fmt.Errorf("expected %s, got %s", protocol.MsgNeedBlobs, head.Type)
	}

	need, err := protocol.DecodeData[protocol.NeedBlobs](head)
	if err != nil {
		return 1, err
	}

	if err := sendMissingBlobs(conn, need.Hashes, manifest.BlobSources); err != nil {
		return 1, err
	}

	if err := conn.WriteJSON(protocol.MsgSyncComplete, nil); err != nil {
		return 1, err
	}

	head, err = conn.ReadHeader()
	if err != nil {
		return 1, err
	}

	if head.Type == protocol.MsgError {
		return 1, decodeServerError(head)
	}

	if head.Type != protocol.MsgWorkspaceReady {
		return 1, fmt.Errorf("expected %s, got %s", protocol.MsgWorkspaceReady, head.Type)
	}

	stdinErrCh := make(chan error, 1)

	go func() { stdinErrCh <- sendStdin(conn, opts.Stdin, opts.ForwardStdin) }()

	exitCode := 0

	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return exitCode, err
		}

		switch head.Type {
		case protocol.MsgError:
			return exitCode, decodeServerError(head)
		case protocol.MsgExecOutput:
			info, err := protocol.DecodeData[protocol.OutputInfo](head)
			if err != nil {
				return exitCode, err
			}

			dst := opts.Stdout
			if info.Stream == "stderr" {
				dst = opts.Stderr
			}

			if _, err := io.Copy(dst, conn.PayloadReader(head)); err != nil {
				return exitCode, err
			}
		case protocol.MsgExecExit:
			info, err := protocol.DecodeData[protocol.ExitInfo](head)
			if err != nil {
				return exitCode, err
			}

			exitCode = info.Code
		case protocol.MsgChangeSet:
			changes, err := protocol.DecodeData[protocol.ChangeSet](head)
			if err != nil {
				return exitCode, err
			}

			if err := syncfs.DeletePaths(root, changes.Deleted); err != nil {
				return exitCode, err
			}

			nonFiles := make([]syncfs.Entry, 0, len(changes.Entries))
			for _, entry := range changes.Entries {
				if entry.Kind != syncfs.KindFile {
					nonFiles = append(nonFiles, entry)
				}
			}

			if err := syncfs.ApplyNonFileEntries(root, nonFiles); err != nil {
				return exitCode, err
			}
		case protocol.MsgChangeBlob:
			info, err := protocol.DecodeData[protocol.BlobInfo](head)
			if err != nil {
				return exitCode, err
			}

			entry := syncfs.Entry{
				Path: info.Path,
				Kind: syncfs.KindFile,
				Hash: info.Hash,
				Size: head.PayloadLen,
				Mode: info.Mode,
			}
			if err := syncfs.WriteFile(root, entry, conn.PayloadReader(head)); err != nil {
				return exitCode, err
			}
		case protocol.MsgChangesDone:
			if err := <-stdinErrCh; err != nil {
				return exitCode, err
			}

			return exitCode, nil
		default:
			if err := conn.DiscardPayload(head); err != nil {
				return exitCode, err
			}

			return exitCode, fmt.Errorf("unexpected frame %s", head.Type)
		}
	}
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
			f.Close()
			return fmt.Errorf("stat blob source %s: %w", srcPath, err)
		}

		if err := conn.WriteFrom(
			protocol.MsgBlob,
			protocol.BlobInfo{Hash: hash, Size: info.Size()},
			f,
			info.Size(),
		); err != nil {
			f.Close()
			return err
		}

		_ = f.Close()
	}

	return nil
}

func sendStdin(conn *protocol.Conn, src io.Reader, enabled bool) error {
	if !enabled || src == nil {
		return conn.WriteJSON(protocol.MsgStdinClose, nil)
	}

	buf := make([]byte, 32*1024)
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
