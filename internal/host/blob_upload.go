package host

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

type blobUploadSession struct {
	contextID string
	session   string
	token     string

	mu      sync.Mutex
	pending map[string]struct{}
	err     error
	doneSet bool
	done    chan struct{}
}

func newBlobUploadSession(contextID, session, token string, hashes []string) *blobUploadSession {
	pending := make(map[string]struct{}, len(hashes))
	for _, hash := range hashes {
		pending[hash] = struct{}{}
	}

	upload := &blobUploadSession{
		contextID: contextID,
		session:   session,
		token:     token,
		pending:   pending,
		done:      make(chan struct{}),
	}

	if len(pending) == 0 {
		upload.doneSet = true
		close(upload.done)
	}

	return upload
}

func (s *blobUploadSession) complete(hash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err != nil {
		return s.err
	}

	if _, ok := s.pending[hash]; !ok {
		return fmt.Errorf("unexpected blob hash %s", hash)
	}

	delete(s.pending, hash)

	if len(s.pending) == 0 {
		s.doneSet = true
		close(s.done)
	}

	return nil
}

func (s *blobUploadSession) fail(err error) error {
	if err == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err == nil && !s.doneSet {
		s.err = err
		s.doneSet = true
		close(s.done)
	}

	return err
}

func (s *blobUploadSession) wait() error {
	<-s.done

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.err
}

func (s *Server) registerBlobUploadSession(upload *blobUploadSession) {
	s.uploadsMu.Lock()
	defer s.uploadsMu.Unlock()

	s.uploads[upload.token] = upload
}

func (s *Server) unregisterBlobUploadSession(token string) {
	s.uploadsMu.Lock()
	defer s.uploadsMu.Unlock()

	delete(s.uploads, token)
}

func (s *Server) lookupBlobUploadSession(
	req protocol.BlobUploadRequest,
) (*blobUploadSession, error) {
	s.uploadsMu.Lock()
	defer s.uploadsMu.Unlock()

	upload := s.uploads[req.Token]
	if upload == nil {
		return nil, errors.New("blob upload session not found")
	}

	if upload.contextID != req.ContextID || upload.session != req.Session {
		return nil, errors.New("blob upload session mismatch")
	}

	return upload, nil
}

func (s *Server) handleBlobUploadRequest(
	conn *protocol.Conn,
	req protocol.BlobUploadRequest,
) error {
	upload, err := s.lookupBlobUploadSession(req)
	if err != nil {
		return err
	}

	closeReader, err := openBlobUploadReader(conn, req.Compression)
	if err != nil {
		return upload.fail(err)
	}

	defer closeReader()

	for {
		keepReading, err := s.receiveBlobUploadFrame(conn, upload)
		if err != nil {
			return upload.fail(err)
		}

		if !keepReading {
			return nil
		}
	}
}

func openBlobUploadReader(conn *protocol.Conn, compression string) (func(), error) {
	if compression == "" {
		return func() {}, nil
	}

	switch compression {
	case protocol.CompressionZstd:
		closeReader, err := conn.EnableZstdReader()
		if err != nil {
			return nil, err
		}

		return closeReader, nil
	default:
		return nil, fmt.Errorf("unsupported blob upload compression: %s", compression)
	}
}

func (s *Server) receiveBlobUploadFrame(
	conn *protocol.Conn,
	upload *blobUploadSession,
) (bool, error) {
	head, err := conn.ReadHeader()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}

		return false, fmt.Errorf("read blob upload frame: %w", err)
	}

	switch head.Type {
	case protocol.MsgHeartbeat:
		return true, conn.DiscardPayload(head)
	case protocol.MsgBlob:
		if err := s.receiveUploadBlob(conn, head, upload); err != nil {
			return false, err
		}
	case protocol.MsgSyncComplete:
		return false, nil
	default:
		if err := conn.DiscardPayload(head); err != nil {
			return false, err
		}

		return false, fmt.Errorf("unexpected blob upload frame: %s", head.Type)
	}

	return true, nil
}

func (s *Server) receiveUploadBlob(
	conn *protocol.Conn,
	head protocol.Header,
	upload *blobUploadSession,
) error {
	info, err := protocol.DecodeData[protocol.BlobInfo](head)
	if err != nil {
		return err
	}

	if info.Hash == "" {
		return errors.New("blob hash is required")
	}

	if err := s.blobStore.Store(info.Hash, head.PayloadLen, conn.PayloadReader(head)); err != nil {
		return err
	}

	return upload.complete(info.Hash)
}
