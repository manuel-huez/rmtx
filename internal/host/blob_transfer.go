package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/syncfs"
)

type blobTransferSession struct {
	contextID string
	session   string
	token     string
	direction string
	chunkSize int64
	parallel  int

	receiver *syncfs.ChunkedBlobReceiver
	jobs     <-chan blobSendJob
	total    int
	sent     int

	mu      sync.Mutex
	err     error
	doneSet bool
	done    chan struct{}
}

type blobSendJob struct {
	info protocol.BlobChunkInfo
	path string
}

func newBlobReceiveSession(
	contextID,
	session,
	token string,
	store *syncfs.BlobStore,
	blobs []protocol.BlobDescriptor,
	chunkSize int64,
	parallel int,
) (*blobTransferSession, error) {
	receiver, err := syncfs.NewChunkedBlobReceiver(store, blobs, chunkSize)
	if err != nil {
		return nil, err
	}

	transfer := &blobTransferSession{
		contextID: contextID,
		session:   session,
		token:     token,
		direction: protocol.BlobTransferUpload,
		chunkSize: chunkSize,
		parallel:  parallel,
		receiver:  receiver,
		done:      make(chan struct{}),
	}

	go func() {
		if err := receiver.Wait(context.Background()); err != nil {
			_ = transfer.fail(err)
			return
		}
		transfer.complete()
	}()

	return transfer, nil
}

func newBlobSendSession(
	contextID,
	session,
	token string,
	jobs <-chan blobSendJob,
	total int,
	chunkSize int64,
	parallel int,
) *blobTransferSession {
	transfer := &blobTransferSession{
		contextID: contextID,
		session:   session,
		token:     token,
		direction: protocol.BlobTransferDownload,
		chunkSize: chunkSize,
		parallel:  parallel,
		jobs:      jobs,
		total:     total,
		done:      make(chan struct{}),
	}
	if total == 0 {
		transfer.doneSet = true
		close(transfer.done)
	}

	return transfer
}

func (s *blobTransferSession) validate(req protocol.BlobTransferRequest) error {
	if s == nil {
		return errors.New("blob transfer session not found")
	}
	if s.contextID != req.ContextID || s.session != req.Session || s.token != req.Token {
		return errors.New("blob transfer session mismatch")
	}
	if s.direction != req.Direction {
		return errors.New("blob transfer direction mismatch")
	}
	return nil
}

func (s *blobTransferSession) complete() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.doneSet {
		s.doneSet = true
		close(s.done)
	}
}

func (s *blobTransferSession) completeChunk() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sent++
	if s.sent >= s.total && !s.doneSet {
		s.doneSet = true
		close(s.done)
	}
}

func (s *blobTransferSession) fail(err error) error {
	if err == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err == nil {
		s.err = err
		if s.receiver != nil {
			_ = s.receiver.Fail(err)
		}
	}
	if !s.doneSet {
		s.doneSet = true
		close(s.done)
	}

	return s.err
}

func (s *blobTransferSession) wait(ctx context.Context) error {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.err
	case <-ctx.Done():
		return s.fail(ctx.Err())
	}
}

func (s *Server) registerBlobTransferSession(transfer *blobTransferSession) {
	s.blobTransfersMu.Lock()
	defer s.blobTransfersMu.Unlock()

	s.blobTransfers[transfer.token] = transfer
}

func (s *Server) unregisterBlobTransferSession(token string) {
	s.blobTransfersMu.Lock()
	defer s.blobTransfersMu.Unlock()

	delete(s.blobTransfers, token)
}

func (s *Server) lookupBlobTransferSession(
	req protocol.BlobTransferRequest,
) (*blobTransferSession, error) {
	s.blobTransfersMu.Lock()
	defer s.blobTransfersMu.Unlock()

	transfer := s.blobTransfers[req.Token]
	if err := transfer.validate(req); err != nil {
		return nil, err
	}

	return transfer, nil
}

func (s *Server) handleBlobTransferRequest(
	ctx context.Context,
	conn *protocol.Conn,
	req protocol.BlobTransferRequest,
) error {
	transfer, err := s.lookupBlobTransferSession(req)
	if err != nil {
		return err
	}

	switch req.Direction {
	case protocol.BlobTransferUpload:
		return s.receiveBlobTransfer(conn, transfer)
	case protocol.BlobTransferDownload:
		return s.sendBlobTransfer(ctx, conn, transfer)
	default:
		return fmt.Errorf("unsupported blob transfer direction: %s", req.Direction)
	}
}

func (s *Server) receiveBlobTransfer(
	conn *protocol.Conn,
	transfer *blobTransferSession,
) error {
	for {
		head, err := conn.ReadHeader()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return transfer.fail(fmt.Errorf("read blob transfer frame: %w", err))
		}

		switch head.Type {
		case protocol.MsgHeartbeat:
			if err := conn.DiscardPayload(head); err != nil {
				return transfer.fail(err)
			}
		case protocol.MsgBlobChunk:
			info, err := protocol.DecodeData[protocol.BlobChunkInfo](head)
			if err != nil {
				return transfer.fail(err)
			}
			if err := transfer.receiver.ReceiveChunk(info, conn.PayloadReader(head), head.PayloadLen); err != nil {
				return transfer.fail(err)
			}
		case protocol.MsgSyncComplete:
			return nil
		default:
			if err := conn.DiscardPayload(head); err != nil {
				return transfer.fail(err)
			}

			return transfer.fail(fmt.Errorf("unexpected blob transfer frame: %s", head.Type))
		}
	}
}

func (s *Server) sendBlobTransfer(
	ctx context.Context,
	conn *protocol.Conn,
	transfer *blobTransferSession,
) error {
	for job := range transfer.jobs {
		if err := ctx.Err(); err != nil {
			return transfer.fail(err)
		}

		if err := sendBlobChunk(conn, job, transfer.chunkSize); err != nil {
			return transfer.fail(err)
		}
		transfer.completeChunk()
	}

	if err := conn.WriteJSON(protocol.MsgSyncComplete, nil); err != nil {
		return transfer.fail(err)
	}

	return nil
}

func sendBlobChunk(conn *protocol.Conn, job blobSendJob, chunkSize int64) error {
	payloadLen := protocol.BlobChunkPayloadLen(job.info, chunkSize)
	file, err := os.Open(job.path)
	if err != nil {
		return fmt.Errorf("open blob source %s: %w", job.path, err)
	}
	defer func() { _ = file.Close() }()

	reader := io.NewSectionReader(file, job.info.Offset, payloadLen)
	return conn.WriteFrom(protocol.MsgBlobChunk, job.info, reader, payloadLen)
}
