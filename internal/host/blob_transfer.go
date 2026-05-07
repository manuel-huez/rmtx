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

	receiver  *syncfs.ChunkedBlobReceiver
	sendItems map[string]downloadBlobItem
	total     int
	sent      map[blobChunkKey]struct{}

	mu      sync.Mutex
	err     error
	doneSet bool
	done    chan struct{}
}

type blobSendJob struct {
	info protocol.BlobChunkInfo
	path string
}

type blobChunkKey struct {
	hash   string
	offset int64
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
	items map[string]downloadBlobItem,
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
		sendItems: items,
		total:     total,
		sent:      map[blobChunkKey]struct{}{},
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

func (s *blobTransferSession) completeChunk(info protocol.BlobChunkInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := blobChunkKey{hash: info.Hash, offset: info.Offset}
	if _, ok := s.sent[key]; !ok {
		s.sent[key] = struct{}{}
	}
	if len(s.sent) >= s.total && !s.doneSet {
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
		return s.sendBlobTransfer(ctx, conn, transfer, req)
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
			if protocol.IsDisconnectError(err) {
				return nil
			}

			return transfer.fail(fmt.Errorf("read blob transfer frame: %w", err))
		}

		done, err := s.handleReceiveBlobTransferFrame(conn, transfer, head)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func (s *Server) handleReceiveBlobTransferFrame(
	conn *protocol.Conn,
	transfer *blobTransferSession,
	head protocol.Header,
) (bool, error) {
	switch head.Type {
	case protocol.MsgHeartbeat:
		if err := conn.DiscardPayload(head); err != nil {
			if protocol.IsDisconnectError(err) {
				return true, nil
			}

			return false, transfer.fail(err)
		}

		return false, nil
	case protocol.MsgBlobChunk:
		return s.receiveBlobTransferChunk(conn, transfer, head)
	case protocol.MsgSyncComplete:
		return true, nil
	default:
		if err := conn.DiscardPayload(head); err != nil {
			if protocol.IsDisconnectError(err) {
				return true, nil
			}

			return false, transfer.fail(err)
		}

		return false, transfer.fail(fmt.Errorf("unexpected blob transfer frame: %s", head.Type))
	}
}

func (s *Server) receiveBlobTransferChunk(
	conn *protocol.Conn,
	transfer *blobTransferSession,
	head protocol.Header,
) (bool, error) {
	info, err := protocol.DecodeData[protocol.BlobChunkInfo](head)
	if err != nil {
		return false, transfer.fail(err)
	}
	err = transfer.receiver.ReceiveChunk(
		info,
		conn.PayloadReader(head),
		head.PayloadLen,
	)
	if err != nil {
		if syncfs.IsChunkReadError(err) || protocol.IsDisconnectError(err) {
			return true, nil
		}

		return false, transfer.fail(err)
	}

	return false, nil
}

func (s *Server) sendBlobTransfer(
	ctx context.Context,
	conn *protocol.Conn,
	transfer *blobTransferSession,
	req protocol.BlobTransferRequest,
) error {
	for {
		// Client requests one chunk at a time, so reconnect can retry without losing queued work.
		if err := s.sendRequestedBlobChunk(ctx, conn, transfer, req); err != nil {
			if protocol.IsDisconnectError(err) {
				return nil
			}
			return transfer.fail(err)
		}

		next, ok, err := readNextBlobTransferRequest(conn)
		if err != nil {
			if protocol.IsDisconnectError(err) {
				return nil
			}
			return transfer.fail(err)
		}
		if !ok {
			return nil
		}
		req = next
	}
}

func (s *Server) sendRequestedBlobChunk(
	ctx context.Context,
	conn *protocol.Conn,
	transfer *blobTransferSession,
	req protocol.BlobTransferRequest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := transfer.validate(req); err != nil {
		return err
	}
	if req.Chunk == nil {
		return errors.New("blob download chunk is required")
	}

	item := transfer.sendItems[req.Chunk.Hash]
	if item.hash == "" {
		return fmt.Errorf("requested unknown blob chunk %s", req.Chunk.Hash)
	}
	if err := validateSendBlobChunk(*req.Chunk, item, transfer.chunkSize); err != nil {
		return err
	}
	job := blobSendJob{info: *req.Chunk, path: item.path}
	if err := sendBlobChunk(conn, job, transfer.chunkSize); err != nil {
		return err
	}

	transfer.completeChunk(*req.Chunk)

	return nil
}

func validateSendBlobChunk(
	info protocol.BlobChunkInfo,
	item downloadBlobItem,
	chunkSize int64,
) error {
	if info.Size != item.size {
		return fmt.Errorf("blob %s size mismatch: %d != %d", info.Hash, info.Size, item.size)
	}
	if info.Offset < 0 || info.Offset > item.size {
		return fmt.Errorf("blob %s invalid chunk offset %d", info.Hash, info.Offset)
	}
	if item.size > 0 && info.Offset%chunkSize != 0 {
		return fmt.Errorf("blob %s unaligned chunk offset %d", info.Hash, info.Offset)
	}
	index := 0
	if item.size > 0 {
		index = int(info.Offset / chunkSize)
	}
	if index >= protocol.BlobChunkCount(item.size, chunkSize) {
		return fmt.Errorf("blob %s chunk index out of range %d", info.Hash, index)
	}

	return nil
}

func readNextBlobTransferRequest(conn *protocol.Conn) (protocol.BlobTransferRequest, bool, error) {
	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return protocol.BlobTransferRequest{}, false, err
		}

		switch head.Type {
		case protocol.MsgHeartbeat:
			if err := conn.DiscardPayload(head); err != nil {
				return protocol.BlobTransferRequest{}, false, err
			}
		case protocol.MsgBlobTransferRequest:
			req, err := protocol.DecodeData[protocol.BlobTransferRequest](head)
			if err != nil {
				return protocol.BlobTransferRequest{}, false, err
			}

			return req, true, nil
		case protocol.MsgSyncComplete:
			if err := conn.DiscardPayload(head); err != nil {
				return protocol.BlobTransferRequest{}, false, err
			}

			return protocol.BlobTransferRequest{}, false, nil
		default:
			if err := conn.DiscardPayload(head); err != nil {
				return protocol.BlobTransferRequest{}, false, err
			}

			return protocol.BlobTransferRequest{}, false, fmt.Errorf("unexpected blob transfer frame: %s", head.Type)
		}
	}
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
