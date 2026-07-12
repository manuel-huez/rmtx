package syncfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/manuel-huez/rmtx/internal/pathutil"
)

const (
	DefaultBlobChunkSize int64 = 4 << 20
	MaxBlobChunkSize     int64 = 64 << 20
	MaxBlobChunks              = 1 << 20
)

type BlobDescriptor struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

type BlobChunkInfo struct {
	Hash   string `json:"hash"`
	Size   int64  `json:"size"`
	Offset int64  `json:"offset"`
}

// ChunkReadError wraps a transport failure while reading a chunk payload.
type ChunkReadError struct {
	Hash string
	Err  error
}

func (e *ChunkReadError) Error() string {
	return fmt.Sprintf("read blob chunk %s: %v", e.Hash, e.Err)
}

func (e *ChunkReadError) Unwrap() error {
	return e.Err
}

// IsChunkReadError reports transport read failures that leave receiver state retryable.
func IsChunkReadError(err error) bool {
	var chunkErr *ChunkReadError
	return errors.As(err, &chunkErr)
}

func PlanBlobChunks(blobs []BlobDescriptor, chunkSize int64) ([]BlobChunkInfo, error) {
	chunkSize, err := normalizeBlobChunkSize(chunkSize)
	if err != nil {
		return nil, err
	}

	total := 0

	seen := make(map[string]struct{}, len(blobs))
	for _, blob := range blobs {
		if err := validateBlobHash(blob.Hash); err != nil {
			return nil, err
		}

		if _, exists := seen[blob.Hash]; exists {
			return nil, fmt.Errorf("duplicate blob descriptor %s", blob.Hash)
		}

		seen[blob.Hash] = struct{}{}

		count, err := BlobChunkCount(blob.Size, chunkSize)
		if err != nil {
			return nil, fmt.Errorf("plan blob %s: %w", blob.Hash, err)
		}

		if count > MaxBlobChunks-total {
			return nil, fmt.Errorf("blob chunk plan exceeds %d chunks", MaxBlobChunks)
		}

		total += count
	}

	chunks := make([]BlobChunkInfo, 0, total)

	for _, blob := range blobs {
		if blob.Size == 0 {
			chunks = append(chunks, BlobChunkInfo{Hash: blob.Hash})
			continue
		}

		for offset := int64(0); offset < blob.Size; offset += chunkSize {
			chunks = append(chunks, BlobChunkInfo{
				Hash:   blob.Hash,
				Size:   blob.Size,
				Offset: offset,
			})
		}
	}

	return chunks, nil
}

func BlobChunkCount(size, chunkSize int64) (int, error) {
	if size < 0 {
		return 0, fmt.Errorf("negative blob size %d", size)
	}

	chunkSize, err := normalizeBlobChunkSize(chunkSize)
	if err != nil {
		return 0, err
	}

	if size == 0 {
		return 1, nil
	}

	count := 1 + (size-1)/chunkSize
	if count > MaxBlobChunks {
		return 0, fmt.Errorf("blob requires %d chunks; maximum is %d", count, MaxBlobChunks)
	}

	return int(count), nil
}

func BlobChunkPayloadLen(info BlobChunkInfo, chunkSize int64) (int64, error) {
	chunkSize, err := normalizeBlobChunkSize(chunkSize)
	if err != nil {
		return 0, err
	}

	if info.Size < 0 {
		return 0, fmt.Errorf("blob %s has negative size", info.Hash)
	}

	if info.Offset < 0 || info.Offset > info.Size {
		return 0, fmt.Errorf("blob %s invalid chunk offset %d", info.Hash, info.Offset)
	}

	if info.Size == 0 {
		if info.Offset != 0 {
			return 0, fmt.Errorf("blob %s invalid empty chunk offset %d", info.Hash, info.Offset)
		}

		return 0, nil
	}

	if info.Offset%chunkSize != 0 {
		return 0, fmt.Errorf("blob %s unaligned chunk offset %d", info.Hash, info.Offset)
	}

	remaining := info.Size - info.Offset
	if remaining < chunkSize {
		return remaining, nil
	}

	return chunkSize, nil
}

func normalizeBlobChunkSize(chunkSize int64) (int64, error) {
	if chunkSize <= 0 {
		return DefaultBlobChunkSize, nil
	}

	if chunkSize > MaxBlobChunkSize {
		return 0, fmt.Errorf("blob chunk size %d exceeds maximum %d", chunkSize, MaxBlobChunkSize)
	}

	return chunkSize, nil
}

type ChunkedBlobReceiver struct {
	store     *BlobStore
	chunkSize int64

	mu      sync.Mutex
	blobs   map[string]BlobDescriptor
	writers map[string]*chunkedBlobWriter
	pending int
	err     error
	doneSet bool
	done    chan struct{}
}

func NewChunkedBlobReceiver(
	store *BlobStore,
	blobs []BlobDescriptor,
	chunkSize int64,
) (*ChunkedBlobReceiver, error) {
	chunkSize, err := normalizeBlobChunkSize(chunkSize)
	if err != nil {
		return nil, err
	}

	receiver := &ChunkedBlobReceiver{
		store:     store,
		chunkSize: chunkSize,
		blobs:     make(map[string]BlobDescriptor, len(blobs)),
		writers:   make(map[string]*chunkedBlobWriter, len(blobs)),
		done:      make(chan struct{}),
	}

	for _, blob := range blobs {
		if err := validateBlobHash(blob.Hash); err != nil {
			receiver.abort()
			return nil, err
		}

		if blob.Size < 0 {
			receiver.abort()
			return nil, fmt.Errorf("blob %s has negative size", blob.Hash)
		}

		if _, exists := receiver.blobs[blob.Hash]; exists {
			receiver.abort()
			return nil, fmt.Errorf("duplicate blob descriptor %s", blob.Hash)
		}

		receiver.blobs[blob.Hash] = blob
		if store.Has(blob.Hash, blob.Size) {
			continue
		}

		writer, err := newChunkedBlobWriter(store, blob, chunkSize)
		if err != nil {
			receiver.abort()
			return nil, err
		}

		receiver.writers[blob.Hash] = writer
		receiver.pending++
	}

	if receiver.pending == 0 {
		receiver.doneSet = true
		close(receiver.done)
	}

	return receiver, nil
}

func (r *ChunkedBlobReceiver) ReceiveChunk(
	info BlobChunkInfo,
	src io.Reader,
	payloadLen int64,
) error {
	if payloadLen < 0 {
		return r.fail(fmt.Errorf("blob %s has negative chunk length", info.Hash))
	}

	r.mu.Lock()
	if r.err != nil {
		err := r.err
		r.mu.Unlock()

		return err
	}

	blob, known := r.blobs[info.Hash]

	writer := r.writers[info.Hash]
	if writer == nil {
		// Retries can replay chunks after commit; validate metadata, drain payload, keep state done.
		if known && r.store.Has(info.Hash, blob.Size) {
			err := validateBlobChunkShape(
				info,
				blob.Hash,
				blob.Size,
				r.chunkSize,
				payloadLen,
			)
			if err != nil {
				r.mu.Unlock()
				return r.fail(err)
			}
			r.mu.Unlock()

			return discardChunkPayload(info, src, payloadLen)
		}
		r.mu.Unlock()

		return r.fail(fmt.Errorf("unexpected blob chunk %s", info.Hash))
	}
	r.mu.Unlock()

	if err := writer.validateChunk(info, payloadLen); err != nil {
		return r.fail(err)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(src, payload); err != nil {
		return &ChunkReadError{Hash: info.Hash, Err: err}
	}

	complete, err := writer.writeChunk(info, payload)
	if err != nil {
		return r.fail(err)
	}

	if !complete {
		return nil
	}

	if err := writer.commit(); err != nil {
		return r.fail(err)
	}

	r.mu.Lock()
	delete(r.writers, info.Hash)
	r.pending--
	r.closeDoneLocked()
	r.mu.Unlock()

	return nil
}

func discardChunkPayload(info BlobChunkInfo, src io.Reader, payloadLen int64) error {
	if _, err := io.CopyN(io.Discard, src, payloadLen); err != nil {
		return &ChunkReadError{Hash: info.Hash, Err: err}
	}

	return nil
}

func (r *ChunkedBlobReceiver) Wait(ctx context.Context) error {
	select {
	case <-r.done:
		r.mu.Lock()
		defer r.mu.Unlock()

		return r.err
	case <-ctx.Done():
		return r.fail(ctx.Err())
	}
}

func (r *ChunkedBlobReceiver) Fail(err error) error {
	return r.fail(err)
}

func (r *ChunkedBlobReceiver) fail(err error) error {
	if err == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.err == nil {
		r.err = err
		r.abortLocked()
		r.closeDoneLocked()
	}

	return r.err
}

func (r *ChunkedBlobReceiver) abort() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.abortLocked()
}

func (r *ChunkedBlobReceiver) abortLocked() {
	for hash, writer := range r.writers {
		writer.abort()
		delete(r.writers, hash)
	}
}

func (r *ChunkedBlobReceiver) closeDoneLocked() {
	if !r.doneSet && (r.err != nil || r.pending == 0) {
		r.doneSet = true
		close(r.done)
	}
}

type chunkedBlobWriter struct {
	store     *BlobStore
	hash      string
	size      int64
	chunkSize int64
	path      string
	tmp       string
	file      *os.File

	mu        sync.Mutex
	completed []bool
	done      int
	committed bool
}

func newChunkedBlobWriter(
	store *BlobStore,
	blob BlobDescriptor,
	chunkSize int64,
) (*chunkedBlobWriter, error) {
	path, err := store.Path(blob.Hash)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(path), blobDefaultDirPerm); err != nil {
		return nil, fmt.Errorf("create blob dir: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("create blob temp: %w", err)
	}

	count, err := BlobChunkCount(blob.Size, chunkSize)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())

		return nil, err
	}

	writer := &chunkedBlobWriter{
		store:     store,
		hash:      blob.Hash,
		size:      blob.Size,
		chunkSize: chunkSize,
		path:      path,
		tmp:       tmp.Name(),
		file:      tmp,
		completed: make([]bool, count),
	}

	if blob.Size > 0 {
		if err := tmp.Truncate(blob.Size); err != nil {
			writer.abort()
			return nil, fmt.Errorf("size blob temp: %w", err)
		}
	}

	return writer, nil
}

func (w *chunkedBlobWriter) writeChunk(info BlobChunkInfo, payload []byte) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := validateBlobChunkShape(
		info,
		w.hash,
		w.size,
		w.chunkSize,
		int64(len(payload)),
	); err != nil {
		return false, err
	}

	index := 0
	if w.size > 0 {
		index = int(info.Offset / w.chunkSize)
	}
	// Lost acks can replay chunks; completed offsets are idempotent.
	if w.committed || w.completed[index] {
		return false, nil
	}

	if len(payload) > 0 {
		if _, err := w.file.WriteAt(payload, info.Offset); err != nil {
			return false, fmt.Errorf("write blob %s chunk: %w", w.hash, err)
		}
	}

	w.completed[index] = true
	w.done++

	return w.done == len(w.completed), nil
}

func (w *chunkedBlobWriter) validateChunk(info BlobChunkInfo, payloadLen int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return validateBlobChunkShape(info, w.hash, w.size, w.chunkSize, payloadLen)
}

func validateBlobChunkShape(
	info BlobChunkInfo,
	hash string,
	size int64,
	chunkSize int64,
	payloadLen int64,
) error {
	if info.Hash != hash {
		return fmt.Errorf("blob chunk hash mismatch: %s != %s", info.Hash, hash)
	}

	if info.Size != size {
		return fmt.Errorf("blob %s size mismatch: %d != %d", hash, info.Size, size)
	}

	wantLen, err := BlobChunkPayloadLen(info, chunkSize)
	if err != nil {
		return err
	}

	if payloadLen != wantLen {
		return fmt.Errorf("blob %s chunk length mismatch: %d != %d", hash, payloadLen, wantLen)
	}

	index := 0
	if size > 0 {
		index = int(info.Offset / chunkSize)
	}

	count, err := BlobChunkCount(size, chunkSize)
	if err != nil {
		return err
	}

	if index >= count {
		return fmt.Errorf("blob %s chunk index out of range %d", hash, index)
	}

	return nil
}

func (w *chunkedBlobWriter) commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.committed {
		return nil
	}

	w.committed = true

	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		_ = os.Remove(w.tmp)

		return fmt.Errorf("sync blob temp: %w", err)
	}

	if err := w.file.Close(); err != nil {
		_ = os.Remove(w.tmp)
		return fmt.Errorf("close blob temp: %w", err)
	}

	actual, err := HashFile(w.tmp)
	if err != nil {
		_ = os.Remove(w.tmp)
		return err
	}

	if actual != w.hash {
		_ = os.Remove(w.tmp)
		return fmt.Errorf("blob hash mismatch: got %s want %s", actual, w.hash)
	}

	if w.store.Has(w.hash, w.size) {
		_ = os.Remove(w.tmp)
		return nil
	}

	if err := pathutil.ReplaceFile(w.tmp, w.path); err != nil {
		_ = os.Remove(w.tmp)
		return fmt.Errorf("move blob file: %w", err)
	}

	w.store.remember(w.hash, w.size, w.path)

	return nil
}

func (w *chunkedBlobWriter) abort() {
	if w.file != nil {
		_ = w.file.Close()
	}

	if w.tmp != "" {
		_ = os.Remove(w.tmp)
	}
}
