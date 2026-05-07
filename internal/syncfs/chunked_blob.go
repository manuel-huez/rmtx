package syncfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const DefaultBlobChunkSize int64 = 4 << 20

type BlobDescriptor struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

type BlobChunkInfo struct {
	Hash   string `json:"hash"`
	Size   int64  `json:"size"`
	Offset int64  `json:"offset"`
}

func PlanBlobChunks(blobs []BlobDescriptor, chunkSize int64) []BlobChunkInfo {
	if chunkSize <= 0 {
		chunkSize = DefaultBlobChunkSize
	}

	total := 0
	for _, blob := range blobs {
		total += BlobChunkCount(blob.Size, chunkSize)
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

	return chunks
}

func BlobChunkCount(size, chunkSize int64) int {
	if size <= 0 {
		return 1
	}
	if chunkSize <= 0 {
		chunkSize = DefaultBlobChunkSize
	}

	return int((size + chunkSize - 1) / chunkSize)
}

func BlobChunkPayloadLen(info BlobChunkInfo, chunkSize int64) int64 {
	if info.Size == 0 {
		return 0
	}
	if chunkSize <= 0 {
		chunkSize = DefaultBlobChunkSize
	}

	remaining := info.Size - info.Offset
	if remaining < chunkSize {
		return remaining
	}

	return chunkSize
}

type ChunkedBlobReceiver struct {
	store     *BlobStore
	chunkSize int64

	mu      sync.Mutex
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
	if chunkSize <= 0 {
		chunkSize = DefaultBlobChunkSize
	}

	receiver := &ChunkedBlobReceiver{
		store:     store,
		chunkSize: chunkSize,
		writers:   make(map[string]*chunkedBlobWriter, len(blobs)),
		done:      make(chan struct{}),
	}

	for _, blob := range blobs {
		if blob.Hash == "" {
			return nil, fmt.Errorf("blob hash is required")
		}
		if blob.Size < 0 {
			return nil, fmt.Errorf("blob %s has negative size", blob.Hash)
		}
		if store.Has(blob.Hash) {
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

func (r *ChunkedBlobReceiver) ReceiveChunk(info BlobChunkInfo, src io.Reader, payloadLen int64) error {
	if payloadLen < 0 {
		return r.fail(fmt.Errorf("blob %s has negative chunk length", info.Hash))
	}

	r.mu.Lock()
	if r.err != nil {
		err := r.err
		r.mu.Unlock()
		return err
	}

	writer := r.writers[info.Hash]
	if writer == nil {
		r.mu.Unlock()
		return r.fail(fmt.Errorf("unexpected blob chunk %s", info.Hash))
	}
	r.mu.Unlock()

	if err := writer.validateChunk(info, payloadLen); err != nil {
		return r.fail(err)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(src, payload); err != nil {
		return r.fail(fmt.Errorf("read blob chunk %s: %w", info.Hash, err))
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
	path := store.Path(blob.Hash)
	if err := os.MkdirAll(filepath.Dir(path), blobDefaultDirPerm); err != nil {
		return nil, fmt.Errorf("create blob dir: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("create blob temp: %w", err)
	}

	writer := &chunkedBlobWriter{
		store:     store,
		hash:      blob.Hash,
		size:      blob.Size,
		chunkSize: chunkSize,
		path:      path,
		tmp:       tmp.Name(),
		file:      tmp,
		completed: make([]bool, BlobChunkCount(blob.Size, chunkSize)),
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

	if err := w.validateChunkLocked(info, int64(len(payload))); err != nil {
		return false, err
	}

	index := 0
	if w.size > 0 {
		index = int(info.Offset / w.chunkSize)
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

	return w.validateChunkLocked(info, payloadLen)
}

func (w *chunkedBlobWriter) validateChunkLocked(info BlobChunkInfo, payloadLen int64) error {
	if w.committed {
		return fmt.Errorf("blob %s already committed", w.hash)
	}
	if info.Hash != w.hash {
		return fmt.Errorf("blob chunk hash mismatch: %s != %s", info.Hash, w.hash)
	}
	if info.Size != w.size {
		return fmt.Errorf("blob %s size mismatch: %d != %d", w.hash, info.Size, w.size)
	}

	wantLen := BlobChunkPayloadLen(info, w.chunkSize)
	if payloadLen != wantLen {
		return fmt.Errorf("blob %s chunk length mismatch: %d != %d", w.hash, payloadLen, wantLen)
	}
	if info.Offset < 0 || info.Offset > w.size {
		return fmt.Errorf("blob %s invalid chunk offset %d", w.hash, info.Offset)
	}
	if w.size > 0 && info.Offset%w.chunkSize != 0 {
		return fmt.Errorf("blob %s unaligned chunk offset %d", w.hash, info.Offset)
	}

	index := 0
	if w.size > 0 {
		index = int(info.Offset / w.chunkSize)
	}
	if index >= len(w.completed) {
		return fmt.Errorf("blob %s chunk index out of range %d", w.hash, index)
	}
	if w.completed[index] {
		return fmt.Errorf("blob %s duplicate chunk offset %d", w.hash, info.Offset)
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

	if _, err := os.Stat(w.path); err == nil {
		_ = os.Remove(w.tmp)
		return nil
	}

	if err := os.Rename(w.tmp, w.path); err != nil {
		_ = os.Remove(w.tmp)
		return fmt.Errorf("move blob file: %w", err)
	}

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
