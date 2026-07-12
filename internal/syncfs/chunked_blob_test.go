package syncfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestPlanBlobChunksSplitsLargeBlob(t *testing.T) {
	hash := sha256Hex([]byte("abcdefghij"))

	chunks, err := PlanBlobChunks([]BlobDescriptor{{Hash: hash, Size: 10}}, 4)
	if err != nil {
		t.Fatal(err)
	}

	if len(chunks) != 3 {
		t.Fatalf("chunks=%d want 3", len(chunks))
	}

	offsets := []int64{chunks[0].Offset, chunks[1].Offset, chunks[2].Offset}
	if offsets[0] != 0 || offsets[1] != 4 || offsets[2] != 8 {
		t.Fatalf("offsets=%v want [0 4 8]", offsets)
	}

	got, err := BlobChunkPayloadLen(chunks[2], 4)
	if err != nil {
		t.Fatal(err)
	}

	if got != 2 {
		t.Fatalf("last chunk len=%d want 2", got)
	}
}

func TestPlanBlobChunksRejectsUnboundedPlan(t *testing.T) {
	hash := sha256Hex([]byte("large"))
	if _, err := PlanBlobChunks(
		[]BlobDescriptor{{Hash: hash, Size: math.MaxInt64}},
		1,
	); err == nil {
		t.Fatal("PlanBlobChunks() accepted unbounded plan")
	}
}

func TestChunkedBlobReceiverRejectsDuplicateDescriptors(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	hash := sha256Hex([]byte("content"))

	if _, err := NewChunkedBlobReceiver(
		store,
		[]BlobDescriptor{
			{Hash: hash, Size: 7},
			{Hash: hash, Size: 7},
		},
		4,
	); err == nil {
		t.Fatal("NewChunkedBlobReceiver() accepted duplicate descriptors")
	}
}

func TestChunkedBlobReceiverStoresVerifiedBlob(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	content := []byte("abcdefghij")
	hash := sha256Hex(content)

	receiver, err := NewChunkedBlobReceiver(
		store,
		[]BlobDescriptor{{Hash: hash, Size: int64(len(content))}},
		4,
	)
	if err != nil {
		t.Fatal(err)
	}

	chunks, err := PlanBlobChunks([]BlobDescriptor{{Hash: hash, Size: int64(len(content))}}, 4)
	if err != nil {
		t.Fatal(err)
	}

	for _, chunk := range chunks {
		start := chunk.Offset

		payloadLen, err := BlobChunkPayloadLen(chunk, 4)
		if err != nil {
			t.Fatal(err)
		}

		end := start + payloadLen
		if err := receiver.ReceiveChunk(
			chunk,
			bytes.NewReader(content[int(start):int(end)]),
			end-start,
		); err != nil {
			t.Fatal(err)
		}
	}

	if err := receiver.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(mustBlobPath(t, store, hash))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, content) {
		t.Fatalf("stored content=%q want %q", got, content)
	}
}

func TestChunkedBlobReceiverAcceptsDuplicateChunk(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	content := []byte("abcdefgh")
	hash := sha256Hex(content)

	receiver, err := NewChunkedBlobReceiver(
		store,
		[]BlobDescriptor{{Hash: hash, Size: int64(len(content))}},
		4,
	)
	if err != nil {
		t.Fatal(err)
	}

	chunk := BlobChunkInfo{Hash: hash, Size: int64(len(content))}
	if err := receiver.ReceiveChunk(chunk, bytes.NewReader(content[:4]), 4); err != nil {
		t.Fatal(err)
	}

	err = receiver.ReceiveChunk(chunk, bytes.NewReader(content[:4]), 4)
	if err != nil {
		t.Fatal(err)
	}

	second := BlobChunkInfo{Hash: hash, Size: int64(len(content)), Offset: 4}
	if err := receiver.ReceiveChunk(second, bytes.NewReader(content[4:]), 4); err != nil {
		t.Fatal(err)
	}

	if err := receiver.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	err = receiver.ReceiveChunk(chunk, bytes.NewReader(content[:4]), 4)
	if err != nil {
		t.Fatal(err)
	}
}

func TestChunkedBlobReceiverCanRetryAfterChunkReadError(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	content := []byte("abcdefgh")
	hash := sha256Hex(content)

	receiver, err := NewChunkedBlobReceiver(
		store,
		[]BlobDescriptor{{Hash: hash, Size: int64(len(content))}},
		4,
	)
	if err != nil {
		t.Fatal(err)
	}

	chunk := BlobChunkInfo{Hash: hash, Size: int64(len(content))}

	err = receiver.ReceiveChunk(chunk, readerFunc(func(p []byte) (int, error) {
		copy(p, content[:2])
		return 2, os.ErrDeadlineExceeded
	}), 4)
	if !IsChunkReadError(err) {
		t.Fatalf("err=%v want chunk read error", err)
	}

	if err := receiver.ReceiveChunk(chunk, bytes.NewReader(content[:4]), 4); err != nil {
		t.Fatal(err)
	}
}

func TestChunkedBlobReceiverRejectsBadLengthBeforeRead(t *testing.T) {
	store := NewBlobStore(t.TempDir())
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	content := []byte("abcdefgh")
	hash := sha256Hex(content)

	receiver, err := NewChunkedBlobReceiver(
		store,
		[]BlobDescriptor{{Hash: hash, Size: int64(len(content))}},
		4,
	)
	if err != nil {
		t.Fatal(err)
	}

	reader := readerFunc(func([]byte) (int, error) {
		t.Fatal("reader was consumed before chunk length validation")

		return 0, errors.New("unexpected read")
	})

	err = receiver.ReceiveChunk(
		BlobChunkInfo{Hash: hash, Size: int64(len(content))},
		reader,
		1<<62,
	)
	if err == nil {
		t.Fatal("expected chunk length error")
	}
}

func TestChunkedBlobReceiverRemovesBadHashTemp(t *testing.T) {
	root := t.TempDir()

	store := NewBlobStore(root)
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	content := []byte("actual")
	wantHash := sha256Hex([]byte("expected"))

	receiver, err := NewChunkedBlobReceiver(
		store,
		[]BlobDescriptor{{Hash: wantHash, Size: int64(len(content))}},
		8,
	)
	if err != nil {
		t.Fatal(err)
	}

	err = receiver.ReceiveChunk(
		BlobChunkInfo{Hash: wantHash, Size: int64(len(content))},
		bytes.NewReader(content),
		int64(len(content)),
	)
	if err == nil {
		t.Fatal("expected hash mismatch")
	}

	if _, statErr := os.Stat(
		mustBlobPath(t, store, wantHash),
	); !errors.Is(
		statErr,
		os.ErrNotExist,
	) {
		t.Fatalf("bad blob path stat err=%v want not exist", statErr)
	}

	matches, globErr := filepath.Glob(filepath.Join(root, "*", "*.tmp-*"))
	if globErr != nil {
		t.Fatal(globErr)
	}

	if len(matches) != 0 {
		t.Fatalf("left temp files: %v", matches)
	}
}

func sha256Hex(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) {
	return f(p)
}
