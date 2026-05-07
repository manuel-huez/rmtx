package syncfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPlanBlobChunksSplitsLargeBlob(t *testing.T) {
	chunks := PlanBlobChunks([]BlobDescriptor{{Hash: "hash", Size: 10}}, 4)

	if len(chunks) != 3 {
		t.Fatalf("chunks=%d want 3", len(chunks))
	}

	offsets := []int64{chunks[0].Offset, chunks[1].Offset, chunks[2].Offset}
	if offsets[0] != 0 || offsets[1] != 4 || offsets[2] != 8 {
		t.Fatalf("offsets=%v want [0 4 8]", offsets)
	}

	if got := BlobChunkPayloadLen(chunks[2], 4); got != 2 {
		t.Fatalf("last chunk len=%d want 2", got)
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

	for _, chunk := range PlanBlobChunks([]BlobDescriptor{{Hash: hash, Size: int64(len(content))}}, 4) {
		start := chunk.Offset
		end := start + BlobChunkPayloadLen(chunk, 4)
		if err := receiver.ReceiveChunk(chunk, bytes.NewReader(content[int(start):int(end)]), end-start); err != nil {
			t.Fatal(err)
		}
	}

	if err := receiver.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(store.Path(hash))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("stored content=%q want %q", got, content)
	}
}

func TestChunkedBlobReceiverRejectsDuplicateChunk(t *testing.T) {
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
	if err == nil {
		t.Fatal("expected duplicate chunk error")
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

	read := false
	reader := readerFunc(func([]byte) (int, error) {
		read = true
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
	if read {
		t.Fatal("reader was consumed before chunk length validation")
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

	if _, statErr := os.Stat(store.Path(wantHash)); !errors.Is(statErr, os.ErrNotExist) {
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
