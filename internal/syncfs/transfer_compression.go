package syncfs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
)

type CompressionCandidate struct {
	Path string
	Size int64
}

const (
	compressionMinTransferBytes = 1 << 20
	compressionMinUnknownBytes  = 8 << 20
	compressionSampleBytes      = 256 << 10
	compressionMinLikelyPercent = 25
	compressionMaxSamplePercent = 90
)

var likelyCompressibleExt = map[string]struct{}{
	".c":       {},
	".cpp":     {},
	".css":     {},
	".csv":     {},
	".go":      {},
	".h":       {},
	".hpp":     {},
	".html":    {},
	".ini":     {},
	".java":    {},
	".js":      {},
	".json":    {},
	".jsonl":   {},
	".jsx":     {},
	".log":     {},
	".md":      {},
	".parquet": {},
	".py":      {},
	".rs":      {},
	".sql":     {},
	".toml":    {},
	".ts":      {},
	".tsx":     {},
	".tsv":     {},
	".txt":     {},
	".xml":     {},
	".yaml":    {},
	".yml":     {},
}

var likelyCompressedExt = map[string]struct{}{
	".7z":   {},
	".avi":  {},
	".br":   {},
	".bz2":  {},
	".gz":   {},
	".jpeg": {},
	".jpg":  {},
	".mkv":  {},
	".mov":  {},
	".mp3":  {},
	".mp4":  {},
	".pdf":  {},
	".png":  {},
	".rar":  {},
	".wasm": {},
	".webm": {},
	".webp": {},
	".xz":   {},
	".zip":  {},
	".zst":  {},
}

func ShouldCompressTransfer(candidates []CompressionCandidate) bool {
	totalBytes := compressionTotalBytes(candidates)
	if totalBytes < compressionMinTransferBytes {
		return false
	}

	likelyBytes := int64(0)

	for _, candidate := range candidates {
		if candidate.Size <= 0 {
			continue
		}

		switch compressionClass(candidate) {
		case compressionLikely:
			likelyBytes += candidate.Size
		case compressionUnknown:
			if candidate.Size >= compressionMinUnknownBytes &&
				sampleCompressesWell(candidate.Path, candidate.Size) {
				likelyBytes += candidate.Size
			}
		case compressionSkip:
			continue
		}
	}

	return likelyBytes > 0 &&
		likelyBytes*100 >= totalBytes*compressionMinLikelyPercent
}

func compressionTotalBytes(candidates []CompressionCandidate) int64 {
	var total int64

	for _, candidate := range candidates {
		if candidate.Size > 0 {
			total += candidate.Size
		}
	}

	return total
}

type compressionKind int

const (
	compressionUnknown compressionKind = iota
	compressionLikely
	compressionSkip
)

func compressionClass(candidate CompressionCandidate) compressionKind {
	ext := strings.ToLower(filepath.Ext(candidate.Path))

	if _, ok := likelyCompressedExt[ext]; ok {
		return compressionSkip
	}

	if _, ok := likelyCompressibleExt[ext]; ok {
		return compressionLikely
	}

	return compressionUnknown
}

func sampleCompressesWell(path string, size int64) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}

	defer func() { _ = f.Close() }()

	limit := min(size, int64(compressionSampleBytes))

	if limit <= 0 {
		return false
	}

	sample := make([]byte, limit)

	n, err := io.ReadFull(f, sample)
	if n <= 0 {
		return false
	}

	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return false
	}

	var compressed bytes.Buffer

	encoder, err := zstd.NewWriter(
		&compressed,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
	)
	if err != nil {
		return false
	}

	if _, err := encoder.Write(sample[:n]); err != nil {
		_ = encoder.Close()
		return false
	}

	if err := encoder.Close(); err != nil {
		return false
	}

	return int64(compressed.Len())*100 <= int64(n)*compressionMaxSamplePercent
}
