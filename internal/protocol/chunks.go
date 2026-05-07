package protocol

import "github.com/manuel-huez/rmtx/internal/syncfs"

const DefaultBlobChunkSize = syncfs.DefaultBlobChunkSize

func PlanBlobChunks(blobs []BlobDescriptor, chunkSize int64) []BlobChunkInfo {
	return syncfs.PlanBlobChunks(blobs, chunkSize)
}

func BlobChunkCount(size, chunkSize int64) int {
	return syncfs.BlobChunkCount(size, chunkSize)
}

func BlobChunkPayloadLen(info BlobChunkInfo, chunkSize int64) int64 {
	return syncfs.BlobChunkPayloadLen(info, chunkSize)
}
