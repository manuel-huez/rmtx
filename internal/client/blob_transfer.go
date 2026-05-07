package client

import (
	"context"
	"fmt"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/syncfs"
)

const (
	blobTransferMaxAttempts = 5
	blobTransferRetryBase   = 200 * time.Millisecond
	blobTransferRetryMax    = 2 * time.Second
)

type blobTransferConn struct {
	conn *protocol.Conn
	stop func()
}

func dialBlobTransferConn(ctx context.Context, opts ExecOptions) (*blobTransferConn, error) {
	conn, err := dialTLS(
		ctx,
		opts.Address,
		opts.DiscoveryService,
		opts.Host.Fingerprint,
		opts.ClientCertPEM,
		opts.ClientKeyPEM,
	)
	if err != nil {
		return nil, err
	}

	return &blobTransferConn{
		conn: conn,
		stop: startConnectionLiveness(ctx, conn, false),
	}, nil
}

func closeBlobTransferConn(conn *blobTransferConn) {
	if conn == nil {
		return
	}
	if conn.stop != nil {
		conn.stop()
	}
	if conn.conn != nil {
		closeQuietly(conn.conn.Raw())
	}
}

func isRetryableBlobTransferError(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}

	return syncfs.IsChunkReadError(err) || protocol.IsDisconnectError(err)
}

func retryBlobTransferChunk(
	ctx context.Context,
	conn *blobTransferConn,
	direction string,
	hash string,
	offset int64,
	logger *runLogger,
	transfer func(*blobTransferConn) (*blobTransferConn, error),
) (*blobTransferConn, error) {
	var lastErr error

	for attempt := 1; attempt <= blobTransferMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return conn, err
		}

		nextConn, err := transfer(conn)
		if err == nil {
			return nextConn, nil
		}

		lastErr = err
		// Retry always starts with a fresh connection because frame boundaries are lost after partial I/O.
		closeBlobTransferConn(nextConn)
		conn = nil

		if !isRetryableBlobTransferError(ctx, err) || attempt == blobTransferMaxAttempts {
			break
		}
		if logger != nil {
			logger.Printf(
				"retrying blob %s chunk: hash=%s offset=%d attempt=%d/%d error=%v",
				direction,
				hash,
				offset,
				attempt+1,
				blobTransferMaxAttempts,
				err,
			)
		}
		if err := waitBlobTransferRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}

	return nil, fmt.Errorf(
		"%s blob chunk %s offset %d failed after %d attempts: %w",
		direction,
		hash,
		offset,
		blobTransferMaxAttempts,
		lastErr,
	)
}

func waitBlobTransferRetry(ctx context.Context, attempt int) error {
	delay := blobTransferRetryBase << max(attempt-1, 0)
	delay = min(delay, blobTransferRetryMax)

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
