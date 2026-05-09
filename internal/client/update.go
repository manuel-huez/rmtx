package client

import (
	"context"
	"fmt"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/version"
)

const (
	hostUpdateRestartTimeout = 2 * time.Minute
	hostUpdatePollInterval   = 500 * time.Millisecond
)

var clientVersion = version.String

func connectUpdatedRemote(
	ctx context.Context,
	opts RemoteOptions,
	logger *runLogger,
) (*protocol.Conn, PingInfo, bool, error) {
	targetVersion := clientVersion()
	if !version.ValidRelease(targetVersion) {
		conn, err := dialRemote(ctx, opts)
		return conn, PingInfo{}, false, err
	}

	for {
		conn, info, err := pingHostConn(ctx, opts)
		if err != nil {
			return nil, PingInfo{}, false, err
		}

		cmp, comparable := version.CompareRelease(targetVersion, info.Version)
		if !comparable {
			// Old/dev hosts may close after ping, so a fresh conn preserves one-request compatibility.
			closeQuietly(conn.Raw())

			conn, err := dialRemote(ctx, opts)
			return conn, PingInfo{}, false, err
		}

		if cmp <= 0 {
			return conn, info, true, nil
		}

		closeQuietly(conn.Raw())

		logger.Printf(
			"host update required: host_version=%s client_version=%s",
			info.Version,
			targetVersion,
		)

		result, err := UpdateHost(ctx, opts, targetVersion)
		if err != nil {
			return nil, PingInfo{}, false, fmt.Errorf(
				"host update required (%s -> %s): %w; if the host is too old for auto-update, run `go install %s@%s` on the host",
				info.Version,
				targetVersion,
				err,
				version.CommandPackage,
				targetVersion,
			)
		}

		if !result.Restarting {
			continue
		}

		if err := waitForHostUpdate(
			ctx,
			opts,
			hostUpdateWaitVersion(result, targetVersion),
			logger,
		); err != nil {
			return nil, PingInfo{}, false, err
		}
	}
}

func hostUpdateWaitVersion(result HostUpdateResult, targetVersion string) string {
	if version.ValidRelease(result.Version) {
		return result.Version
	}

	return targetVersion
}

func waitForHostUpdate(
	ctx context.Context,
	opts RemoteOptions,
	targetVersion string,
	logger *runLogger,
) error {
	waitCtx, cancel := context.WithTimeout(ctx, hostUpdateRestartTimeout)
	defer cancel()

	var lastErr error

	for {
		info, err := pingHost(waitCtx, opts)
		if err == nil {
			cmp, ok := version.CompareRelease(info.Version, targetVersion)
			if ok && cmp >= 0 {
				logger.Printf("host update ready: version=%s", info.Version)

				return nil
			}

			lastErr = fmt.Errorf("host reported version %s", info.Version)
		} else {
			lastErr = err
		}

		timer := time.NewTimer(hostUpdatePollInterval)
		select {
		case <-waitCtx.Done():
			timer.Stop()

			return fmt.Errorf("wait for host update to %s: %w", targetVersion, lastErr)
		case <-timer.C:
		}
	}
}
