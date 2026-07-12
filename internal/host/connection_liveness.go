package host

import (
	"context"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

type idleTimeoutConn interface {
	SetIdleTimeout(time.Duration)
}

func setConnectionIdleTimeout(conn *protocol.Conn, timeout time.Duration) {
	if conn == nil || conn.Raw() == nil {
		return
	}

	if idleConn, ok := conn.Raw().(idleTimeoutConn); ok {
		idleConn.SetIdleTimeout(timeout)
	}
}

func requestUsesSessionLiveness(messageType string) bool {
	switch messageType {
	case protocol.MsgRunRequest,
		protocol.MsgBlobTransferRequest,
		protocol.MsgHostUpdateRequest,
		protocol.MsgDeleteContextsRequest,
		protocol.MsgContextArtifactsRequest,
		protocol.MsgCachePruneRequest:
		return true
	default:
		return false
	}
}

func requestUsesOutboundHeartbeat(messageType string) bool {
	return messageType != protocol.MsgBlobTransferRequest
}

func startSessionLiveness(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *protocol.Conn,
	sendHeartbeat bool,
) func() {
	setConnectionIdleTimeout(conn, sessionIdleTimeout)

	if !sendHeartbeat {
		return func() {
			setConnectionIdleTimeout(conn, 0)
		}
	}

	stopHeartbeat := protocol.StartHeartbeat(
		ctx,
		conn,
		sessionHeartbeatInterval,
		func(error) {
			cancel()

			if conn != nil && conn.Raw() != nil {
				_ = conn.Raw().Close()
			}
		},
	)

	return func() {
		stopHeartbeat()
		setConnectionIdleTimeout(conn, 0)
	}
}
