package client

import (
	"context"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func Ping(ctx context.Context, opts RemoteOptions) (PingInfo, error) {
	conn, err := dialRemote(ctx, opts)
	if err != nil {
		return PingInfo{}, err
	}
	defer closeQuietly(conn.Raw())

	if err := conn.WriteJSON(protocol.MsgPingRequest, protocol.PingRequest{}); err != nil {
		return PingInfo{}, err
	}

	return expectDataFrameWithOutput[protocol.PingResponse](
		conn,
		protocol.MsgPingResponse,
		opts.Stderr,
	)
}

func UpdateHost(
	ctx context.Context,
	opts RemoteOptions,
	targetVersion string,
) (HostUpdateResult, error) {
	conn, err := dialRemote(ctx, opts)
	if err != nil {
		return HostUpdateResult{}, err
	}
	defer closeQuietly(conn.Raw())

	stopContextClose := context.AfterFunc(ctx, func() { closeQuietly(conn.Raw()) })
	defer stopContextClose()

	req := protocol.HostUpdateRequest{Version: targetVersion}
	if err := conn.WriteJSON(protocol.MsgHostUpdateRequest, req); err != nil {
		return HostUpdateResult{}, err
	}

	return expectDataFrameWithOutput[protocol.HostUpdateResponse](
		conn,
		protocol.MsgHostUpdateResponse,
		opts.Stderr,
	)
}

func ListContexts(ctx context.Context, opts RemoteOptions) ([]ContextInfo, error) {
	conn, err := dialRemote(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer closeQuietly(conn.Raw())

	if err := conn.WriteJSON(
		protocol.MsgListContextsRequest,
		protocol.ListContextsRequest{},
	); err != nil {
		return nil, err
	}

	resp, err := expectDataFrameWithOutput[protocol.ListContextsResponse](
		conn,
		protocol.MsgListContextsResponse,
		opts.Stderr,
	)
	if err != nil {
		return nil, err
	}

	return resp.Contexts, nil
}

func DeleteContexts(ctx context.Context, opts DeleteContextsOptions) (DeleteContextsResult, error) {
	conn, err := dialRemote(ctx, opts.Remote)
	if err != nil {
		return DeleteContextsResult{}, err
	}
	defer closeQuietly(conn.Raw())

	req := protocol.DeleteContextsRequest{
		IDs:       append([]string(nil), opts.IDs...),
		All:       opts.All,
		OlderThan: opts.OlderThan,
	}
	if err := conn.WriteJSON(protocol.MsgDeleteContextsRequest, req); err != nil {
		return DeleteContextsResult{}, err
	}

	return expectDataFrameWithOutput[protocol.DeleteContextsResponse](
		conn,
		protocol.MsgDeleteContextsResponse,
		opts.Remote.Stderr,
	)
}

func ContextArtifacts(
	ctx context.Context,
	opts ContextArtifactsOptions,
) (ContextArtifactsResult, error) {
	conn, err := dialRemote(ctx, opts.Remote)
	if err != nil {
		return ContextArtifactsResult{}, err
	}
	defer closeQuietly(conn.Raw())

	req := protocol.ContextArtifactsRequest{
		ContextID: opts.ContextID,
		Prune:     opts.Prune,
		Delete:    opts.Delete,
		Volume:    opts.Volume,
	}
	if err := conn.WriteJSON(protocol.MsgContextArtifactsRequest, req); err != nil {
		return ContextArtifactsResult{}, err
	}

	return expectDataFrameWithOutput[protocol.ContextArtifactsResponse](
		conn,
		protocol.MsgContextArtifactsResponse,
		opts.Remote.Stderr,
	)
}

func CachePrune(ctx context.Context, opts RemoteOptions) (CachePruneResult, error) {
	conn, err := dialRemote(ctx, opts)
	if err != nil {
		return CachePruneResult{}, err
	}
	defer closeQuietly(conn.Raw())

	if err := conn.WriteJSON(
		protocol.MsgCachePruneRequest,
		protocol.CachePruneRequest{},
	); err != nil {
		return CachePruneResult{}, err
	}

	return expectDataFrameWithOutput[protocol.CachePruneResponse](
		conn,
		protocol.MsgCachePruneResponse,
		opts.Stderr,
	)
}

func dialRemote(ctx context.Context, opts RemoteOptions) (*protocol.Conn, error) {
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

	return conn, nil
}
