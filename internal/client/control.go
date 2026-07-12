package client

import (
	"context"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func Ping(ctx context.Context, opts RemoteOptions) (protocol.PingResponse, error) {
	conn, info, checked, err := connectUpdatedRemote(ctx, opts, newRunLogger(opts.Stderr))
	if err != nil {
		return protocol.PingResponse{}, err
	}
	defer closeQuietly(conn.Raw())

	if checked {
		return info, nil
	}

	if err := conn.WriteJSON(protocol.MsgPingRequest, protocol.PingRequest{}); err != nil {
		return protocol.PingResponse{}, err
	}

	return expectDataFrameWithOutput[protocol.PingResponse](
		conn,
		protocol.MsgPingResponse,
		opts.Stderr,
	)
}

func pingHost(ctx context.Context, opts RemoteOptions) (protocol.PingResponse, error) {
	conn, info, err := pingHostConn(ctx, opts)
	if err != nil {
		return protocol.PingResponse{}, err
	}
	defer closeQuietly(conn.Raw())

	return info, nil
}

func pingHostConn(
	ctx context.Context,
	opts RemoteOptions,
) (*protocol.Conn, protocol.PingResponse, error) {
	conn, err := dialRemote(ctx, opts)
	if err != nil {
		return nil, protocol.PingResponse{}, err
	}

	if err := conn.WriteJSON(protocol.MsgPingRequest, protocol.PingRequest{}); err != nil {
		closeQuietly(conn.Raw())

		return nil, protocol.PingResponse{}, err
	}

	info, err := expectDataFrameWithOutput[protocol.PingResponse](
		conn,
		protocol.MsgPingResponse,
		opts.Stderr,
	)
	if err != nil {
		closeQuietly(conn.Raw())

		return nil, protocol.PingResponse{}, err
	}

	return conn, info, nil
}

func HostStats(ctx context.Context, opts RemoteOptions) (protocol.HostStatsResponse, error) {
	conn, err := updatedRemoteConn(ctx, opts)
	if err != nil {
		return protocol.HostStatsResponse{}, err
	}
	defer closeQuietly(conn.Raw())

	if err := conn.WriteJSON(
		protocol.MsgHostStatsRequest,
		protocol.HostStatsRequest{},
	); err != nil {
		return protocol.HostStatsResponse{}, err
	}

	return expectDataFrameWithOutput[protocol.HostStatsResponse](
		conn,
		protocol.MsgHostStatsResponse,
		opts.Stderr,
	)
}

func UpdateHost(
	ctx context.Context,
	opts RemoteOptions,
	targetVersion string,
) (protocol.HostUpdateResponse, error) {
	conn, err := dialRemote(ctx, opts)
	if err != nil {
		return protocol.HostUpdateResponse{}, err
	}
	defer closeQuietly(conn.Raw())

	stopContextClose := context.AfterFunc(ctx, func() { closeQuietly(conn.Raw()) })
	defer stopContextClose()

	req := protocol.HostUpdateRequest{Version: targetVersion}
	if err := conn.WriteJSON(protocol.MsgHostUpdateRequest, req); err != nil {
		return protocol.HostUpdateResponse{}, err
	}

	stopLiveness := startConnectionLiveness(ctx, conn, false)
	defer stopLiveness()

	return expectDataFrameWithOutput[protocol.HostUpdateResponse](
		conn,
		protocol.MsgHostUpdateResponse,
		opts.Stderr,
	)
}

func ListContexts(ctx context.Context, opts RemoteOptions) ([]protocol.ContextSummary, error) {
	conn, err := updatedRemoteConn(ctx, opts)
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

func DeleteContexts(
	ctx context.Context,
	opts DeleteContextsOptions,
) (protocol.DeleteContextsResponse, error) {
	conn, err := updatedRemoteConn(ctx, opts.Remote)
	if err != nil {
		return protocol.DeleteContextsResponse{}, err
	}
	defer closeQuietly(conn.Raw())

	req := protocol.DeleteContextsRequest{
		IDs:       append([]string(nil), opts.IDs...),
		All:       opts.All,
		OlderThan: opts.OlderThan,
	}
	if err := conn.WriteJSON(protocol.MsgDeleteContextsRequest, req); err != nil {
		return protocol.DeleteContextsResponse{}, err
	}

	stopLiveness := startConnectionLiveness(ctx, conn, false)
	defer stopLiveness()

	return expectDataFrameWithOutput[protocol.DeleteContextsResponse](
		conn,
		protocol.MsgDeleteContextsResponse,
		opts.Remote.Stderr,
	)
}

func WorkspaceLeases(
	ctx context.Context,
	opts WorkspaceLeasesOptions,
) (protocol.WorkspaceLeasesResponse, error) {
	conn, err := updatedRemoteConn(ctx, opts.Remote)
	if err != nil {
		return protocol.WorkspaceLeasesResponse{}, err
	}
	defer closeQuietly(conn.Raw())

	req := protocol.WorkspaceLeasesRequest{
		ContextID: opts.ContextID,
		Delete:    opts.Delete,
		IDs:       append([]string(nil), opts.IDs...),
	}
	if err := conn.WriteJSON(protocol.MsgWorkspaceLeasesRequest, req); err != nil {
		return protocol.WorkspaceLeasesResponse{}, err
	}

	stopLiveness := startConnectionLiveness(ctx, conn, false)
	defer stopLiveness()

	return expectDataFrameWithOutput[protocol.WorkspaceLeasesResponse](
		conn,
		protocol.MsgWorkspaceLeasesResponse,
		opts.Remote.Stderr,
	)
}

func ContextArtifacts(
	ctx context.Context,
	opts ContextArtifactsOptions,
) (protocol.ContextArtifactsResponse, error) {
	conn, err := updatedRemoteConn(ctx, opts.Remote)
	if err != nil {
		return protocol.ContextArtifactsResponse{}, err
	}
	defer closeQuietly(conn.Raw())

	req := protocol.ContextArtifactsRequest{
		ContextID: opts.ContextID,
		Prune:     opts.Prune,
		Delete:    opts.Delete,
		Volume:    opts.Volume,
	}
	if err := conn.WriteJSON(protocol.MsgContextArtifactsRequest, req); err != nil {
		return protocol.ContextArtifactsResponse{}, err
	}

	stopLiveness := startConnectionLiveness(ctx, conn, false)
	defer stopLiveness()

	return expectDataFrameWithOutput[protocol.ContextArtifactsResponse](
		conn,
		protocol.MsgContextArtifactsResponse,
		opts.Remote.Stderr,
	)
}

func HostCachePrune(ctx context.Context, opts RemoteOptions) (protocol.CachePruneResponse, error) {
	conn, err := updatedRemoteConn(ctx, opts)
	if err != nil {
		return protocol.CachePruneResponse{}, err
	}
	defer closeQuietly(conn.Raw())

	if err := conn.WriteJSON(
		protocol.MsgCachePruneRequest,
		protocol.CachePruneRequest{},
	); err != nil {
		return protocol.CachePruneResponse{}, err
	}

	stopLiveness := startConnectionLiveness(ctx, conn, false)
	defer stopLiveness()

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

func updatedRemoteConn(ctx context.Context, opts RemoteOptions) (*protocol.Conn, error) {
	conn, _, _, err := connectUpdatedRemote(ctx, opts, newRunLogger(opts.Stderr))
	return conn, err
}
