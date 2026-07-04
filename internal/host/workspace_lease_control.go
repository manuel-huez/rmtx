package host

import (
	"errors"
	"fmt"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func (s *Server) handleWorkspaceLeases(
	conn *protocol.Conn,
	req protocol.WorkspaceLeasesRequest,
	requestLogs *hostLogSubscription,
) error {
	contextID, err := normalizeContextID(req.ContextID)
	if err != nil {
		return err
	}

	contextDir, err := s.contextDataDir(contextID)
	if err != nil {
		return err
	}

	var resp protocol.WorkspaceLeasesResponse

	if req.Delete {
		if len(req.IDs) == 0 {
			return errors.New("workspace lease delete requires at least one id")
		}

		deleted, notFound, err := s.deleteWorkspaceLeases(contextID, contextDir, req.IDs)
		if err != nil {
			return err
		}

		resp.Deleted = deleted
		resp.NotFound = notFound

		s.logger.Printf(
			"workspace leases deleted: context=%s deleted=%d not_found=%d",
			contextID,
			len(deleted),
			len(notFound),
		)

		return writeJSONAfterLogs(conn, requestLogs, protocol.MsgWorkspaceLeasesResponse, resp)
	}

	workspaces, err := s.listWorkspaceLeases(contextDir, contextID)
	if err != nil {
		return fmt.Errorf("list workspace leases: %w", err)
	}

	resp.Workspaces = workspaces

	return writeJSONAfterLogs(conn, requestLogs, protocol.MsgWorkspaceLeasesResponse, resp)
}
