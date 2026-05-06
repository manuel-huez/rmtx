//go:build linux || windows

package host

import (
	"io"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func (s *Server) consumeTTYInput(
	conn *protocol.Conn,
	writer io.Writer,
	cancelRun func(),
) error {
	inputClosed := false

	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return err
		}

		if inputClosed {
			if head.Type == protocol.MsgRunCancel && cancelRun != nil {
				cancelRun()
			}

			if err := conn.DiscardPayload(head); err != nil {
				return err
			}
			continue
		}

		done, err := s.handleInputFrame(conn, head, writer, true, cancelRun)
		if err != nil {
			return err
		}

		if done {
			inputClosed = true
		}
	}
}

func (s *Server) consumeQueuedTTYInput(
	conn *protocol.Conn,
	input *ttyInputForwarding,
	cancelRun func(),
) error {
	return s.consumeTTYInput(conn, input, cancelRun)
}
