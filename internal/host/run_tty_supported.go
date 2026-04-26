//go:build linux || windows

package host

import (
	"io"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func (s *Server) consumeTTYInput(conn *protocol.Conn, writer io.Writer) error {
	for {
		head, err := conn.ReadHeader()
		if err != nil {
			return err
		}

		done, err := s.handleInputFrame(conn, head, writer, true)
		if err != nil {
			return err
		}

		if done {
			return nil
		}
	}
}
