package host

import (
	"context"
	"os/exec"
	"sync"
)

func (s *Server) commandCancel(
	cmd *exec.Cmd,
	cancel context.CancelFunc,
) context.CancelFunc {
	var once sync.Once

	return func() {
		once.Do(func() {
			cancel()
			killCommandProcessTree(cmd)
		})
	}
}
