//go:build windows

package client

import (
	"time"

	"github.com/manuel-huez/rmtx/internal/terminal"
)

const resizePollInterval = 250 * time.Millisecond

func (s *ttyInputSession) startResizeWatcher(opts ExecOptions) {
	sizeFile := selectTTYSizeFile(opts)
	if sizeFile == nil {
		return
	}

	rows, cols, err := terminal.Size(sizeFile)
	if err != nil {
		rows, cols = 0, 0
	}

	go func() {
		ticker := time.NewTicker(resizePollInterval)
		defer ticker.Stop()

		lastRows, lastCols := rows, cols

		for {
			select {
			case <-s.done:
				return
			case <-ticker.C:
				rows, cols, err := terminal.Size(sizeFile)
				if err != nil || (rows == lastRows && cols == lastCols) {
					continue
				}

				lastRows, lastCols = rows, cols
				s.writeTTYSize(rows, cols)
			}
		}
	}()
}
