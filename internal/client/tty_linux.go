//go:build linux

package client

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/manuel-huez/rmtx/internal/terminal"
)

func (s *ttyInputSession) startResizeWatcher(opts ExecOptions) {
	sizeFile := selectTTYSizeFile(opts)
	if sizeFile == nil {
		return
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGWINCH)

	s.closeFn = func() {
		signal.Stop(signals)
	}

	go func() {
		for {
			select {
			case <-s.done:
				return
			case <-signals:
				rows, cols, err := terminal.Size(sizeFile)
				if err != nil {
					continue
				}

				s.writeTTYSize(rows, cols)
			}
		}
	}()
}
