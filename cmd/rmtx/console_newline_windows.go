//go:build windows

package main

import (
	"io"
	"os"

	"github.com/manuel-huez/rmtx/internal/terminal"
)

func hostLogWriter(file *os.File) io.Writer {
	if terminal.IsTerminal(file) {
		return &crlfLineFeedWriter{w: file}
	}

	return file
}
