//go:build !windows

package main

import (
	"io"
	"os"
)

func hostLogWriter(file *os.File) io.Writer {
	return file
}
