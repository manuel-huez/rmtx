package protocol

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
)

// IsDisconnectError reports whether err means the peer connection stopped.
func IsDisconnectError(err error) bool {
	return isCommonDisconnectError(err) ||
		isDisconnectErrnoError(err) ||
		isTimeoutDisconnectError(err)
}

func isCommonDisconnectError(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, os.ErrDeadlineExceeded)
}

func isDisconnectErrnoError(err error) bool {
	var errno syscall.Errno

	return errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, windowsConnectionReset) ||
		(errors.As(err, &errno) && isDisconnectErrno(errno))
}

func isTimeoutDisconnectError(err error) bool {
	var netErr net.Error

	return errors.As(err, &netErr) &&
		netErr.Timeout() &&
		!errors.Is(err, context.DeadlineExceeded)
}

const windowsConnectionReset syscall.Errno = 10054

func isDisconnectErrno(errno syscall.Errno) bool {
	return errno == syscall.ECONNABORTED ||
		errno == syscall.ECONNRESET ||
		errno == syscall.EPIPE ||
		errno == windowsConnectionReset
}
