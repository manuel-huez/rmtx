//go:build !linux && !windows

package host

func hostSupportsOCIRuntime() bool {
	return false
}
