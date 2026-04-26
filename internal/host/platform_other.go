//go:build !windows

package host

func hostIsWindows() bool {
	return false
}
