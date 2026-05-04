//go:build !linux && !windows

package host

func hostCapabilities() []string {
	return nil
}

func hostSupportsOCIRuntime() bool {
	return false
}
