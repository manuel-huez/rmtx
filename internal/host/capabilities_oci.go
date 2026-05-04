//go:build linux || windows

package host

import "github.com/manuel-huez/rmtx/internal/protocol"

func hostCapabilities() []string {
	return []string{protocol.HostCapabilityOCIRuntime}
}

func hostSupportsOCIRuntime() bool {
	return true
}
