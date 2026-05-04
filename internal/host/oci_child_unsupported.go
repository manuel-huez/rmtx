//go:build !linux

package host

import (
	"fmt"
	"os"
	"runtime"
)

func RunOCIChild(args []string) int {
	_ = args

	fmt.Fprintf(os.Stderr, "error: OCI child runtime is not supported on %s\n", runtime.GOOS)

	return 1
}
