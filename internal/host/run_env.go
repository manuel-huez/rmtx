package host

import (
	"context"
	"runtime"
	"strconv"

	"github.com/shirou/gopsutil/v4/mem"
)

func rmtxRunEnv(ctx context.Context, workspace string, contextID string) []string {
	cpuCount := max(runtime.NumCPU(), 1)

	availableMemory := uint64(0)
	if vm, err := mem.VirtualMemoryWithContext(ctx); err == nil {
		availableMemory = vm.Available
	}

	return []string{
		"RMTX=1",
		"RMTX_RUNNER=host",
		"RMTX_WORKSPACE=" + workspace,
		"RMTX_CONTEXT_ID=" + contextID,
		"RMTX_CPU_COUNT=" + strconv.Itoa(cpuCount),
		"RMTX_MEMORY_AVAILABLE_BYTES=" + strconv.FormatUint(availableMemory, 10),
	}
}
