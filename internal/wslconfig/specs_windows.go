//go:build windows

package wslconfig

import (
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
)

type memoryStatusEx struct {
	length               uint32
	memoryLoad           uint32
	totalPhys            uint64
	availPhys            uint64
	totalPageFile        uint64
	availPageFile        uint64
	totalVirtual         uint64
	availVirtual         uint64
	availExtendedVirtual uint64
}

func DetectSystemSpecs() (SystemSpecs, error) {
	status := memoryStatusEx{length: uint32(unsafe.Sizeof(memoryStatusEx{}))}
	kernel32 := windows.NewLazySystemDLL("kernel32.dll")
	globalMemoryStatusEx := kernel32.NewProc("GlobalMemoryStatusEx")
	ret, _, err := globalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&status)))
	if ret == 0 {
		return SystemSpecs{}, err
	}

	return SystemSpecs{
		LogicalProcessors: runtime.NumCPU(),
		TotalMemoryBytes:  status.totalPhys,
	}, nil
}
