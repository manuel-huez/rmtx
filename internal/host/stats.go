package host

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/version"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

const (
	systemStatsSampleInterval = 250 * time.Millisecond
	percentScale              = 100
)

func (s *Server) handleHostStats(
	ctx context.Context,
	conn *protocol.Conn,
	requestLogs *hostLogSubscription,
) error {
	resp, err := s.hostStats(ctx)
	if err != nil {
		return err
	}

	s.logger.Printf(
		"host stats handled: remote=%s cpu_used=%.1f memory_used=%.1f active_runs=%d",
		conn.Raw().RemoteAddr(),
		resp.CPU.UsedPercent,
		resp.Memory.UsedPercent,
		resp.ActiveRuns,
	)

	return writeJSONAfterLogs(conn, requestLogs, protocol.MsgHostStatsResponse, resp)
}

func (s *Server) hostStats(ctx context.Context) (protocol.HostStatsResponse, error) {
	cpuStats, memoryStats, warnings, err := collectMachineStats(ctx)
	if err != nil {
		return protocol.HostStatsResponse{}, err
	}

	contexts, err := s.listContexts()
	if err != nil {
		warnings = append(warnings, fmt.Sprintf("contexts: %v", err))
	}

	return protocol.HostStatsResponse{
		Version:            version.String(),
		Name:               s.hostName(),
		Address:            s.Addr(),
		Fingerprint:        s.fingerprint,
		Now:                time.Now().UTC(),
		OS:                 runtime.GOOS,
		Arch:               runtime.GOARCH,
		CPU:                cpuStats,
		Memory:             memoryStats,
		ContextCount:       len(contexts),
		ActiveRuns:         s.activeRunCount(),
		ActiveContextCount: s.activeContextCount(),
		Warnings:           warnings,
	}, nil
}

func collectMachineStats(
	ctx context.Context,
) (protocol.HostCPUStats, protocol.HostMemoryStats, []string, error) {
	var warnings []string

	logicalCores := max(runtime.NumCPU(), 1)

	physicalCores, err := cpu.CountsWithContext(ctx, false)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return protocol.HostCPUStats{}, protocol.HostMemoryStats{}, nil, err
		}

		warnings = append(warnings, fmt.Sprintf("physical cores: %v", err))
		physicalCores = 0
	}

	cpuStats, cpuWarnings, err := collectCPUStats(ctx, logicalCores, physicalCores)
	if err != nil {
		return protocol.HostCPUStats{}, protocol.HostMemoryStats{}, nil, err
	}

	warnings = append(warnings, cpuWarnings...)

	memoryStats, memoryWarnings, err := collectMemoryStats(ctx)
	if err != nil {
		return protocol.HostCPUStats{}, protocol.HostMemoryStats{}, nil, err
	}

	warnings = append(warnings, memoryWarnings...)

	return cpuStats, memoryStats, warnings, nil
}

func collectCPUStats(
	ctx context.Context,
	logicalCores int,
	physicalCores int,
) (protocol.HostCPUStats, []string, error) {
	perCore, warnings, err := sampleCPUPercent(ctx)
	if err != nil {
		return protocol.HostCPUStats{}, nil, err
	}

	if len(perCore) > 0 {
		logicalCores = len(perCore)
	}

	usedPercent, usedCores := summarizeCPUUsage(perCore, logicalCores)

	return protocol.HostCPUStats{
		LogicalCores:       logicalCores,
		PhysicalCores:      physicalCores,
		UsedPercent:        usedPercent,
		UsedCores:          usedCores,
		PerCoreUsedPercent: perCore,
	}, warnings, nil
}

func sampleCPUPercent(ctx context.Context) ([]float64, []string, error) {
	percents, err := cpu.PercentWithContext(ctx, systemStatsSampleInterval, true)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, nil, err
		}

		return nil, []string{fmt.Sprintf("cpu usage: %v", err)}, nil
	}

	if len(percents) == 0 {
		return nil, []string{"cpu usage: no samples"}, nil
	}

	return percents, nil, nil
}

func summarizeCPUUsage(perCore []float64, logicalCores int) (float64, float64) {
	if len(perCore) == 0 || logicalCores <= 0 {
		return 0, 0
	}

	var usedCores float64

	for _, percent := range perCore {
		usedCores += percent / percentScale
	}

	return usedCores / float64(logicalCores) * percentScale, usedCores
}

func collectMemoryStats(ctx context.Context) (protocol.HostMemoryStats, []string, error) {
	vm, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return protocol.HostMemoryStats{}, nil, err
		}

		return protocol.HostMemoryStats{}, []string{fmt.Sprintf("memory: %v", err)}, nil
	}

	return protocol.HostMemoryStats{
		TotalBytes:     vm.Total,
		AvailableBytes: vm.Available,
		UsedBytes:      vm.Used,
		UsedPercent:    vm.UsedPercent,
	}, nil, nil
}

func (s *Server) activeRunCount() int {
	s.restartMu.Lock()
	defer s.restartMu.Unlock()

	return s.activeRuns
}

func (s *Server) activeContextCount() int {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()

	return len(s.activeContexts)
}
