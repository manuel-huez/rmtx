package host

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/manuel-huez/rmtx/internal/protocol"
)

func TestSummarizeCPUUsageDerivesAggregateFromPerCore(t *testing.T) {
	usedPercent, usedCores := summarizeCPUUsage([]float64{0, 50, 100, 25}, 4)

	if usedPercent != 43.75 {
		t.Fatalf("used percent=%f want 43.75", usedPercent)
	}

	if usedCores != 1.75 {
		t.Fatalf("used cores=%f want 1.75", usedCores)
	}
}

func TestHostStatsReportsContextDiskUsage(t *testing.T) {
	stateDir := t.TempDir()
	contextID := "stats-context"
	contextDir := filepath.Join(stateDir, contextDirName, contextID)
	if err := os.MkdirAll(filepath.Join(contextDir, contextWorkspaceDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(contextDir, contextWorkspaceDir, "file.txt"), []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(contextDir, contextMetaFile), []byte("meta"), 0o644); err != nil {
		t.Fatal(err)
	}

	s := &Server{opts: Options{StateDir: stateDir}}
	stats, err := s.hostStats(
		context.Background(),
		protocol.HostStatsRequest{ContextID: contextID},
	)
	if err != nil {
		t.Fatal(err)
	}

	if stats.ContextID != contextID {
		t.Fatalf("context id=%q want %q", stats.ContextID, contextID)
	}
	if stats.ContextDiskBytes != 7 {
		t.Fatalf("context bytes=%d want 7", stats.ContextDiskBytes)
	}
}
