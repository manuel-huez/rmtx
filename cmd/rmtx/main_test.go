package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/manuel-huez/rmtx/internal/app"
	"github.com/manuel-huez/rmtx/internal/client"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/wslconfig"
)

func TestResolveTTYModeDefaultsToDisabled(t *testing.T) {
	mode, err := resolveTTYMode(false, false)
	if err != nil {
		t.Fatal(err)
	}

	if mode != app.TTYDisable {
		t.Fatalf("expected default TTY mode %v, got %v", app.TTYDisable, mode)
	}
}

func TestResolveTTYModeForceAndDisableConflict(t *testing.T) {
	if _, err := resolveTTYMode(true, true); err == nil {
		t.Fatal("expected conflict error")
	}
}

func TestCRLFLineFeedWriterConvertsLoneLineFeeds(t *testing.T) {
	var out bytes.Buffer
	writer := &crlfLineFeedWriter{w: &out}

	if _, err := writer.Write([]byte("one\ntwo\r\nthree\r")); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("\nfour\n")); err != nil {
		t.Fatal(err)
	}

	want := "one\r\ntwo\r\nthree\r\nfour\r\n"
	if out.String() != want {
		t.Fatalf("output=%q want %q", out.String(), want)
	}
}

func TestRunCacheRequiresPruneSubcommand(t *testing.T) {
	if code := runCache(context.Background(), nil); code != exitUsage {
		t.Fatalf("exit code=%d want %d", code, exitUsage)
	}
}

func TestRunExecWithFlagsRejectsNegativeKeepWorkspace(t *testing.T) {
	code, stderr := captureRunStderr(t, func() int {
		return runExecWithFlags(
			context.Background(),
			[]string{"--keep-workspace=-1s", "--", "true"},
		)
	})
	if code != exitUsage {
		t.Fatalf("exit code=%d want %d", code, exitUsage)
	}

	if !strings.Contains(stderr, "keep-workspace duration must be positive") {
		t.Fatalf("stderr missing keep-workspace error: %q", stderr)
	}
}

func TestContextTargetFromFlagsRejectsCurrentAndContext(t *testing.T) {
	_, _, err := contextTargetFromFlags(true, "ctx")
	if err == nil {
		t.Fatal("expected conflict error")
	}
}

func TestRunRejectsUnknownCommandInsteadOfExec(t *testing.T) {
	for _, args := range [][]string{
		{"echo", "test"},
		{"status"},
	} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			code, stderr := captureRunStderr(t, func() int {
				return run(context.Background(), args)
			})

			if code != exitUsage {
				t.Fatalf("exit code=%d want %d", code, exitUsage)
			}
			if !strings.Contains(stderr, `error: unknown command "`+args[0]+`"`) {
				t.Fatalf("stderr missing unknown command error: %q", stderr)
			}
		})
	}
}

func captureRunStderr(t *testing.T, fn func() int) (int, string) {
	t.Helper()

	original := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = writer

	code := fn()

	os.Stderr = original
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	return code, string(output)
}

func TestFormatStatsLineIncludesMachineFields(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	got := formatStatsLine(client.HostStatsInfo{
		Version:     "v1.2.3",
		Name:        "build-host",
		Address:     "127.0.0.1:33221",
		Fingerprint: "sha256:abc",
		Now:         now,
		OS:          "linux",
		Arch:        "amd64",
		CPU: protocol.HostCPUStats{
			LogicalCores:       8,
			PhysicalCores:      4,
			UsedPercent:        25,
			UsedCores:          2,
			PerCoreUsedPercent: []float64{10, 25.5, 0, 100},
		},
		Memory: protocol.HostMemoryStats{
			TotalBytes:     16,
			UsedBytes:      8,
			AvailableBytes: 8,
			UsedPercent:    50,
		},
		ActiveRuns:         1,
		ActiveContextCount: 1,
		ContextCount:       3,
	})

	for _, want := range []string{
		"stats\tbuild-host\t127.0.0.1:33221",
		"os=linux",
		"logical_cpus=8",
		"physical_cores=4",
		"cpu_used_percent=25.0",
		"cpu_used_cores=2.00",
		"cpu_per_core_used_percent=10.0,25.5,0.0,100.0",
		"memory_available_bytes=8",
		"contexts=3",
		"at=2026-05-07T12:00:00Z",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("stats line missing %q: %s", want, got)
		}
	}

	for _, unwanted := range []string{"context_id=", "context_disk_bytes="} {
		if strings.Contains(got, unwanted) {
			t.Fatalf("stats line should not include %q: %s", unwanted, got)
		}
	}
}

func TestFormatStatsLineUsesPlaceholderForMissingPerCoreUsage(t *testing.T) {
	got := formatStatsLine(client.HostStatsInfo{})

	if !strings.Contains(got, "cpu_per_core_used_percent=-") {
		t.Fatalf("stats line missing per-core placeholder: %s", got)
	}
}

func TestPrintArtifactsIncludesTotalListedSize(t *testing.T) {
	var out bytes.Buffer
	printArtifacts(&out, []client.ContextArtifact{
		{Kind: "workspace", Size: 3},
		{Kind: "volume", Size: 4},
	})

	got := out.String()
	for _, line := range strings.Split(got, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 4 && fields[0] == "total" && fields[3] == "7" {
			return
		}
	}

	t.Fatalf("artifact output missing total size: %q", got)
}

func TestPrintWorkspaceLeases(t *testing.T) {
	var out bytes.Buffer
	printWorkspaceLeases(&out, []client.WorkspaceLeaseInfo{{
		ID:        "ws_1234",
		Path:      "/tmp/workspace",
		ExpiresAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
		Dirty:     true,
		Active:    true,
	}})

	got := out.String()
	for _, want := range []string{
		"ID",
		"EXPIRES",
		"ws_1234",
		"2026-07-04T12:00:00Z",
		labelYes,
		"/tmp/workspace",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("workspace output missing %q: %s", want, got)
		}
	}
}

func TestSelectWSLProfileByFlag(t *testing.T) {
	profiles := []wslconfig.Profile{
		{Name: "50%", Settings: map[string]string{"processors": "4", "memory": "8GB"}},
		{Name: "100%", Settings: map[string]string{"processors": "8", "memory": "16GB"}},
	}

	selected, err := selectWSLProfile("100", profiles, strings.NewReader(""), &strings.Builder{})
	if err != nil {
		t.Fatal(err)
	}

	if selected == nil || selected.Name != "100%" {
		t.Fatalf("selected=%v", selected)
	}
}

func TestConfirmUsesBufferedInput(t *testing.T) {
	input := bufio.NewReader(strings.NewReader("1\ny\n"))
	profiles := []wslconfig.Profile{
		{Name: "50%", Settings: map[string]string{"processors": "4", "memory": "8GB"}},
		{Name: "100%", Settings: map[string]string{"processors": "8", "memory": "16GB"}},
	}

	selected, err := selectWSLProfile("", profiles, input, &strings.Builder{})
	if err != nil {
		t.Fatal(err)
	}
	if selected == nil || selected.Name != "50%" {
		t.Fatalf("selected=%v", selected)
	}

	if !confirm(input, &strings.Builder{}, "") {
		t.Fatal("expected confirmation")
	}
}
