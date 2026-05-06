package wslconfig

import (
	"strings"
	"testing"
)

func TestApplyUpdatesExistingWSL2Section(t *testing.T) {
	content := strings.Join([]string{
		"# comment",
		"[wsl2]",
		"memory=4GB",
		"processors=2",
		"",
		"[experimental]",
		"sparseVhd=true",
	}, "\n")

	got := Apply(content, SectionWSL2, map[string]string{
		"memory":     "16GB",
		"processors": "8",
	})

	if !strings.Contains(got, "[wsl2]\nmemory=16GB\nprocessors=8\n\n[experimental]") {
		t.Fatalf("unexpected content:\n%s", got)
	}

	if !strings.Contains(got, "sparseVhd=true") {
		t.Fatalf("lost unrelated section:\n%s", got)
	}
}

func TestApplyUpdatesDuplicateWSL2Settings(t *testing.T) {
	content := strings.Join([]string{
		"[wsl2]",
		"memory=4GB",
		"processors=2",
		"memory=6GB",
		"processors=3",
	}, "\n")

	got := Apply(content, SectionWSL2, map[string]string{
		"memory":     "16GB",
		"processors": "8",
	})

	settings := ParseSection(got, SectionWSL2)
	if settings["memory"] != "16GB" || settings["processors"] != "8" {
		t.Fatalf("settings=%v\ncontent:\n%s", settings, got)
	}

	if strings.Contains(got, "memory=4GB") || strings.Contains(got, "memory=6GB") {
		t.Fatalf("stale memory setting remained:\n%s", got)
	}

	if strings.Contains(got, "processors=2") || strings.Contains(got, "processors=3") {
		t.Fatalf("stale processor setting remained:\n%s", got)
	}
}

func TestApplyAddsMissingWSL2Section(t *testing.T) {
	got := Apply("[experimental]\nsparseVhd=true", SectionWSL2, map[string]string{
		"memory":     "8GB",
		"processors": "4",
	})

	if !strings.Contains(got, "[wsl2]\nmemory=8GB\nprocessors=4") {
		t.Fatalf("missing wsl2 section:\n%s", got)
	}
}

func TestParseSectionIgnoresComments(t *testing.T) {
	got := ParseSection("[wsl2]\n# memory=2GB\nmemory = 8GB\nprocessors=4\n", SectionWSL2)

	if got["memory"] != "8GB" || got["processors"] != "4" {
		t.Fatalf("settings=%v", got)
	}
}

func TestProfileSettings(t *testing.T) {
	settings, err := ProfileSettings(SystemSpecs{
		LogicalProcessors: 16,
		TotalMemoryBytes:  32 * OneGiB,
	}, 0.5)
	if err != nil {
		t.Fatal(err)
	}

	if settings["processors"] != "8" || settings["memory"] != "16GB" {
		t.Fatalf("settings=%v", settings)
	}
}
