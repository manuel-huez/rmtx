package host

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPruneContextArtifactsPreservesFilesWhenStateIsCorrupt(t *testing.T) {
	runtimeDir := t.TempDir()
	statePath := filepath.Join(runtimeDir, runtimeDirName, artifactStateFile)

	specPath := filepath.Join(runtimeDir, runtimeDirName, runtimeSpecDirName, "run.json")
	for path, content := range map[string]string{statePath: "{", specPath: "spec"} {
		if err := os.MkdirAll(filepath.Dir(path), defaultDirMode); err != nil {
			t.Fatal(err)
		}

		if err := os.WriteFile(path, []byte(content), defaultFileMode); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := (&Server{}).pruneContextArtifacts(runtimeDir); err == nil {
		t.Fatal("expected corrupt artifact state error")
	}

	if _, err := os.Stat(specPath); err != nil {
		t.Fatalf("runtime spec removed before state validation: %v", err)
	}
}
