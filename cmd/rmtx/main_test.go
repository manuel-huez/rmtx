package main

import (
	"context"
	"testing"

	"github.com/manuel-huez/rmtx/internal/app"
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

func TestRunCacheRequiresPruneSubcommand(t *testing.T) {
	if code := runCache(context.Background(), nil); code != exitUsage {
		t.Fatalf("exit code=%d want %d", code, exitUsage)
	}
}
