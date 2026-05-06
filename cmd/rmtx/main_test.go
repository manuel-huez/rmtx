package main

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/manuel-huez/rmtx/internal/app"
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
