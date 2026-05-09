package client

import "testing"

func TestHostUpdateWaitVersionUsesPendingRestartVersion(t *testing.T) {
	got := hostUpdateWaitVersion(
		HostUpdateResult{Restarting: true, Version: "v1.2.4"},
		"v1.2.5",
	)
	if got != "v1.2.4" {
		t.Fatalf("hostUpdateWaitVersion()=%s want v1.2.4", got)
	}
}

func TestHostUpdateWaitVersionFallsBackToTargetVersion(t *testing.T) {
	got := hostUpdateWaitVersion(
		HostUpdateResult{Restarting: true},
		"v1.2.5",
	)
	if got != "v1.2.5" {
		t.Fatalf("hostUpdateWaitVersion()=%s want v1.2.5", got)
	}
}
