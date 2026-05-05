package client

import "testing"

func TestHostNeedsUpdateOnlyForNewerReleaseClient(t *testing.T) {
	tests := []struct {
		name         string
		hostVersion  string
		localVersion string
		want         bool
	}{
		{name: "newer patch", hostVersion: "v1.2.3", localVersion: "v1.2.4", want: true},
		{name: "same", hostVersion: "v1.2.3", localVersion: "v1.2.3", want: false},
		{name: "host newer", hostVersion: "v1.2.4", localVersion: "v1.2.3", want: false},
		{name: "dev client", hostVersion: "v1.2.3", localVersion: "dev", want: false},
		{name: "dev host", hostVersion: "dev", localVersion: "v1.2.3", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := hostNeedsUpdate(tc.hostVersion, tc.localVersion)
			if got != tc.want {
				t.Fatalf(
					"hostNeedsUpdate(%q, %q)=%t want %t",
					tc.hostVersion,
					tc.localVersion,
					got,
					tc.want,
				)
			}
		})
	}
}

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
