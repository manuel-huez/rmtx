package version

import "testing"

func TestStringReturnsDevForLocalBuild(t *testing.T) {
	if got := String(); got != devVersion {
		t.Fatalf("expected %q for local build, got %q", devVersion, got)
	}
}
