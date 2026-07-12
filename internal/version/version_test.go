//nolint:goconst // Repeated fixture literals keep each test case self-contained.
package version

import "testing"

func TestStringReturnsDevForLocalBuild(t *testing.T) {
	if got := String(); got != devVersion {
		t.Fatalf("expected %q for local build, got %q", devVersion, got)
	}
}

func TestCompareRelease(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
		want int
	}{
		{name: "equal", a: "v1.2.3", b: "v1.2.3", want: 0},
		{name: "minor", a: "v1.10.0", b: "v1.2.0", want: 1},
		{name: "patch", a: "v1.2.4", b: "v1.2.3", want: 1},
		{name: "prerelease before release", a: "v1.2.3-rc.1", b: "v1.2.3", want: -1},
		{name: "numeric prerelease", a: "v1.2.3-rc.10", b: "v1.2.3-rc.2", want: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := CompareRelease(tc.a, tc.b)
			if !ok {
				t.Fatalf("CompareRelease(%q, %q) rejected valid releases", tc.a, tc.b)
			}

			switch {
			case got < 0 && tc.want >= 0:
				t.Fatalf("CompareRelease(%q, %q)=%d, want %d", tc.a, tc.b, got, tc.want)
			case got > 0 && tc.want <= 0:
				t.Fatalf("CompareRelease(%q, %q)=%d, want %d", tc.a, tc.b, got, tc.want)
			case got == 0 && tc.want != 0:
				t.Fatalf("CompareRelease(%q, %q)=%d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestValidReleaseRejectsUnsafeVersions(t *testing.T) {
	for _, value := range []string{
		"dev",
		"latest",
		"v1.2",
		"v1.02.3",
		"v1.2.3;rm -rf /",
		"v1.2.3-",
		"v1.2.3+",
	} {
		if ValidRelease(value) {
			t.Fatalf("ValidRelease(%q)=true, want false", value)
		}
	}
}
