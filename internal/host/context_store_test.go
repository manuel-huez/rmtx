package host

import "testing"

func TestNormalizeContextIDRejectsFilesystemDotNames(t *testing.T) {
	for _, id := range []string{".", ".."} {
		t.Run(id, func(t *testing.T) {
			if _, err := normalizeContextID(id); err == nil {
				t.Fatalf("normalizeContextID(%q) succeeded", id)
			}
		})
	}
}
