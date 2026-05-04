//go:build windows

package host

import (
	"strings"
	"testing"
)

func TestWSLChildScriptUsesNetworkNamespaceForNone(t *testing.T) {
	script, err := wslChildScript(ociChildSpec{
		RootFS:  "/rootfs",
		WorkDir: "/workspace",
		Command: []string{"sh"},
		Network: "none",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(script, "exec unshare -m -n --fork \"$0\" inner") {
		t.Fatalf("script does not request network namespace:\n%s", script)
	}

	if strings.Contains(script, "exec sh \"$0\" inner") {
		t.Fatalf("script must not fall back without network isolation:\n%s", script)
	}
}

func TestWSLChildScriptRequiresMountNamespaceForHostNetwork(t *testing.T) {
	script, err := wslChildScript(ociChildSpec{
		RootFS:  "/rootfs",
		WorkDir: "/workspace",
		Command: []string{"sh"},
		Network: "host",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(script, "exec unshare -m --fork \"$0\" inner") {
		t.Fatalf("script does not keep mount namespace attempt:\n%s", script)
	}

	if strings.Contains(script, "exec sh \"$0\" inner") {
		t.Fatalf("script must not fall back without mount namespace:\n%s", script)
	}

	if !strings.Contains(script, "requires unshare with mount namespace support") {
		t.Fatalf("script should explain missing unshare requirement:\n%s", script)
	}
}

func TestWSLChildScriptRejectsEscapingBindTarget(t *testing.T) {
	_, err := wslChildScript(ociChildSpec{
		RootFS:  "/rootfs",
		WorkDir: "/workspace",
		Command: []string{"sh"},
		Binds: []ociBind{{
			Source: "/host/cache",
			Target: "/../../tmp/rmtx",
		}},
	})
	if err == nil {
		t.Fatal("expected escaping bind target error")
	}
}

func TestWSLChildScriptCleansBindTarget(t *testing.T) {
	script, err := wslChildScript(ociChildSpec{
		RootFS:  "/rootfs",
		WorkDir: "/workspace",
		Command: []string{"sh"},
		Binds: []ociBind{{
			Source: "/host/cache",
			Target: "/cache/./npm",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(script, `target="$rootfs"'/cache/npm'`) {
		t.Fatalf("script did not clean bind target:\n%s", script)
	}
}
