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
		Network: noneValue,
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

func TestWSLChildScriptBindsHostNetworkFilesForHostNetwork(t *testing.T) {
	script, err := wslChildScript(ociChildSpec{
		RootFS:  "/rootfs",
		WorkDir: "/workspace",
		Command: []string{"sh"},
		Network: "host",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, file := range []string{"/etc/resolv.conf", "/etc/hosts", "/etc/hostname"} {
		if !strings.Contains(script, "mount --bind '"+file+"' \"$target\"") {
			t.Fatalf("script does not bind %s:\n%s", file, script)
		}
	}
}

func TestWSLChildScriptStagesRootFSWhenConfigured(t *testing.T) {
	script, err := wslChildScript(ociChildSpec{
		RootFS:       "/mnt/c/rmtx/rootfs",
		StagedRootFS: "/var/lib/rmtx/rootfs/key",
		RootFSID:     "instance",
		WorkDir:      "/workspace",
		Command:      []string{"sh"},
		Network:      "host",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"source_rootfs='/mnt/c/rmtx/rootfs'",
		"rootfs='/var/lib/rmtx/rootfs/key'",
		"rootfs_id='instance'",
		"tar -cpf - .",
		".rmtx-rootfs-stage-id",
		".rmtx-wsl-stage-canonical",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q:\n%s", want, script)
		}
	}
}

func TestWSLStagedRootFSPathUsesNativeWSLPath(t *testing.T) {
	first := wslStagedRootFSPath("/mnt/c/state/rootfs/a")
	second := wslStagedRootFSPath("/mnt/c/state/rootfs/a")
	other := wslStagedRootFSPath("/mnt/c/state/rootfs/b")

	if !strings.HasPrefix(first, "/var/lib/rmtx/rootfs/") {
		t.Fatalf("unexpected staged rootfs path: %s", first)
	}
	if first != second {
		t.Fatal("staged rootfs path should be deterministic")
	}
	if first == other {
		t.Fatal("different rootfs paths should use different staging paths")
	}
}

func TestWSLPruneRootFSScriptDeletesNonLiveRoots(t *testing.T) {
	script := wslPruneRootFSScript()
	for _, want := range []string{
		"/var/lib/rmtx/rootfs",
		"for live in \"$@\"",
		"case \"$name\" in *.tmp.*) continue ;; esac",
		"rm -rf \"$path\"",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("prune script missing %q:\n%s", want, script)
		}
	}
	if strings.Contains(script, "\"$root\"/*.tmp.*") {
		t.Fatalf("prune script should not target active staging temps:\n%s", script)
	}
}

func TestWSLChildScriptSkipsHostNetworkFilesForNoNetwork(t *testing.T) {
	script, err := wslChildScript(ociChildSpec{
		RootFS:  "/rootfs",
		WorkDir: "/workspace",
		Command: []string{"sh"},
		Network: noneValue,
	})
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(script, "mount --bind '/etc/resolv.conf' \"$target\"") {
		t.Fatalf("script should not bind resolver without network:\n%s", script)
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
