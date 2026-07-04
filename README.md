# rmtx

> [!WARNING]
> `rmtx` is still **experimental**. Expect rough edges, breaking changes, and incomplete docs.

`rmtx` runs commands on a nearby host machine while keeping a persistent context on that host.

## Quick overview

- `rmtx host`: run the host service.
- `rmtx init`: discover host, create local config, pair client.
- `rmtx exec -- <command> ...`: run a command remotely in the current context.
- `rmtx pair`: pair a client with a host.
- `rmtx ping`: verify host connectivity/auth.
- `rmtx stats`: report host CPU/RAM/core/per-core usage.
- `rmtx context ...`: list/delete/prune host contexts.
- `rmtx context workspaces ...`: list/delete kept host workspaces.
- `rmtx context artifacts ...`: list/prune/delete host-side context artifacts.
- `rmtx cache prune`: delete unreferenced host cache data.

## Install

```bash
go install github.com/manuel-huez/rmtx/cmd/rmtx@latest
rmtx version
```

## Build from source

```bash
go build ./cmd/rmtx
```

## Supported platforms

Core client and host commands build on these platforms:

- Linux: `386`, `amd64`, `arm`, `arm64`, `loong64`, `mips`, `mips64`,
  `mips64le`, `mipsle`, `ppc64`, `ppc64le`, `riscv64`, `s390x`
- macOS: `amd64`, `arm64`
- Windows: `386`, `amd64`, `arm64`
- FreeBSD: `386`, `amd64`, `arm`, `arm64`
- OpenBSD: `386`, `amd64`, `arm`, `arm64`, `riscv64`
- NetBSD: `amd64`, `arm`, `arm64`
- DragonFly BSD: `amd64`

Support means `rmtx host`, non-TTY remote exec, sync, pairing, ping, stats,
context, and cache commands compile for that OS/architecture. Feature-specific
support is narrower:

- Interactive `--tty`: Linux and Windows.
- OCI runtime: Linux hosts natively, Windows hosts through WSL2.
- NVIDIA GPU runtime support: Linux hosts and Windows hosts through WSL2.
- `rmtx wsl config`: Windows only.

Unsupported Go targets include Android, iOS, JS/WASM, wasip1/WASM, Plan 9,
Solaris, illumos, AIX, NetBSD/386, and OpenBSD/ppc64.

## Minimal setup

Start host:

```bash
./rmtx host --listen :33221
```

In your project on the client, initialize once:

```bash
./rmtx init
./rmtx exec -- go test ./...
```

`rmtx init` discovers available hosts, asks you to trust selected host fingerprint, writes `.rmtx.json`, then requests a one-time pairing code from that host and prompts you to enter it. After init, `rmtx pair` re-pairs an existing config. If LAN discovery is blocked, run `rmtx host pair-code` on host to get fingerprint, then use `rmtx init --host 192.168.1.42:33221 --fingerprint sha256:...`. For non-interactive/manual pairing, `rmtx host pair-code` and `rmtx pair --code ...` still work.

Generated `.rmtx.json` looks like:

```json
{
  "version": 1,
  "context": { "name": "my-project" },
  "tls": { "host_fingerprint": "sha256:..." },
  "mounts": [{ "path": "." }],
  "ignore": [".git/**", "node_modules/**"],
  "ignore_gitignore": true
}
```

## Config file

`rmtx` looks upward from the current directory for:

- `.rmtx.json`
- `rmtx.json`

A config file is required for remote execution.

Use top-level `ignore` patterns to skip files for every mount. Use per-mount `exclude`
patterns for mount-specific ignores:

```json
{
  "mounts": [{ "path": ".", "exclude": ["tmp/**"] }],
  "ignore": [".git/**", "node_modules/**", "dist/**"],
  "ignore_gitignore": true
}
```

Use `sync_back` to limit which host-side changes are copied back after
each run. Paths are relative to the project root; a directory path includes its
descendants, and glob patterns like `generated/**` are supported:

```json
{
  "context": { "name": "my-project" },
  "sync_back": ["coverage/", "generated/report.json"]
}
```

When omitted, all mounted paths are eligible for sync-back. Only paths whose
metadata or content changed are sent back to the client. After each run, the
host workspace is cleaned and rehydrated from synced blobs on the next run;
use runtime volumes for state that should persist only on the host.

For expensive repeated runs, `rmtx exec --keep-workspace 2h -- <command>` keeps
that run workspace on the host until its TTL expires and prints the workspace
lease id to stderr. Reuse it with
`rmtx exec --reuse-workspace ws_... --keep-workspace 2h -- <command>`. Reused
workspaces apply only client-side manifest changes before the command, avoiding
full rehydration for unchanged large data. If sync or sync-back fails, the
workspace lease is marked dirty and cannot be reused. List or delete leases with
`rmtx context workspaces list --current` and
`rmtx context workspaces delete --current ws_...`.

Remote commands receive these rmtx environment variables:

- `RMTX=1`: command is running under rmtx.
- `RMTX_RUNNER=host`: command is running on the rmtx host.
- `RMTX_WORKSPACE`: host workspace path visible to the command.
- `RMTX_CONTEXT_ID`: rmtx context id.
- `RMTX_CPU_COUNT`: host logical CPU count.
- `RMTX_MEMORY_AVAILABLE_BYTES`: host available memory in bytes when the
  command starts.

## Isolated runtimes

By default, `rmtx` still runs commands directly on the host context workspace.
Add `runtime` to run commands inside an OCI image pulled by `rmtx` itself
without Docker or Podman:

```json
{
  "runtime": {
    "type": "oci",
    "image": "docker.io/library/ubuntu:24.04",
    "pull_policy": "if_missing",
    "workdir": "/workspace",
    "network": "host",
    "user": "root",
    "wsl_distro": "Ubuntu-24.04",
    "gpu": "none",
    "setup": {
      "image_commands": [
        "apt-get update",
        "apt-get install -y build-essential nodejs npm",
        "npm install -g pnpm"
      ],
      "context_commands": [
        "pnpm install --frozen-lockfile"
      ],
      "context_inputs": [
        "package.json",
        "pnpm-lock.yaml"
      ]
    },
    "volumes": [
      { "name": "pnpm-store", "target": "/pnpm/store" },
      { "name": "npm-cache", "target": "/root/.npm" }
    ]
  }
}
```

Runtime defaults:

- `pull_policy`: `if_missing`; use `always` to refresh registry metadata and
  `never` to require the image to already exist in the local cache.
- `workdir`: `/workspace`.
- `network`: `host`; set `none` to isolate networking.
- `user`: `root` only in v1.
- `gpu`: `none`; set `nvidia` to require NVIDIA devices.
- `wsl_distro` (Windows hosts only): required. Distro name passed to WSL.

Runtime storage has three roles:

- Image/rootfs: base OS and global tools. Use `setup.image_commands` for system
  packages and global CLIs. These commands run inside the isolated rootfs and
  never install anything onto the host OS.
- Workspace: synced project copy, mounted at `runtime.workdir`. Source files
  and outputs that should come back to the client belong here and are governed
  by `sync_back`.
- Volumes: persistent host-side context artifacts mounted inside the runtime.
  Volumes never enter the sync manifest, never upload/download, and never
  sync back. Use them for dependency caches, package stores, build caches,
  local DB files, model caches, and other state that should survive runs but
  stay on the host.

`setup.context_commands` run after workspace sync and before the requested
command. When `setup.context_inputs` is set, `rmtx` hashes those workspace files
and reruns context setup only when they change. If `context_inputs` is omitted,
context setup runs every command.

Synced file blobs, OCI image blobs, manifests, and refs are stored once in host
global caches, while contexts keep references to the data they use.
On Windows OCI runs, hot runtime data (synced blobs, OCI cache, workspaces,
volumes, prepared rootfs, and setup cache) is stored inside the configured WSL
distro under `${XDG_STATE_HOME:-$HOME/.local/state}/rmtx`; host identity,
pairing, trust, and update data stay in the Windows host state directory.
Context artifact commands show the project-owned view and list total bytes:

```bash
rmtx context artifacts list --current
rmtx context artifacts prune --current
rmtx context artifacts delete --current --volume pnpm-store
rmtx cache prune
```

`rmtx context delete --current` removes that context workspace, volumes,
prepared runtime references, and global cache data that has no remaining context
references. Prepared runtime metadata tracks the current runtime only, so older
rootfs variants stop pinning OCI data when runtime config changes. `rmtx cache
prune` can also remove global cache data with no remaining context references,
old update installs, and stale Windows WSL staged rootfs copies. Runs prune
unreferenced synced file blobs after tracked manifests change.

Linux hosts use rootless user, mount, PID, IPC, and UTS namespaces for OCI
execution. `network=none` adds a network namespace. Windows hosts delegate OCI
execution to WSL2 through `wsl.exe`. Set `runtime.wsl_distro` in the project
config; rmtx uses that distro and auto-installs it if missing.
Windows OCI runtime storage uses that distro's Linux filesystem by default, so
commands avoid `/mnt/c` hot paths. If a context changes WSL distro, rmtx drops
the old runtime data for that context and syncs into the new distro.

`gpu=nvidia` requires NVIDIA/WSL GPU devices and fails clearly when unavailable.
Linux binds `/dev/nvidia*`, NVIDIA driver libraries discovered through
`ldconfig`, common CUDA driver paths, and `nvidia-smi` when present. Windows
WSL binds `/dev/dxg` and `/usr/lib/wsl/lib`. This is enough for common CUDA
images, but it is not full `nvidia-container-runtime` parity.

To ignore everything under a specific directory, use `dir/**`. For example,
`"ignore": ["data/cache/**"]` skips every file and subdirectory under
`data/cache`. A trailing slash also works, so `"ignore": ["data/cache/"]`
means the same directory tree.

When `ignore_gitignore` is `true`, patterns from the project root `.gitignore`
are added to every mount. Negated `.gitignore` patterns (`!path`) are ignored
because `rmtx` ignore rules only exclude files.

## Common commands

```bash
rmtx exec --host 192.168.1.42:33221 -- go run ./cmd/api
rmtx exec --tty -- bash
rmtx ping
rmtx stats
rmtx context list
rmtx context delete --current
rmtx context prune --older-than 168h
rmtx context workspaces list --current
rmtx context artifacts list --current
rmtx cache prune
```

## Notes

- Contexts keep manifests and shared blobs on the host; default workspaces are
  scratch copies cleaned after each run. `--keep-workspace` keeps opt-in
  workspace leases until TTL expiry.
- OCI prepared rootfs data is a per-context runtime cache. Runtime config changes
  replace older prepared rootfs refs; context delete removes the cache.
- Discovery uses UDP broadcast on port `33222`; hosts also send outbound announcements so clients can discover Windows hosts even when inbound UDP is blocked.
- If direct TCP to the host is blocked, `rmtx` can fall back to a reverse LAN connection where the host dials back to the client.
- Client state is stored in `~/.rmtx/state.json`.
- Interactive TTY mode is supported on Linux hosts/clients.

## Test

```bash
go test ./...
```
