# rmtx

> [!WARNING]
> `rmtx` is still **experimental**. Expect rough edges, breaking changes, and incomplete docs.

`rmtx` runs commands on a nearby host machine while keeping a persistent context on that host.

## Quick overview

- `rmtx host`: run the host service.
- `rmtx init`: discover host, create local config, pair client.
- `rmtx <command> ...` or `rmtx exec -- <command> ...`: run a command remotely in the current context.
- `rmtx pair`: pair a client with a host.
- `rmtx ping`: verify host connectivity/auth.
- `rmtx context ...`: list/delete/prune host contexts.

## Install

```bash
go install github.com/manuel-huez/rmtx/cmd/rmtx@latest
rmtx version
```

## Build from source

```bash
go build ./cmd/rmtx
```

## Minimal setup

Start host:

```bash
./rmtx host --listen :33221
```

In your project on the client, initialize once:

```bash
./rmtx init
./rmtx go test ./...
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
metadata or content changed are sent back to the client.

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
rmtx context list
rmtx context delete --current
rmtx context prune --older-than 168h
```

## Notes

- Context workspaces are persistent on the host, so repeated runs are faster.
- Discovery uses UDP broadcast on port `33222`; hosts also send outbound announcements so clients can discover Windows hosts even when inbound UDP is blocked.
- If direct TCP to the host is blocked, `rmtx` can fall back to a reverse LAN connection where the host dials back to the client.
- Client state is stored in `~/.rmtx/state.json`.
- Interactive TTY mode is supported on Linux hosts/clients.

## Test

```bash
go test ./...
```
