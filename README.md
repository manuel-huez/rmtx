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
  "ignore": [".git/**", "node_modules/**"]
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
  "ignore": [".git/**", "node_modules/**", "dist/**"]
}
```

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
