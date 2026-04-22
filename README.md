# rmtx

> [!WARNING]
> `rmtx` is still **experimental**. Expect rough edges, breaking changes, and incomplete docs.

`rmtx` runs commands on a nearby host machine while keeping a persistent context on that host.

## Quick overview

- `rmtx host`: run the host service.
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

In your project on the client, create `.rmtx.json`:

```json
{
  "version": 1,
  "context": { "name": "my-project" },
  "tls": { "host_fingerprint": "sha256:..." },
  "mounts": [{ "path": ".", "exclude": [".git/**", "node_modules/**"] }]
}
```

Then pair and run:

```bash
./rmtx pair
./rmtx go test ./...
```

`rmtx pair` asks host to generate a one-time code, prints that code in host terminal, then prompts client to enter it. For non-interactive/manual pairing, `rmtx host pair-code` and `rmtx pair --code ...` still work.

## Config file

`rmtx` looks upward from the current directory for:

- `.rmtx.json`
- `rmtx.json`

A config file is required for remote execution.

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
- Discovery uses UDP broadcast on port `33222`.
- Client state is stored in `~/.rmtx/state.json`.
- Interactive TTY mode is supported on Linux hosts/clients.

## Test

```bash
go test ./...
```
