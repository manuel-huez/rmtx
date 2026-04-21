# rmtx

`rmtx` is a small Go tool that runs commands on a nearby host machine and streams the command output back to the local client.

It has two runtime modes:

- `rmtx host`: starts the host service on a machine that should execute commands.
- `rmtx <command> ...` or `rmtx exec -- <command> ...`: runs a command remotely.

## What it does

- Discovers a host quickly on the local network with UDP broadcast discovery.
- Reads a project config file from the current directory or any parent directory.
- Synchronizes selected files/directories to the host before execution.
- Uses a content-addressed blob cache on the host, so unchanged files are not re-uploaded on later runs.
- Streams `stdout` and `stderr` back to the client while the command is running.
- Synchronizes changed files back to the client after the command finishes.

## Important behavior

The current implementation does **not** provide a live FUSE/NFS-style mount.

Instead, it performs:

1. a pre-run sync to an isolated workspace on the host,
2. remote command execution in that workspace,
3. a post-run sync of changed files back to the client.

For repeated runs on the same host, the host-side blob cache avoids re-sending unchanged file contents.

## Building

```bash
go build ./cmd/rmtx
```

## Running

On the host machine:

```bash
export RMTX_TOKEN='replace-me'
./rmtx host --listen :33221
```

On the client machine, inside a project directory:

```bash
export RMTX_TOKEN='replace-me'
./rmtx go test ./...
```

Or with explicit flags:

```bash
./rmtx exec --host 192.168.1.42:33221 -- go run ./cmd/api
```

## Config file lookup

The client searches upward from the current working directory for:

- `.rmtx.json`
- `rmtx.json`

If no config file is found, it defaults to mounting `.` and trying host discovery.

## Config format

Example:

```json
{
  "version": 1,
  "host": "192.168.1.42:33221",
  "token_env": "RMTX_TOKEN",
  "mounts": [
    {
      "path": ".",
      "exclude": [".git/**", "node_modules/**", "tmp/**"]
    }
  ],
  "env": {
    "forward": ["GOPRIVATE", "CGO_ENABLED", "AWS_PROFILE"]
  },
  "discovery": {
    "enabled": true,
    "service": "rmtx",
    "timeout": "750ms"
  }
}
```

### Fields

- `host`: optional explicit `host:port`. If omitted, discovery is used unless disabled.
- `token_env`: environment variable used to read the shared token. Defaults to `RMTX_TOKEN`.
- `mounts`: files/directories to include in the remote workspace.
- `mounts[].exclude`: glob-like ignore patterns. `**` is supported.
- `env.forward`: environment variable names to copy from the client into the remote command environment.
- `discovery.enabled`: enable/disable automatic host discovery.
- `discovery.service`: logical discovery service name. Defaults to `rmtx`.
- `discovery.timeout`: discovery timeout, e.g. `500ms`, `1s`.

## Discovery

Discovery uses UDP broadcast on port `33222`.

The host listens for discovery queries and replies with its TCP execution port. The client sends broadcast queries and expects exactly one matching host; if multiple hosts respond, discovery fails with a list of candidates so the caller can pin a specific host.

## Authentication

Host and client share a token. The token itself is not sent directly during the handshake; the client replies to a server nonce with an HMAC.

Typical setup:

```bash
export RMTX_TOKEN='replace-me'
```

## Stdin / TTY behavior

- Non-interactive stdin (pipes/files) is forwarded.
- Interactive TTY emulation is not implemented yet.

Commands that need a real terminal are outside the current scope of this implementation.

## Tests

```bash
go test ./...
```

The repository includes:

- config search/default tests,
- manifest/exclude/blob-cache tests,
- an end-to-end test that starts a real host, runs a remote command, forwards environment variables, and verifies file changes sync back.

## Repository layout

- `cmd/rmtx`: CLI entrypoint.
- `internal/app`: CLI orchestration and config/discovery wiring.
- `internal/client`: client-side sync and execution flow.
- `internal/host`: host-side server and execution flow.
- `internal/syncfs`: manifesting, hashing, blob cache, and sync-back helpers.
- `internal/protocol`: framed transport and wire messages.
- `internal/config`: config loading and parent-directory lookup.
- `internal/discovery`: LAN discovery responder/query logic.

## Example workflow

```bash
# host machine
export RMTX_TOKEN='replace-me'
./rmtx host

# client machine, inside a Go project
cat > .rmtx.json <<'JSON'
{
  "version": 1,
  "mounts": [
    {"path": ".", "exclude": [".git/**"]}
  ],
  "env": {
    "forward": ["GOPRIVATE"]
  }
}
JSON

export RMTX_TOKEN='replace-me'
./rmtx go test ./...
```
