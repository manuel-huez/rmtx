# rmtx

`rmtx` runs commands on a nearby host machine and keeps a persistent execution context on that host.

It has two runtime modes:

- `rmtx host`: starts the host service on a machine that should execute commands.
- `rmtx <command> ...` or `rmtx exec -- <command> ...`: runs a command remotely inside the current context.

## What changed

The execution model is now context-based:

- A local config file is **required** for remote execution.
- The config file identifies the current execution context.
- The host keeps a persistent directory for each context instead of creating a temporary workspace for every run.
- Subsequent commands reuse the same host-side context directory, so unchanged files are not re-uploaded and host-local build artifacts can survive between runs.
- The client can list, delete, and prune contexts on the host.
- `rmtx ping` can check whether the host is online.
- Interactive TTY execution is supported on Linux hosts/clients.

## What it does

- Discovers a host quickly on the local network with UDP broadcast discovery.
- Reads a project config file from the current directory or any parent directory.
- Synchronizes selected files/directories into a persistent context directory on the host before execution.
- Uses a content-addressed blob cache on the host, so unchanged file contents are not re-uploaded.
- Streams command output back to the client while the command is running.
- Synchronizes tracked file changes back to the client after the command finishes.
- Preserves excluded or host-generated files inside the host context between runs.

## Building

```bash
go build ./cmd/rmtx
```

## Installing (Go)

Install the CLI directly from the module:

```bash
go install github.com/manuel-huez/rmtx/cmd/rmtx@latest
```

Make sure your Go bin directory is on `PATH` (usually `$(go env GOPATH)/bin` or `~/go/bin`), then verify:

```bash
rmtx version
```

## Running

On the host machine:

```bash
./rmtx host --listen :33221
./rmtx host pair-code
```

On the client machine, inside a project directory with a config file:

```bash
./rmtx pair --code 123456
./rmtx go test ./...
```

With explicit flags:

```bash
./rmtx exec --host 192.168.1.42:33221 -- go run ./cmd/api
./rmtx exec --tty -- bash
./rmtx ping --host 192.168.1.42:33221
./rmtx context list --host 192.168.1.42:33221
./rmtx context delete --current
./rmtx context prune --older-than 168h
```

## Config file lookup

The client searches upward from the current directory for:

- `.rmtx.json`
- `rmtx.json`

Remote execution now requires one of these config files.

## Config format

Example:

```json
{
  "version": 1,
  "context": {
    "name": "my-api"
  },
  "host": "192.168.1.42:33221",
  "tls": {
    "host_fingerprint": "sha256:..."
  },
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

- `context.name`: optional stable logical name for the host-side context. When omitted, the context is derived from the local project root path.
- `host`: optional explicit `host:port`. If omitted, discovery is used unless disabled.
- `tls.host_fingerprint`: optional pinned host identity fingerprint. Useful when pairing or targeting a fixed host.
- `mounts`: files/directories to include in the remote context workspace.
- `mounts[].exclude`: glob-like ignore patterns. `**` is supported.
- `env.forward`: environment variable names to copy from the client into the remote command environment.
- `discovery.enabled`: enable/disable automatic host discovery.
- `discovery.service`: logical discovery service name. Defaults to `rmtx`.
- `discovery.timeout`: discovery timeout, e.g. `500ms`, `1s`.

## Persistent contexts

Each context is stored on the host under the host state directory:

```text
<state-dir>/contexts/<context-id>/workspace
```

That workspace remains alive between runs. `rmtx` only applies tracked changes from the client into that workspace, so excluded paths and host-generated files can persist.

This makes repeated commands much cheaper for long-lived contexts such as build, dependency, or toolchain environments.

## Context management

List contexts on the host:

```bash
rmtx context list
```

Delete the current context derived from the local config:

```bash
rmtx context delete --current
```

Delete specific contexts by id:

```bash
rmtx context delete <context-id> [<context-id>...]
```

Prune contexts by age or delete everything:

```bash
rmtx context prune --older-than 168h
rmtx context prune --all
```

## Host health check

Check whether the host is reachable and authenticated:

```bash
rmtx ping
```

The command returns the host identity, version, address, current context count, and timestamp.

## Discovery

Discovery uses UDP broadcast on port `33222`.

The host listens for discovery queries and replies with its TCP execution port, host name, OS, and host identity fingerprint. Discovery helps the user choose a host; trust is still enforced by TLS fingerprint verification.

## Authentication

`rmtx` uses TLS for transport encryption and host identity, plus client certificates for machine/user identity.

- Host startup creates a local CA and server certificate in the host state directory.
- `rmtx host pair-code` creates a short-lived single-use pairing code.
- `rmtx pair --code ...` requires the host identity fingerprint from `tls.host_fingerprint` or `--fingerprint`, discovers or targets the host, verifies that fingerprint, sends a CSR, and stores the issued client cert/key in `~/.rmtx/state.json`.
- Subsequent `exec`, `ping`, and `context` commands use mutual TLS automatically.

## Client state

Global client identity and known hosts are stored in:

```bash
~/.rmtx/state.json
```

The directory is created with restrictive permissions.

## Stdin / TTY behavior

- Piped or redirected stdin is forwarded.
- Interactive terminals automatically use a remote PTY when local stdin/stdout are terminals.
- You can force or disable TTY mode with `rmtx exec --tty` or `rmtx exec --no-tty`.
- Linux is supported for the PTY/raw-terminal path. Other platforms fall back to non-TTY execution.

## Tests

```bash
go test ./...
```

The repository includes:

- config search/default/context-id tests,
- manifest/exclude/blob-cache tests,
- an end-to-end test that starts a real host, reuses a persistent context, forwards environment variables, verifies sync-back behavior, checks `ping`, lists contexts, and deletes the context.

## Repository layout

- `cmd/rmtx`: primary CLI entrypoint.
- `internal/app`: CLI orchestration and config/discovery wiring.
- `internal/client`: client-side sync, execution flow, TTY handling, and control commands.
- `internal/host`: host-side server, persistent context management, and execution flow.
- `internal/syncfs`: manifesting, hashing, blob-cache, and sync-back helpers.
- `internal/protocol`: framed transport and wire messages.
- `internal/config`: config loading, parent-directory lookup, and context identity helpers.
- `internal/discovery`: LAN discovery responder/query logic.

## Example workflow

```bash
# host machine
./rmtx host
./rmtx host pair-code

# client machine, inside a Go project
cat > .rmtx.json <<'JSON'
{
  "version": 1,
  "context": {
    "name": "go-project"
  },
  "tls": {
    "host_fingerprint": "sha256:..."
  },
  "mounts": [
    {"path": ".", "exclude": [".git/**", "cache/**"]}
  ],
  "env": {
    "forward": ["GOPRIVATE"]
  }
}
JSON

./rmtx pair --code 123456
./rmtx go test ./...
./rmtx context list
```
