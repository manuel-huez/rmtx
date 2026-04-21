package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/manuel-huez/rmtx/internal/app"
	"github.com/manuel-huez/rmtx/internal/config"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	os.Exit(run(ctx, os.Args[1:]))
}

func run(ctx context.Context, args []string) int {
	if len(args) == 0 {
		printUsage(os.Stderr)
		return 2
	}

	switch args[0] {
	case "host":
		return runHost(ctx, args[1:])
	case "exec":
		return runExecWithFlags(ctx, args[1:])
	case "help", "--help", "-h":
		printUsage(os.Stdout)
		return 0
	default:
		return runExec(
			ctx,
			app.ExecParams{
				Command:      args,
				Stdout:       os.Stdout,
				Stderr:       os.Stderr,
				Stdin:        os.Stdin,
				ForwardStdin: app.ShouldForwardStdin(os.Stdin),
			},
		)
	}
}

func runHost(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("rmtx host", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	listen := fs.String("listen", fmt.Sprintf(":%d", config.DefaultPort), "listen address")
	token := fs.String("token", "", "shared token; defaults to RMTX_TOKEN")
	stateDir := fs.String("state-dir", "", "state directory for blobs and session workspaces")
	name := fs.String("name", "", "discovery instance name")
	service := fs.String("service", config.DefaultService, "discovery service name")

	noDiscovery := fs.Bool("no-discovery", false, "disable LAN discovery")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	logger := log.New(os.Stderr, "rmtx: ", log.LstdFlags)
	if err := app.RunHost(
		ctx,
		app.HostParams{
			ListenAddr:       *listen,
			StateDir:         *stateDir,
			Token:            *token,
			AdvertiseName:    *name,
			DiscoveryService: *service,
			DisableDiscovery: *noDiscovery,
			Logger:           logger,
		},
	); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	return 0
}

func runExecWithFlags(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("rmtx exec", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	hostAddr := fs.String("host", "", "host address, e.g. 192.168.1.20:33221")
	cfgPath := fs.String("config", "", "path to .rmtx.json")
	token := fs.String("token", "", "shared token; defaults to RMTX_TOKEN or config token_env")

	discoveryTimeout := fs.Duration("discovery-timeout", 0, "override discovery timeout")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	if fs.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "error: missing command")
		return 2
	}

	return runExec(
		ctx,
		app.ExecParams{
			AddressOverride:  *hostAddr,
			ConfigPath:       *cfgPath,
			TokenOverride:    *token,
			DiscoveryTimeout: *discoveryTimeout,
			Command:          fs.Args(),
			Stdout:           os.Stdout,
			Stderr:           os.Stderr,
			Stdin:            os.Stdin,
			ForwardStdin:     app.ShouldForwardStdin(os.Stdin),
		},
	)
}

func runExec(ctx context.Context, params app.ExecParams) int {
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	code, err := app.RunExec(ctx, cwd, params)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)

		if code == 0 {
			return 1
		}

		return code
	}

	return code
}

func printUsage(f *os.File) {
	fmt.Fprint(f, `rmtx runs local commands on a host machine over the local network.

Usage:
  rmtx host [flags]
  rmtx exec [flags] -- <command> [args...]
  rmtx <command> [args...]

Examples:
  rmtx host --listen :33221
  rmtx go run ./cmd/api
  rmtx exec --host 192.168.1.42:33221 -- go test ./...
`)
}
