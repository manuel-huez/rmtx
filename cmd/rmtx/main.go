package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/manuel-huez/rmtx/internal/app"
	"github.com/manuel-huez/rmtx/internal/client"
	"github.com/manuel-huez/rmtx/internal/config"
	"github.com/manuel-huez/rmtx/internal/host"
)

const exitUsage = 2
const tabWriterTabWidth = 8
const tabWriterPadding = 2
const defaultPairCodeTTL = 5 * time.Minute

type remoteFlags struct {
	hostAddr         *string
	cfgPath          *string
	discoveryTimeout *time.Duration
}

type pairLikeFlags struct {
	hostAddr         *string
	cfgPath          *string
	discoveryTimeout *time.Duration
	code             *string
	label            *string
	selectIndex      *int
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:])

	cancel()
	os.Exit(code)
}

func run(ctx context.Context, args []string) int {
	if len(args) == 0 {
		printUsage(os.Stderr)
		return exitUsage
	}

	switch args[0] {
	case "version", "--version", "-v":
		_, _ = fmt.Fprintln(os.Stdout, host.Version)
		return 0
	case "host":
		return runHost(ctx, args[1:])
	case "exec":
		return runExecWithFlags(ctx, args[1:])
	case "init":
		return runInit(ctx, args[1:])
	case "ping":
		return runPing(ctx, args[1:])
	case "pair":
		return runPair(ctx, args[1:])
	case "context", "contexts":
		return runContext(ctx, args[1:])
	case "help", "--help", "-h":
		printUsage(os.Stdout)
		return 0
	default:
		return runExec(
			ctx,
			app.ExecParams{
				Command:    args,
				Stdout:     os.Stdout,
				Stderr:     os.Stderr,
				Stdin:      os.Stdin,
				StdinFile:  os.Stdin,
				StdoutFile: os.Stdout,
				StderrFile: os.Stderr,
				TTYMode:    app.TTYAuto,
			},
		)
	}
}

func runHost(ctx context.Context, args []string) int {
	if len(args) > 0 && args[0] == "pair-code" {
		return runHostPairCode(args[1:])
	}

	fs := flag.NewFlagSet("rmtx host", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	listen := fs.String("listen", fmt.Sprintf(":%d", config.DefaultPort), "listen address")
	stateDir := fs.String("state-dir", "", "state directory for blobs and contexts")
	name := fs.String("name", "", "discovery instance name")
	service := fs.String("service", config.DefaultService, "discovery service name")

	noDiscovery := fs.Bool("no-discovery", false, "disable LAN discovery")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	logger := log.New(os.Stderr, "rmtx: ", log.LstdFlags)
	if err := app.RunHost(
		ctx,
		app.HostParams{
			ListenAddr:       *listen,
			StateDir:         *stateDir,
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

func runHostPairCode(args []string) int {
	fs := flag.NewFlagSet("rmtx host pair-code", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	stateDir := fs.String("state-dir", "", "state directory for host data")

	ttl := fs.Duration("ttl", defaultPairCodeTTL, "pairing code ttl")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	info, err := app.RunHostPairCode(app.HostPairCodeParams{StateDir: *stateDir, TTL: *ttl})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	_, _ = fmt.Fprintf(
		os.Stdout,
		"code=%s host=%s fingerprint=%s expires=%s\n",
		info.Code,
		info.HostName,
		info.HostFingerprint,
		info.ExpiresAt.Format(time.RFC3339),
	)

	return 0
}

func runExecWithFlags(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("rmtx exec", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	hostAddr := fs.String("host", "", "host address, e.g. 192.168.1.20:33221")
	cfgPath := fs.String("config", "", "path to .rmtx.json")
	discoveryTimeout := fs.Duration("discovery-timeout", 0, "override discovery timeout")
	ttyFlag := fs.Bool("tty", false, "force interactive TTY")

	noTTYFlag := fs.Bool("no-tty", false, "disable interactive TTY")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	if fs.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "error: missing command")
		return exitUsage
	}

	ttyMode, err := resolveTTYMode(*ttyFlag, *noTTYFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return exitUsage
	}

	return runExec(
		ctx,
		app.ExecParams{
			AddressOverride:  *hostAddr,
			ConfigPath:       *cfgPath,
			DiscoveryTimeout: *discoveryTimeout,
			Command:          fs.Args(),
			Stdout:           os.Stdout,
			Stderr:           os.Stderr,
			Stdin:            os.Stdin,
			StdinFile:        os.Stdin,
			StdoutFile:       os.Stdout,
			StderrFile:       os.Stderr,
			TTYMode:          ttyMode,
		},
	)
}

func runPing(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("rmtx ping", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	remote := bindRemoteFlags(fs)

	params, cwd, code := parseRemoteFlagsAndCWD(fs, args, remote)
	if code != 0 {
		return code
	}

	info, err := app.RunPing(
		ctx,
		cwd,
		params,
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	_, _ = fmt.Fprintf(
		os.Stdout,
		"online\t%s\t%s\tversion=%s\tcontexts=%d\tfingerprint=%s\tat=%s\n",
		emptyFallback(info.Name, "rmtx-host"),
		info.Address,
		info.Version,
		info.ContextCount,
		info.Fingerprint,
		info.Now.Format(time.RFC3339),
	)

	return 0
}

func runPair(ctx context.Context, args []string) int {
	cwd, params, code := preparePairLikeExecution(
		args,
		"rmtx pair",
		"host address, e.g. 192.168.1.20:33221",
		"pairing code; omit to request one from host",
		"path to .rmtx.json",
		"expected host TLS fingerprint; required unless config tls.host_fingerprint is set",
	)
	if code != 0 {
		return code
	}

	return executePairLike(
		ctx,
		cwd,
		params,
		func(ctx context.Context, cwd string, params app.PairParams) (string, error) {
			record, err := app.RunPair(ctx, cwd, params)
			if err != nil {
				return "", err
			}

			return fmt.Sprintf(
				"paired\t%s\t%s\t%s",
				record.Name,
				record.Address,
				record.Fingerprint,
			), nil
		},
	)
}

func runInit(ctx context.Context, args []string) int {
	cwd, params, code := preparePairLikeExecution(
		args,
		"rmtx init",
		"preferred discovered host address, e.g. 192.168.1.20:33221",
		"pairing code; omit to request one from selected host",
		"path to create .rmtx.json",
		"expected host TLS fingerprint; required for manual init when discovery is unavailable",
	)
	if code != 0 {
		return code
	}

	return executePairLike(
		ctx,
		cwd,
		params,
		func(ctx context.Context, cwd string, params app.PairParams) (string, error) {
			result, err := app.RunInit(ctx, cwd, app.InitParams(params))
			if err != nil {
				return "", err
			}

			return fmt.Sprintf(
				"initialized\t%s\t%s\t%s",
				result.ConfigPath,
				result.Host.Address,
				result.Host.Fingerprint,
			), nil
		},
	)
}

func bindPairLikeFlags(
	fs *flag.FlagSet,
	hostHelp string,
	codeHelp string,
	configHelp string,
) pairLikeFlags {
	return pairLikeFlags{
		hostAddr:         fs.String("host", "", hostHelp),
		cfgPath:          fs.String("config", "", configHelp),
		discoveryTimeout: fs.Duration("discovery-timeout", 0, "override discovery timeout"),
		code:             fs.String("code", "", codeHelp),
		label:            fs.String("label", "", "client label"),
		selectIndex:      fs.Int("select", 0, "discovered host index"),
	}
}

func pairLikeParams(flags pairLikeFlags) app.PairParams {
	return app.PairParams{
		AddressOverride:  *flags.hostAddr,
		ConfigPath:       *flags.cfgPath,
		DiscoveryTimeout: *flags.discoveryTimeout,
		Code:             *flags.code,
		ClientLabel:      *flags.label,
		SelectionIndex:   *flags.selectIndex,
	}
}

func preparePairLikeCommand(
	args []string,
	fs *flag.FlagSet,
	common pairLikeFlags,
	fingerprint *string,
) (string, app.PairParams, int) {
	if err := fs.Parse(args); err != nil {
		return "", app.PairParams{}, exitUsage
	}

	cwd, code := mustGetwdOrExit()
	if code != 0 {
		return "", app.PairParams{}, code
	}

	params := pairLikeParams(common)
	if fingerprint != nil {
		params.Fingerprint = *fingerprint
	}

	params.Stdin = os.Stdin
	params.Stdout = os.Stdout

	return cwd, params, 0
}

func preparePairLikeExecution(
	args []string,
	command string,
	hostHelp string,
	codeHelp string,
	configHelp string,
	fingerprintHelp string,
) (string, app.PairParams, int) {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	common := bindPairLikeFlags(fs, hostHelp, codeHelp, configHelp)
	fingerprint := fs.String("fingerprint", "", fingerprintHelp)

	return preparePairLikeCommand(args, fs, common, fingerprint)
}

func executePairLike(
	ctx context.Context,
	cwd string,
	params app.PairParams,
	run func(context.Context, string, app.PairParams) (string, error),
) int {
	output, err := run(ctx, cwd, params)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	_, _ = fmt.Fprintln(os.Stdout, output)

	return 0
}

func mustGetwdOrExit() (string, int) {
	cwd, err := mustGetwd()
	if err != nil {
		printErr(err)
		return "", 1
	}

	return cwd, 0
}

func runContext(ctx context.Context, args []string) int {
	if len(args) == 0 {
		return runContextList(ctx, nil)
	}

	switch args[0] {
	case "list", "ls":
		return runContextList(ctx, args[1:])
	case "delete", "rm", "remove":
		return runContextDelete(ctx, args[1:])
	case "prune":
		return runContextPrune(ctx, args[1:])
	default:
		return runContextList(ctx, args)
	}
}

func runContextList(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("rmtx context list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	remote := bindRemoteFlags(fs)

	params, cwd, code := parseRemoteFlagsAndCWD(fs, args, remote)
	if code != 0 {
		return code
	}

	contexts, err := app.RunListContexts(ctx, cwd, params)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, tabWriterTabWidth, tabWriterPadding, ' ', 0)
	_, _ = fmt.Fprintln(tw, "ID\tNAME\tUPDATED\tACTIVE\tWORKSPACE")

	for _, context := range contexts {
		updated := "-"
		if !context.UpdatedAt.IsZero() {
			updated = context.UpdatedAt.Format(time.RFC3339)
		}

		active := "no"
		if context.Active {
			active = "yes"
		}

		_, _ = fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\n",
			context.ID,
			context.Name,
			updated,
			active,
			context.Workspace,
		)
	}

	_ = tw.Flush()

	return 0
}

func runContextDelete(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("rmtx context delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	remote := bindRemoteFlags(fs)

	current := fs.Bool("current", false, "delete the current context from the local config")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	ids := fs.Args()
	useCurrent := *current || len(ids) == 0

	cwd, err := mustGetwd()
	if err != nil {
		printErr(err)
		return 1
	}

	params := remote.deleteParams()
	params.IDs = ids
	params.Current = useCurrent

	result, err := app.RunDeleteContexts(ctx, cwd, params)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	printDeletedContexts(result.Deleted)

	if len(result.NotFound) > 0 {
		fmt.Fprintf(os.Stderr, "not found: %s\n", strings.Join(result.NotFound, ", "))
		return 1
	}

	return 0
}

func runContextPrune(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("rmtx context prune", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	remote := bindRemoteFlags(fs)
	olderThan := fs.Duration("older-than", 0, "delete contexts last updated before this duration")

	all := fs.Bool("all", false, "delete all contexts")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	if !*all && *olderThan <= 0 {
		fmt.Fprintln(os.Stderr, "error: prune requires --all or --older-than")
		return exitUsage
	}

	cwd, err := mustGetwd()
	if err != nil {
		printErr(err)
		return 1
	}

	params := remote.deleteParams()
	params.All = *all
	params.OlderThan = *olderThan

	result, err := app.RunDeleteContexts(ctx, cwd, params)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	printDeletedContexts(result.Deleted)

	if len(result.Deleted) == 0 {
		_, _ = fmt.Fprintln(os.Stdout, "deleted\t0")
	}

	return 0
}

func runExec(ctx context.Context, params app.ExecParams) int {
	cwd, err := mustGetwd()
	if err != nil {
		printErr(err)
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

func resolveTTYMode(force, disable bool) (app.TTYMode, error) {
	if force && disable {
		return app.TTYAuto, errors.New("--tty and --no-tty cannot be used together")
	}

	switch {
	case force:
		return app.TTYForce, nil
	case disable:
		return app.TTYDisable, nil
	default:
		return app.TTYDisable, nil
	}
}

func emptyFallback(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}

	return value
}

func printUsage(f *os.File) {
	if _, err := fmt.Fprint(f, `rmtx runs local commands on a host machine over the local network.

Usage:
  rmtx host [flags]
  rmtx host pair-code [flags]
  rmtx exec [flags] -- <command> [args...]
  rmtx init [flags]
  rmtx pair [flags]
  rmtx ping [flags]
  rmtx context [list|delete|prune] [flags]
  rmtx version
  rmtx <command> [args...]

Examples:
  rmtx host --listen :33221
  rmtx init
  rmtx init --host 192.168.1.42:33221 --fingerprint sha256:...
  rmtx pair
  rmtx host pair-code
  rmtx go test ./...
  rmtx pair --code 123456
  rmtx exec --tty -- bash
  rmtx ping
  rmtx version
  rmtx context list
  rmtx context delete --current
`); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "failed to print usage:", err)
	}
}

func bindRemoteFlags(fs *flag.FlagSet) remoteFlags {
	return remoteFlags{
		hostAddr:         fs.String("host", "", "host address, e.g. 192.168.1.20:33221"),
		cfgPath:          fs.String("config", "", "optional path to .rmtx.json"),
		discoveryTimeout: fs.Duration("discovery-timeout", 0, "override discovery timeout"),
	}
}

func (r remoteFlags) params() app.RemoteParams {
	return app.RemoteParams{
		AddressOverride:  *r.hostAddr,
		ConfigPath:       *r.cfgPath,
		DiscoveryTimeout: *r.discoveryTimeout,
	}
}

func (r remoteFlags) deleteParams() app.ContextDeleteParams {
	return app.ContextDeleteParams{
		AddressOverride:  *r.hostAddr,
		ConfigPath:       *r.cfgPath,
		DiscoveryTimeout: *r.discoveryTimeout,
	}
}

func parseRemoteFlagsAndCWD(
	fs *flag.FlagSet,
	args []string,
	remote remoteFlags,
) (app.RemoteParams, string, int) {
	if err := fs.Parse(args); err != nil {
		return app.RemoteParams{}, "", exitUsage
	}

	cwd, err := mustGetwd()
	if err != nil {
		printErr(err)
		return app.RemoteParams{}, "", 1
	}

	return remote.params(), cwd, 0
}

func mustGetwd() (string, error) {
	return os.Getwd()
}

func printErr(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
}

func printDeletedContexts(deleted []client.ContextInfo) {
	for _, context := range deleted {
		_, _ = fmt.Fprintf(os.Stdout, "deleted\t%s\t%s\n", context.ID, context.Name)
	}
}
