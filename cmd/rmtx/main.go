package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
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
	"github.com/manuel-huez/rmtx/internal/version"
	"github.com/manuel-huez/rmtx/internal/wslconfig"
)

const exitUsage = 2
const tabWriterTabWidth = 8
const tabWriterPadding = 2
const defaultPairCodeTTL = 5 * time.Minute
const commandPrune = "prune"

var errSelectionCancelled = errors.New("selection cancelled")

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
	if len(os.Args) > 1 && os.Args[1] == "__rmtx-oci-child" {
		os.Exit(host.RunOCIChild(os.Args[2:]))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:])

	cancel()
	os.Exit(code)
}

//nolint:cyclop // Top-level command dispatch is intentionally explicit.
func run(ctx context.Context, args []string) int {
	if len(args) == 0 {
		printUsage(os.Stderr)
		return exitUsage
	}

	switch args[0] {
	case "version", "--version", "-v":
		_, _ = fmt.Fprintln(os.Stdout, version.String())
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
	case "cache":
		return runCache(ctx, args[1:])
	case "wsl":
		return runWSL(ctx, args[1:])
	case "help", "--help", "-h":
		return runHelp(args[1:])
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
				TTYMode:    app.TTYDisable,
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
		var restartRequest *host.RestartRequestedError
		if errors.As(err, &restartRequest) {
			if restartErr := restartHostProcess(restartRequest.Executable, args); restartErr != nil {
				fmt.Fprintln(os.Stderr, "error: restart host:", restartErr)

				return 1
			}

			return 0
		}

		if errors.Is(err, host.ErrRestartRequested) {
			if restartErr := restartHostProcess("", args); restartErr != nil {
				fmt.Fprintln(os.Stderr, "error: restart host:", restartErr)

				return 1
			}

			return 0
		}

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
		Stderr:           os.Stderr,
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
	case commandPrune:
		return runContextPrune(ctx, args[1:])
	case "artifacts":
		return runContextArtifacts(ctx, args[1:])
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

func runContextArtifacts(ctx context.Context, args []string) int {
	if len(args) == 0 {
		return runContextArtifactsList(ctx, nil)
	}

	switch args[0] {
	case "list", "ls":
		return runContextArtifactsList(ctx, args[1:])
	case commandPrune:
		return runContextArtifactsPrune(ctx, args[1:])
	case "delete", "rm", "remove":
		return runContextArtifactsDelete(ctx, args[1:])
	default:
		return runContextArtifactsList(ctx, args)
	}
}

func runContextArtifactsList(ctx context.Context, args []string) int {
	return runContextArtifactsCommand(
		ctx,
		args,
		"list",
		func(_ *app.ContextArtifactsParams, _ *flag.FlagSet) {},
		func(result client.ContextArtifactsResult) { printArtifacts(result.Artifacts) },
	)
}

func runContextArtifactsPrune(ctx context.Context, args []string) int {
	return runContextArtifactsCommand(
		ctx,
		args,
		commandPrune,
		func(params *app.ContextArtifactsParams, _ *flag.FlagSet) { params.Prune = true },
		func(result client.ContextArtifactsResult) { printDeletedArtifacts(result.Deleted) },
	)
}

func runContextArtifactsDelete(ctx context.Context, args []string) int {
	return runContextArtifactsCommand(
		ctx,
		args,
		"delete",
		func(params *app.ContextArtifactsParams, fs *flag.FlagSet) {
			fs.String("volume", "", "volume name to delete")

			params.Delete = true
		},
		func(result client.ContextArtifactsResult) { printDeletedArtifacts(result.Deleted) },
	)
}

func runContextArtifactsCommand(
	ctx context.Context,
	args []string,
	subcommand string,
	configure func(*app.ContextArtifactsParams, *flag.FlagSet),
	printResult func(client.ContextArtifactsResult),
) int {
	fs := flag.NewFlagSet("rmtx context artifacts "+subcommand, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	remote := bindRemoteFlags(fs)
	current := fs.Bool("current", false, "use the current context")
	contextID := fs.String("context", "", "context id")

	artifactParams := app.ContextArtifactsParams{}
	configure(&artifactParams, fs)

	params, cwd, code := parseRemoteFlagsAndCWD(fs, args, remote)
	if code != 0 {
		return code
	}

	artifactParams.AddressOverride = params.AddressOverride
	artifactParams.ConfigPath = params.ConfigPath
	artifactParams.DiscoveryTimeout = params.DiscoveryTimeout
	artifactParams.Stderr = params.Stderr
	artifactParams.ContextID = *contextID
	artifactParams.Current = *current || *contextID == ""

	if volume := fs.Lookup("volume"); volume != nil {
		artifactParams.Volume = volume.Value.String()
	}

	result, err := app.RunContextArtifacts(ctx, cwd, artifactParams)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	printResult(result)

	return 0
}

func runCache(ctx context.Context, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "error: cache requires prune")
		return exitUsage
	}

	if args[0] != commandPrune {
		fmt.Fprintln(os.Stderr, "error: cache supports only prune")
		return exitUsage
	}

	args = args[1:]

	fs := flag.NewFlagSet("rmtx cache prune", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	remote := bindRemoteFlags(fs)

	params, cwd, code := parseRemoteFlagsAndCWD(fs, args, remote)
	if code != 0 {
		return code
	}

	result, err := app.RunCachePrune(ctx, cwd, params)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	for _, artifact := range result.Deleted {
		_, _ = fmt.Fprintf(
			os.Stdout,
			"deleted\t%s\t%s\t%s\n",
			artifact.Kind,
			artifact.Ref,
			artifact.Path,
		)
	}

	_, _ = fmt.Fprintf(os.Stdout, "deleted\t%d\tbytes=%d\n", len(result.Deleted), result.Bytes)

	return 0
}

//nolint:cyclop // Interactive command keeps validation and side effects in visible order.
func runWSL(ctx context.Context, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "error: wsl requires config")
		return exitUsage
	}

	if args[0] != "config" {
		fmt.Fprintln(os.Stderr, "error: wsl supports only config")
		return exitUsage
	}

	fs := flag.NewFlagSet("rmtx wsl config", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	profileFlag := fs.String("profile", "", "profile to apply: 50 or 100")
	pathFlag := fs.String("path", "", "path to .wslconfig")
	yes := fs.Bool("yes", false, "apply without confirmation")
	noRestart := fs.Bool("no-restart", false, "do not run wsl --shutdown after writing")
	dryRun := fs.Bool("dry-run", false, "print proposed settings without writing")
	if err := fs.Parse(args[1:]); err != nil {
		return exitUsage
	}

	specs, err := wslconfig.DetectSystemSpecs()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	path := *pathFlag
	if strings.TrimSpace(path) == "" {
		path, err = wslconfig.DefaultPath()
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			return 1
		}
	}

	file, err := wslconfig.Read(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	profiles, err := wslconfig.Profiles(specs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	printWSLConfigStatus(os.Stdout, path, specs, file, profiles)

	input := bufio.NewReader(os.Stdin)
	selected, err := selectWSLProfile(*profileFlag, profiles, input, os.Stdout)
	if errors.Is(err, errSelectionCancelled) {
		_, _ = fmt.Fprintln(os.Stdout, "cancelled")
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	next := wslconfig.Apply(file.Content, wslconfig.SectionWSL2, selected.Settings)
	_, _ = fmt.Fprintf(
		os.Stdout,
		"proposed\tprocessors=%s\tmemory=%s\n",
		selected.Settings["processors"],
		selected.Settings["memory"],
	)

	if *dryRun {
		return 0
	}

	if !*yes && !confirm(input, os.Stdout, "Apply changes to "+path+"? [y/N]: ") {
		_, _ = fmt.Fprintln(os.Stdout, "cancelled")
		return 0
	}

	if err := wslconfig.Write(path, next); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	_, _ = fmt.Fprintln(os.Stdout, "updated\t"+path)

	if *noRestart {
		_, _ = fmt.Fprintln(os.Stdout, "restart\tskipped")
		return 0
	}

	restart := *yes || confirm(input, os.Stdout, "Run wsl --shutdown now? [y/N]: ")
	if !restart {
		_, _ = fmt.Fprintln(os.Stdout, "restart\tskipped")
		return 0
	}

	if err := wslconfig.Shutdown(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}

	_, _ = fmt.Fprintln(os.Stdout, "restart\twsl shutdown complete")

	return 0
}

func printWSLConfigStatus(
	w io.Writer,
	path string,
	specs wslconfig.SystemSpecs,
	file wslconfig.File,
	profiles []wslconfig.Profile,
) {
	_, _ = fmt.Fprintf(
		w,
		"system\tprocessors=%d\tmemory=%s\n",
		specs.LogicalProcessors,
		formatGiB(specs.TotalMemoryBytes),
	)
	_, _ = fmt.Fprintf(w, "config\t%s\texists=%t\n", path, file.Exists)
	_, _ = fmt.Fprintf(
		w,
		"current\tprocessors=%s\tmemory=%s\n",
		settingOrDefault(file.Settings, "processors", fmt.Sprintf("default:%d", specs.LogicalProcessors)),
		settingOrDefault(file.Settings, "memory", "default:50%"),
	)

	_, _ = fmt.Fprintln(
		w,
		"gpu\t.wslconfig has no global GPU key; WSL GPU defaults enabled, rmtx OCI uses runtime.gpu=nvidia",
	)

	for i, profile := range profiles {
		_, _ = fmt.Fprintf(
			w,
			"option\t%d\t%s\tprocessors=%s\tmemory=%s\n",
			i+1,
			profile.Name,
			profile.Settings["processors"],
			profile.Settings["memory"],
		)
	}
}

func selectWSLProfile(
	value string,
	profiles []wslconfig.Profile,
	in io.Reader,
	out io.Writer,
) (*wslconfig.Profile, error) {
	value = strings.TrimSpace(value)
	if value != "" {
		for i := range profiles {
			if strings.EqualFold(value, profiles[i].Name) || strings.TrimSuffix(profiles[i].Name, "%") == value {
				return &profiles[i], nil
			}
		}

		return nil, fmt.Errorf("unknown profile %q", value)
	}

	_, _ = fmt.Fprint(out, "Select profile [1-2, q]: ")
	line, err := readLine(in)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(strings.TrimSpace(line)) {
	case "q", "quit", "cancel", "":
		return nil, errSelectionCancelled
	case "1":
		return &profiles[0], nil
	case "2":
		return &profiles[1], nil
	default:
		return nil, fmt.Errorf("invalid selection %q", strings.TrimSpace(line))
	}
}

func confirm(in io.Reader, out io.Writer, prompt string) bool {
	_, _ = fmt.Fprint(out, prompt)
	line, err := readLine(in)
	if err != nil {
		return false
	}

	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true
	default:
		return false
	}
}

func readLine(in io.Reader) (string, error) {
	reader, ok := in.(*bufio.Reader)
	if !ok {
		reader = bufio.NewReader(in)
	}
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}

	return strings.TrimSpace(line), nil
}

func settingOrDefault(settings map[string]string, key string, fallback string) string {
	if value := strings.TrimSpace(settings[key]); value != "" {
		return value
	}

	return fallback
}

func formatGiB(bytes uint64) string {
	return fmt.Sprintf("%dGB", bytes/wslconfig.OneGiB)
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

	if force {
		return app.TTYForce, nil
	}

	return app.TTYDisable, nil
}

func emptyFallback(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}

	return value
}

func runHelp(args []string) int {
	if len(args) == 0 {
		printUsage(os.Stdout)
		return 0
	}

	switch args[0] {
	case "config", ".rmtx.json", "rmtx.json":
		printConfigHelp(os.Stdout)
		return 0
	case "commands", "command", "cli":
		printUsage(os.Stdout)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "error: unknown help topic %q\n", args[0])
		fmt.Fprintln(os.Stderr, "topics: commands, config")
		return exitUsage
	}
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
  rmtx context [list|delete|prune|artifacts] [flags]
  rmtx cache prune [flags]
  rmtx wsl config [flags]
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
  rmtx context artifacts list --current
  rmtx cache prune
  rmtx wsl config

Help topics:
  rmtx help commands       full command reference
  rmtx help config         full .rmtx.json schema and defaults

How rmtx works:
  1. Run "rmtx host" on a host machine. It listens on TCP 33221 by default and
     advertises itself on LAN discovery service "rmtx".
  2. Run "rmtx init" in a client project. It discovers or uses --host, verifies
     host TLS fingerprint, writes .rmtx.json, and pairs a client identity.
  3. Run "rmtx <command> [args...]" or "rmtx exec -- <command> [args...]".
     rmtx syncs configured mounts to a persistent host context, runs the command
     in that context, streams stdin/stdout/stderr, then syncs changed files back.

Config lookup:
  Remote commands search upward from the current directory for .rmtx.json, then
  rmtx.json. Use --config PATH to override. "rmtx init --config PATH" creates a
  config at PATH. "rmtx help config" documents every supported field.

Command reference:
  rmtx host [--listen ADDR] [--state-dir DIR] [--name NAME]
            [--service NAME] [--no-discovery]
      Start host service. Defaults: --listen :33221, --service rmtx.

  rmtx host pair-code [--state-dir DIR] [--ttl DURATION]
      Print one-time pairing code plus host fingerprint. Default ttl: 5m.
      Output: code=<code> host=<name> fingerprint=sha256:<hex> expires=<rfc3339>

  rmtx init [--host ADDR] [--fingerprint sha256:<hex>] [--config PATH]
            [--code CODE] [--label LABEL] [--select INDEX]
            [--discovery-timeout DURATION]
      Create project config and pair client. Discovery chooses host unless
      --host is set. --fingerprint is required for manual init without trusted
      discovery result. Output: initialized<TAB><config><TAB><addr><TAB><fp>

  rmtx pair [--host ADDR] [--fingerprint sha256:<hex>] [--config PATH]
            [--code CODE] [--label LABEL] [--select INDEX]
            [--discovery-timeout DURATION]
      Pair or re-pair current client with configured/discovered host.
      Output: paired<TAB><name><TAB><addr><TAB><fingerprint>

  rmtx exec [--host ADDR] [--config PATH] [--discovery-timeout DURATION]
            [--tty|--no-tty] -- <command> [args...]
      Run command remotely. "rmtx <command> [args...]" is shorthand for exec
      with TTY disabled. Use --tty for interactive shells/programs.

  rmtx ping [--host ADDR] [--config PATH] [--discovery-timeout DURATION]
      Verify host reachability, TLS fingerprint, and pairing.
      Output: online<TAB><name><TAB><addr><TAB>version=... contexts=...

  rmtx context list [--host ADDR] [--config PATH] [--discovery-timeout DURATION]
      List contexts on host. Columns: ID, NAME, UPDATED, ACTIVE, WORKSPACE.

  rmtx context delete [--current] [ID ...] [--host ADDR] [--config PATH]
      Delete context IDs. With no IDs, deletes current config context.

  rmtx context prune (--all|--older-than DURATION) [--host ADDR] [--config PATH]
      Delete old/all contexts.

  rmtx context artifacts list [--current|--context ID] [remote flags]
      List context artifacts. Columns: KIND, NAME, REF, SIZE, PATH, DETAIL.

  rmtx context artifacts prune [--current|--context ID] [remote flags]
      Delete unreferenced artifacts for context.

  rmtx context artifacts delete [--current|--context ID] --volume NAME [remote flags]
      Delete named persistent runtime volume.

  rmtx cache prune [--host ADDR] [--config PATH] [--discovery-timeout DURATION]
      Delete host global OCI cache data with no remaining context refs.

  rmtx wsl config [--profile 50|100] [--yes] [--no-restart]
                  [--path PATH] [--dry-run]
      Windows only. Show system CPU/RAM, show current user-profile .wslconfig
      [wsl2] processors/memory settings, offer half and full profiles, write
      selected settings, then optionally run "wsl --shutdown".

Remote resolution order:
  --host ADDR, RMTX_HOST env var, config host, LAN discovery.
  Host ports default to 33221 when omitted.

State files:
  Client pairings live in ~/.rmtx/state.json.
  Host state defaults to a platform app-data directory unless --state-dir set.

Exit codes:
  0 success. 1 runtime/error. 2 usage/invalid flags.
`); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "failed to print usage:", err)
	}
}

func printConfigHelp(f *os.File) {
	if _, err := fmt.Fprint(f, `rmtx config file reference (.rmtx.json or rmtx.json)

Lookup:
  rmtx searches current directory, then parents, for .rmtx.json then rmtx.json.
  --config PATH overrides lookup. Paths inside config are relative to config dir.
  "rmtx exec" and "rmtx <command>" require config. Utility commands such as
  ping/context/pair can also use --host, env, pairing state, or discovery.

Minimal config:
  {
    "version": 1,
    "context": { "name": "my-project" },
    "host": "192.168.1.42:33221",
    "tls": { "host_fingerprint": "sha256:..." },
    "mounts": [{ "path": "." }]
  }

Full schema:
  {
    "version": 1,
    "context": { "name": "my-project" },
    "host": "192.168.1.42:33221",
    "tls": { "host_fingerprint": "sha256:..." },
    "discovery": {
      "enabled": true,
      "service": "rmtx",
      "timeout": "750ms"
    },
    "mounts": [
      { "path": ".", "exclude": ["tmp/**"] }
    ],
    "sync_back": ["coverage/", "generated/report.json"],
    "ignore": [".git/**", "node_modules/**", "dist/**"],
    "ignore_gitignore": true,
    "env": { "forward": ["GITHUB_TOKEN", "AWS_PROFILE"] },
    "runtime": {
      "type": "oci",
      "image": "docker.io/library/ubuntu:24.04",
      "pull_policy": "if_missing",
      "workdir": "/workspace",
      "network": "host",
      "user": "root",
      "gpu": "none",
      "wsl_distro": "Ubuntu-24.04",
      "setup": {
        "image_commands": ["apt-get update"],
        "context_commands": ["npm ci"],
        "context_inputs": ["package.json", "package-lock.json"]
      },
      "volumes": [
        { "name": "npm-cache", "target": "/root/.npm" }
      ]
    }
  }

Top-level fields:
  version              Config version. Default: 1.
  context.name         Stable context name on host. Default: config dir basename.
  host                 Preferred host address. Port defaults to 33221 if omitted.
  tls.host_fingerprint Expected host TLS cert fingerprint. Required for trust.
  discovery.enabled    Enable LAN discovery. Default: true.
  discovery.service    Discovery service name. Default: rmtx.
  discovery.timeout    Go duration for discovery wait, e.g. 750ms, 2s. Default: 750ms.
  mounts               Client paths synced to host. Default: [{ "path": "." }].
  mounts[].path        File/dir relative to config dir, or absolute path.
  mounts[].exclude     Ignore patterns for that mount only.
  ignore               Global ignore patterns for every mount.
  ignore_gitignore     Add root .gitignore patterns to ignores. Default: false.
  sync_back            Paths/globs copied back after command. Default: all mounts.
  env.forward          Env var names copied from client to remote command.
  runtime              Optional isolated OCI runtime. Omit to run directly on host.

Ignore and sync patterns:
  Patterns use slash paths. Directory pattern dir/** excludes descendants.
  Trailing slash means same tree, e.g. data/cache/ equals data/cache/**.
  Negated .gitignore patterns are ignored. rmtx rules only exclude.
  sync_back paths are relative to project root. Directory paths include children.

Runtime fields:
  runtime.type         Supported: oci. Required when runtime configured.
  runtime.image        OCI image ref. Required for oci.
  runtime.pull_policy  if_missing, always, never. Default: if_missing.
  runtime.workdir      Workspace mount path inside runtime. Default: /workspace.
  runtime.network      host or none. Default: host.
  runtime.user         root only in v1. Default: root.
  runtime.gpu          none or nvidia. Default: none.
  runtime.wsl_distro   Windows host WSL distro for OCI runs.
  setup.image_commands Commands run once per prepared image/rootfs.
  setup.context_commands Commands run after workspace sync before requested command.
  setup.context_inputs Files hashed to decide when context_commands rerun.
  volumes[].name       Persistent volume name. No slash, not "." or "..".
  volumes[].target     Absolute runtime path for persistent host-side volume.

Runtime behavior:
  image_commands affect isolated rootfs, not host OS.
  context_commands run in synced workspace. If context_inputs omitted, they run
  before every command. If set, they rerun only when listed inputs change.
  volumes persist on host, do not sync, do not enter manifests, do not sync back.

Validation:
  Unsupported top-level token/token_env fields fail; use "rmtx pair".
  runtime target paths must be absolute and must not contain ".." path elements.
  unsupported runtime values fail before command execution.
`); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "failed to print config help:", err)
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
		Stderr:           os.Stderr,
	}
}

func (r remoteFlags) deleteParams() app.ContextDeleteParams {
	return app.ContextDeleteParams{
		AddressOverride:  *r.hostAddr,
		ConfigPath:       *r.cfgPath,
		DiscoveryTimeout: *r.discoveryTimeout,
		Stderr:           os.Stderr,
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

func printArtifacts(artifacts []client.ContextArtifact) {
	tw := tabwriter.NewWriter(os.Stdout, 0, tabWriterTabWidth, tabWriterPadding, ' ', 0)

	_, _ = fmt.Fprintln(tw, "KIND\tNAME\tREF\tSIZE\tPATH\tDETAIL")

	for _, artifact := range artifacts {
		_, _ = fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%d\t%s\t%s\n",
			artifact.Kind,
			artifact.Name,
			artifact.Ref,
			artifact.Size,
			artifact.Path,
			artifact.Detail,
		)
	}

	_ = tw.Flush()
}

func printDeletedArtifacts(deleted []client.ContextArtifact) {
	for _, artifact := range deleted {
		_, _ = fmt.Fprintf(
			os.Stdout,
			"deleted\t%s\t%s\t%s\n",
			artifact.Kind,
			emptyFallback(artifact.Name, artifact.Ref),
			artifact.Path,
		)
	}

	if len(deleted) == 0 {
		_, _ = fmt.Fprintln(os.Stdout, "deleted\t0")
	}
}
