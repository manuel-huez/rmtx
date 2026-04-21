package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/manuel-huez/rmtx/internal/client"
	"github.com/manuel-huez/rmtx/internal/config"
	"github.com/manuel-huez/rmtx/internal/discovery"
	"github.com/manuel-huez/rmtx/internal/host"
	"github.com/manuel-huez/rmtx/internal/syncfs"
	"github.com/manuel-huez/rmtx/internal/terminal"
)

type TTYMode int

const (
	TTYAuto TTYMode = iota
	TTYForce
	TTYDisable
)

type ExecParams struct {
	AddressOverride  string
	ConfigPath       string
	TokenOverride    string
	DiscoveryTimeout time.Duration
	Command          []string
	Stdout           io.Writer
	Stderr           io.Writer
	Stdin            io.Reader
	ForwardStdin     bool
	StdinFile        *os.File
	StdoutFile       *os.File
	StderrFile       *os.File
	TTYMode          TTYMode
}

type RemoteParams struct {
	AddressOverride  string
	ConfigPath       string
	TokenOverride    string
	DiscoveryTimeout time.Duration
}

type ContextDeleteParams struct {
	AddressOverride  string
	ConfigPath       string
	TokenOverride    string
	DiscoveryTimeout time.Duration
	IDs              []string
	All              bool
	OlderThan        time.Duration
	Current          bool
}

type HostParams struct {
	ListenAddr       string
	StateDir         string
	Token            string
	AdvertiseName    string
	DiscoveryService string
	DisableDiscovery bool
	Logger           *log.Logger
}

func RunExec(ctx context.Context, cwd string, params ExecParams) (int, error) {
	loaded, err := config.ResolveRequired(cwd, params.ConfigPath)
	if err != nil {
		return 1, err
	}

	cfg := config.WithDefaults(loaded.Config)

	address, err := resolveHost(ctx, cfg, params.AddressOverride, params.DiscoveryTimeout)
	if err != nil {
		return 1, err
	}

	token := strings.TrimSpace(params.TokenOverride)
	if token == "" {
		token = strings.TrimSpace(cfg.TokenValue())
	}

	if token == "" {
		return 1, fmt.Errorf("no token configured; set %s or use --token", tokenEnvName(cfg))
	}

	useTTY, err := resolveTTY(params)
	if err != nil {
		return 1, err
	}

	mounts := make([]syncfs.MountSpec, 0, len(cfg.Mounts))
	for _, mount := range cfg.Mounts {
		mounts = append(
			mounts,
			syncfs.MountSpec{Path: mount.Path, Exclude: append([]string(nil), mount.Exclude...)},
		)
	}

	forwardStdin := params.ForwardStdin || useTTY || ShouldForwardStdin(params.StdinFile)

	return client.Run(ctx, client.ExecOptions{
		Address:      address,
		Token:        token,
		Root:         loaded.Root,
		CWD:          cwd,
		Command:      params.Command,
		Mounts:       mounts,
		ForwardEnv:   append([]string(nil), cfg.Env.Forward...),
		Stdout:       params.Stdout,
		Stderr:       params.Stderr,
		Stdin:        params.Stdin,
		StdinFile:    params.StdinFile,
		StdoutFile:   params.StdoutFile,
		StderrFile:   params.StderrFile,
		ForwardStdin: forwardStdin,
		Project:      filepath.Base(loaded.Root),
		ContextID:    loaded.ContextID(),
		ContextName:  loaded.ContextName(),
		TTY:          useTTY,
	})
}

func RunPing(ctx context.Context, cwd string, params RemoteParams) (client.PingInfo, error) {
	address, token, _, err := resolveRemoteTarget(ctx, cwd, params)
	if err != nil {
		return client.PingInfo{}, err
	}

	return client.Ping(ctx, client.RemoteOptions{Address: address, Token: token})
}

func RunListContexts(
	ctx context.Context,
	cwd string,
	params RemoteParams,
) ([]client.ContextInfo, error) {
	address, token, _, err := resolveRemoteTarget(ctx, cwd, params)
	if err != nil {
		return nil, err
	}

	return client.ListContexts(ctx, client.RemoteOptions{Address: address, Token: token})
}

func RunDeleteContexts(
	ctx context.Context,
	cwd string,
	params ContextDeleteParams,
) (client.DeleteContextsResult, error) {
	address, token, loaded, err := resolveRemoteTarget(
		ctx,
		cwd,
		RemoteParams{
			AddressOverride:  params.AddressOverride,
			ConfigPath:       params.ConfigPath,
			TokenOverride:    params.TokenOverride,
			DiscoveryTimeout: params.DiscoveryTimeout,
		},
	)
	if err != nil {
		return client.DeleteContextsResult{}, err
	}

	ids := append([]string(nil), params.IDs...)
	if params.Current {
		if loaded == nil {
			loaded, err = config.ResolveRequired(cwd, params.ConfigPath)
			if err != nil {
				return client.DeleteContextsResult{}, err
			}
		}

		ids = append(ids, loaded.ContextID())
	}

	req := client.DeleteContextsOptions{
		Remote: client.RemoteOptions{Address: address, Token: token},
		IDs:    ids,
		All:    params.All,
	}

	if params.OlderThan > 0 {
		req.OlderThan = params.OlderThan.String()
	}

	return client.DeleteContexts(ctx, req)
}

func RunHost(ctx context.Context, params HostParams) error {
	token := strings.TrimSpace(params.Token)
	if token == "" {
		token = os.Getenv("RMTX_TOKEN")
	}

	server, err := host.New(host.Options{
		ListenAddr:       params.ListenAddr,
		Token:            token,
		StateDir:         params.StateDir,
		AdvertiseName:    params.AdvertiseName,
		DiscoveryService: params.DiscoveryService,
		DisableDiscovery: params.DisableDiscovery,
		Logger:           params.Logger,
	})
	if err != nil {
		return err
	}

	return server.Serve(ctx)
}

func ShouldForwardStdin(f *os.File) bool {
	if f == nil {
		return false
	}

	info, err := f.Stat()
	if err != nil {
		return false
	}

	return info.Mode()&os.ModeCharDevice == 0
}

func ShouldUseTTY(stdin, stdout, stderr *os.File) bool {
	_ = stderr

	return terminal.IsTerminal(stdin) && terminal.IsTerminal(stdout)
}

func resolveTTY(params ExecParams) (bool, error) {
	switch params.TTYMode {
	case TTYDisable:
		return false, nil
	case TTYForce:
		if !ShouldUseTTY(params.StdinFile, params.StdoutFile, params.StderrFile) {
			return false, errors.New("TTY requested but local stdin/stdout are not terminals")
		}

		return true, nil
	case TTYAuto:
		return ShouldUseTTY(params.StdinFile, params.StdoutFile, params.StderrFile), nil
	}

	return false, fmt.Errorf("unsupported tty mode %q", params.TTYMode)
}

func resolveRemoteTarget(
	ctx context.Context,
	cwd string,
	params RemoteParams,
) (string, string, *config.Loaded, error) {
	var (
		loaded *config.Loaded
		err    error
	)

	if strings.TrimSpace(params.ConfigPath) != "" {
		loaded, err = config.Load(params.ConfigPath)
		if err != nil {
			return "", "", nil, err
		}
	} else {
		loaded, err = config.Search(cwd)
		if err != nil && !errors.Is(err, config.ErrConfigNotFound) {
			return "", "", nil, err
		}

		if errors.Is(err, config.ErrConfigNotFound) {
			loaded = nil
		}
	}

	cfg := config.Default()
	if loaded != nil {
		cfg = config.WithDefaults(loaded.Config)
	} else {
		cfg = config.WithDefaults(cfg)
	}

	address, err := resolveHost(ctx, cfg, params.AddressOverride, params.DiscoveryTimeout)
	if err != nil {
		return "", "", loaded, err
	}

	token := strings.TrimSpace(params.TokenOverride)
	if token == "" {
		if loaded != nil {
			token = strings.TrimSpace(cfg.TokenValue())
		} else {
			token = strings.TrimSpace(os.Getenv("RMTX_TOKEN"))
		}
	}

	if token == "" {
		return "", "", loaded, fmt.Errorf(
			"no token configured; set %s or use --token",
			tokenEnvName(cfg),
		)
	}

	return address, token, loaded, nil
}

func resolveHost(
	ctx context.Context,
	cfg config.Config,
	override string,
	timeout time.Duration,
) (string, error) {
	if override = strings.TrimSpace(override); override != "" {
		return discovery.NormalizeAddress(override, config.DefaultPort), nil
	}

	if env := strings.TrimSpace(os.Getenv("RMTX_HOST")); env != "" {
		return discovery.NormalizeAddress(env, config.DefaultPort), nil
	}

	if cfgHost := strings.TrimSpace(cfg.Host); cfgHost != "" {
		return discovery.NormalizeAddress(cfgHost, config.DefaultPort), nil
	}

	if !cfg.DiscoveryEnabled() {
		return "", errors.New("no host configured and discovery disabled")
	}

	d := timeout
	if d <= 0 {
		d = cfg.DiscoveryTimeout()
	}

	result, err := discovery.DiscoverOne(ctx, cfg.Discovery.Service, d)
	if err != nil {
		return "", err
	}

	return result.Address, nil
}

func tokenEnvName(cfg config.Config) string {
	if strings.TrimSpace(cfg.TokenEnv) != "" {
		return cfg.TokenEnv
	}

	return "RMTX_TOKEN"
}
