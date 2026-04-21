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
	loaded, err := config.Resolve(cwd, params.ConfigPath)
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

	mounts := make([]syncfs.MountSpec, 0, len(cfg.Mounts))
	for _, mount := range cfg.Mounts {
		mounts = append(
			mounts,
			syncfs.MountSpec{Path: mount.Path, Exclude: append([]string(nil), mount.Exclude...)},
		)
	}

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
		ForwardStdin: params.ForwardStdin,
		Project:      filepath.Base(loaded.Root),
	})
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
