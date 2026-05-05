package app

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/manuel-huez/rmtx/internal/client"
	"github.com/manuel-huez/rmtx/internal/clientstate"
	"github.com/manuel-huez/rmtx/internal/config"
	"github.com/manuel-huez/rmtx/internal/discovery"
	"github.com/manuel-huez/rmtx/internal/host"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/security"
	"github.com/manuel-huez/rmtx/internal/syncfs"
	"github.com/manuel-huez/rmtx/internal/terminal"
)

type TTYMode int

const (
	TTYAuto TTYMode = iota
	TTYForce
	TTYDisable
)

var (
	discoverAllHosts = discovery.DiscoverAll
	discoverOneHost  = discovery.DiscoverOne
)

const (
	initConfigDirMode  = 0o755
	initConfigFileMode = 0o644
)

type ExecParams struct {
	AddressOverride  string
	ConfigPath       string
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
	DiscoveryTimeout time.Duration
	Stderr           io.Writer
}

type ContextDeleteParams struct {
	AddressOverride  string
	ConfigPath       string
	DiscoveryTimeout time.Duration
	Stderr           io.Writer
	IDs              []string
	All              bool
	OlderThan        time.Duration
	Current          bool
}

type ContextArtifactsParams struct {
	AddressOverride  string
	ConfigPath       string
	DiscoveryTimeout time.Duration
	Stderr           io.Writer
	ContextID        string
	Current          bool
	Prune            bool
	Delete           bool
	Volume           string
}

type HostParams struct {
	ListenAddr       string
	StateDir         string
	AdvertiseName    string
	DiscoveryService string
	DisableDiscovery bool
	Logger           *log.Logger
}

type HostPairCodeParams struct {
	StateDir string
	TTL      time.Duration
}

type PairParams struct {
	AddressOverride  string
	Fingerprint      string
	ConfigPath       string
	DiscoveryTimeout time.Duration
	Stderr           io.Writer
	Code             string
	ClientLabel      string
	SelectionIndex   int
	Stdin            io.Reader
	Stdout           io.Writer
}

type InitParams struct {
	AddressOverride  string
	Fingerprint      string
	ConfigPath       string
	DiscoveryTimeout time.Duration
	Stderr           io.Writer
	Code             string
	ClientLabel      string
	SelectionIndex   int
	Stdin            io.Reader
	Stdout           io.Writer
}

type InitResult struct {
	ConfigPath string
	Host       clientstate.HostRecord
}

func loadPairConfig(cwd, configPath string) (config.Config, error) {
	cfg := config.WithDefaults(config.Default())

	if strings.TrimSpace(configPath) != "" {
		loaded, err := config.Load(configPath)
		if err != nil {
			return config.Config{}, err
		}

		return config.WithDefaults(loaded.Config), nil
	}

	loaded, err := config.Search(cwd)
	switch {
	case err == nil:
		return config.WithDefaults(loaded.Config), nil
	case errors.Is(err, config.ErrConfigNotFound):
		return cfg, nil
	default:
		return config.Config{}, err
	}
}

//nolint:cyclop // Command bootstrap mixes config, pairing, tty, and mount checks in one orchestration path.
func RunExec(ctx context.Context, cwd string, params ExecParams) (int, error) {
	loaded, err := config.ResolveRequired(cwd, params.ConfigPath)
	if err != nil {
		return 1, err
	}

	cfg := config.WithDefaults(loaded.Config)

	target, err := resolveRemoteHost(ctx, cfg, params.AddressOverride, params.DiscoveryTimeout)
	if err != nil {
		return 1, err
	}

	state, hostRecord, err := resolveClientHost(target.Address, target.Fingerprint)
	if err != nil {
		return 1, err
	}

	if hostRecord == nil || !hostRecord.Paired {
		return 1, errors.New("host not paired; run `rmtx pair`")
	}

	clientCertPEM, clientKeyPEM := state.HostCredentials(target.Address, hostRecord.Fingerprint)
	if strings.TrimSpace(clientCertPEM) == "" || strings.TrimSpace(clientKeyPEM) == "" {
		return 1, errors.New("client identity missing; run `rmtx pair`")
	}

	if pinned := strings.TrimSpace(
		cfg.TLS.HostFingerprint,
	); pinned != "" &&
		pinned != hostRecord.Fingerprint {
		return 1, fmt.Errorf(
			"configured host fingerprint %s does not match paired host %s",
			pinned,
			hostRecord.Fingerprint,
		)
	}

	useTTY, err := resolveTTY(params)
	if err != nil {
		return 1, err
	}

	mounts, err := buildMountSpecs(loaded.Root, cfg)
	if err != nil {
		return 1, err
	}

	syncBack := cfg.SyncBack
	if syncBack != nil {
		syncBack = append([]string(nil), syncBack...)
	}

	forwardStdin := params.ForwardStdin || useTTY || ShouldForwardStdin(params.StdinFile)

	return client.Run(ctx, client.ExecOptions{
		Address:          target.Address,
		DiscoveryService: target.DiscoveryService,
		Host:             *hostRecord,
		ClientCertPEM:    []byte(clientCertPEM),
		ClientKeyPEM:     []byte(clientKeyPEM),
		Root:             loaded.Root,
		CWD:              cwd,
		Command:          params.Command,
		Mounts:           mounts,
		SyncBack:         syncBack,
		Runtime:          runtimeSpec(cfg.Runtime),
		ForwardEnv:       append([]string(nil), cfg.Env.Forward...),
		Stdout:           params.Stdout,
		Stderr:           params.Stderr,
		Stdin:            params.Stdin,
		StdinFile:        params.StdinFile,
		StdoutFile:       params.StdoutFile,
		StderrFile:       params.StderrFile,
		ForwardStdin:     forwardStdin,
		Project:          filepath.Base(loaded.Root),
		ContextID:        loaded.ContextID(),
		ContextName:      loaded.ContextName(),
		TTY:              useTTY,
	})
}

func runtimeSpec(runtime config.RuntimeConfig) protocol.RuntimeSpec {
	return protocol.RuntimeSpec{
		Type:       runtime.Type,
		Image:      runtime.Image,
		PullPolicy: runtime.PullPolicy,
		WSLDistro:  runtime.WSLDistro,
		WorkDir:    runtime.WorkDir,
		Network:    runtime.Network,
		User:       runtime.User,
		GPU:        runtime.GPU,
		Setup: protocol.RuntimeSetup{
			ImageCommands:   append([]string(nil), runtime.Setup.ImageCommands...),
			ContextCommands: append([]string(nil), runtime.Setup.ContextCommands...),
			ContextInputs:   append([]string(nil), runtime.Setup.ContextInputs...),
		},
		Volumes: runtimeVolumes(runtime.Volumes),
	}
}

func runtimeVolumes(volumes []config.RuntimeVolume) []protocol.RuntimeVolume {
	return append([]protocol.RuntimeVolume(nil), volumes...)
}

func buildMountSpecs(root string, cfg config.Config) ([]syncfs.MountSpec, error) {
	globalIgnore := append([]string(nil), cfg.Ignore...)
	if cfg.IgnoreGitignore {
		patterns, err := loadGitignorePatterns(root)
		if err != nil {
			return nil, err
		}

		globalIgnore = append(globalIgnore, patterns...)
	}

	mounts := make([]syncfs.MountSpec, 0, len(cfg.Mounts))
	for _, mount := range cfg.Mounts {
		exclude := append([]string(nil), globalIgnore...)
		exclude = append(exclude, mount.Exclude...)
		mounts = append(
			mounts,
			syncfs.MountSpec{Path: mount.Path, Exclude: exclude},
		)
	}

	return mounts, nil
}

func loadGitignorePatterns(root string) ([]string, error) {
	content, err := os.ReadFile(filepath.Join(root, ".gitignore"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, fmt.Errorf("read .gitignore: %w", err)
	}

	var patterns []string

	for _, line := range strings.Split(string(content), "\n") {
		pattern, ok := gitignoreLineToExclude(line)
		if ok {
			patterns = append(patterns, pattern)
		}
	}

	return patterns, nil
}

func gitignoreLineToExclude(line string) (string, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", false
	}

	if strings.HasPrefix(line, `\#`) || strings.HasPrefix(line, `\!`) {
		line = line[1:]
	} else if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
		return "", false
	}

	line = strings.TrimPrefix(line, "/")
	if line == "" {
		return "", false
	}

	dirOnly := strings.HasSuffix(line, "/")

	line = strings.TrimRight(line, "/")
	if line == "" {
		return "", false
	}

	hasSlash := strings.Contains(line, "/")
	if dirOnly {
		line += "/**"
	}

	if !hasSlash {
		return "**/" + line, true
	}

	return line, true
}

func RunPing(ctx context.Context, cwd string, params RemoteParams) (client.PingInfo, error) {
	address, service, hostRecord, _, err := resolveRemoteTarget(ctx, cwd, params)
	if err != nil {
		return client.PingInfo{}, err
	}

	state, err := clientstate.Load()
	if err != nil {
		return client.PingInfo{}, err
	}

	clientCertPEM, clientKeyPEM := state.HostCredentials(address, hostRecord.Fingerprint)

	return client.Ping(ctx, client.RemoteOptions{
		Address:          address,
		DiscoveryService: service,
		Host:             *hostRecord,
		ClientCertPEM:    []byte(clientCertPEM),
		ClientKeyPEM:     []byte(clientKeyPEM),
		Stderr:           params.Stderr,
	})
}

func RunListContexts(
	ctx context.Context,
	cwd string,
	params RemoteParams,
) ([]client.ContextInfo, error) {
	address, service, hostRecord, _, err := resolveRemoteTarget(ctx, cwd, params)
	if err != nil {
		return nil, err
	}

	state, err := clientstate.Load()
	if err != nil {
		return nil, err
	}

	clientCertPEM, clientKeyPEM := state.HostCredentials(address, hostRecord.Fingerprint)

	return client.ListContexts(ctx, client.RemoteOptions{
		Address:          address,
		DiscoveryService: service,
		Host:             *hostRecord,
		ClientCertPEM:    []byte(clientCertPEM),
		ClientKeyPEM:     []byte(clientKeyPEM),
		Stderr:           params.Stderr,
	})
}

func RunDeleteContexts(
	ctx context.Context,
	cwd string,
	params ContextDeleteParams,
) (client.DeleteContextsResult, error) {
	address, service, hostRecord, loaded, err := resolveRemoteTarget(
		ctx,
		cwd,
		RemoteParams{
			AddressOverride:  params.AddressOverride,
			ConfigPath:       params.ConfigPath,
			DiscoveryTimeout: params.DiscoveryTimeout,
			Stderr:           params.Stderr,
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
		Remote: client.RemoteOptions{
			Address:          address,
			DiscoveryService: service,
			Host:             *hostRecord,
			Stderr:           params.Stderr,
		},
		IDs: ids,
		All: params.All,
	}

	state, err := clientstate.Load()
	if err != nil {
		return client.DeleteContextsResult{}, err
	}

	clientCertPEM, clientKeyPEM := state.HostCredentials(address, hostRecord.Fingerprint)
	req.Remote.ClientCertPEM = []byte(clientCertPEM)
	req.Remote.ClientKeyPEM = []byte(clientKeyPEM)

	if params.OlderThan > 0 {
		req.OlderThan = params.OlderThan.String()
	}

	return client.DeleteContexts(ctx, req)
}

func RunContextArtifacts(
	ctx context.Context,
	cwd string,
	params ContextArtifactsParams,
) (client.ContextArtifactsResult, error) {
	address, service, hostRecord, loaded, err := resolveRemoteTarget(
		ctx,
		cwd,
		RemoteParams{
			AddressOverride:  params.AddressOverride,
			ConfigPath:       params.ConfigPath,
			DiscoveryTimeout: params.DiscoveryTimeout,
			Stderr:           params.Stderr,
		},
	)
	if err != nil {
		return client.ContextArtifactsResult{}, err
	}

	contextID := strings.TrimSpace(params.ContextID)
	if params.Current || contextID == "" {
		if loaded == nil {
			loaded, err = config.ResolveRequired(cwd, params.ConfigPath)
			if err != nil {
				return client.ContextArtifactsResult{}, err
			}
		}

		contextID = loaded.ContextID()
	}

	state, err := clientstate.Load()
	if err != nil {
		return client.ContextArtifactsResult{}, err
	}

	clientCertPEM, clientKeyPEM := state.HostCredentials(address, hostRecord.Fingerprint)

	return client.ContextArtifacts(ctx, client.ContextArtifactsOptions{
		Remote: client.RemoteOptions{
			Address:          address,
			DiscoveryService: service,
			Host:             *hostRecord,
			ClientCertPEM:    []byte(clientCertPEM),
			ClientKeyPEM:     []byte(clientKeyPEM),
			Stderr:           params.Stderr,
		},
		ContextID: contextID,
		Prune:     params.Prune,
		Delete:    params.Delete,
		Volume:    params.Volume,
	})
}

func RunCachePrune(
	ctx context.Context,
	cwd string,
	params RemoteParams,
) (client.CachePruneResult, error) {
	address, service, hostRecord, _, err := resolveRemoteTarget(ctx, cwd, params)
	if err != nil {
		return client.CachePruneResult{}, err
	}

	state, err := clientstate.Load()
	if err != nil {
		return client.CachePruneResult{}, err
	}

	clientCertPEM, clientKeyPEM := state.HostCredentials(address, hostRecord.Fingerprint)

	return client.CachePrune(ctx, client.RemoteOptions{
		Address:          address,
		DiscoveryService: service,
		Host:             *hostRecord,
		ClientCertPEM:    []byte(clientCertPEM),
		ClientKeyPEM:     []byte(clientKeyPEM),
		Stderr:           params.Stderr,
	})
}

func RunHost(ctx context.Context, params HostParams) error {
	server, err := host.New(host.Options{
		ListenAddr:       params.ListenAddr,
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

func RunHostPairCode(params HostPairCodeParams) (host.PairCodeInfo, error) {
	return host.CreatePairCodeInfo(params.StateDir, params.TTL)
}

//nolint:cyclop // Init flow discovers pairable hosts, writes config, then completes pairing.
func RunInit(ctx context.Context, cwd string, params InitParams) (InitResult, error) {
	pairParams := PairParams(params)
	preparePairIO(&pairParams)

	configPath, err := resolveInitConfigPath(cwd, params.ConfigPath)
	if err != nil {
		return InitResult{}, err
	}

	cfg := config.WithDefaults(config.Default())

	result, err := resolveInitTarget(ctx, cfg, &pairParams)
	if err != nil {
		return InitResult{}, err
	}

	if err := promptForTrust(&pairParams, result); err != nil {
		return InitResult{}, err
	}

	configHost := ""
	if strings.TrimSpace(params.AddressOverride) != "" {
		configHost = result.Address
	}

	if err := writeInitConfig(configPath, cwd, result.HostFingerprint, configHost); err != nil {
		return InitResult{}, err
	}

	record, err := completePair(ctx, result, &pairParams)
	if err != nil {
		return InitResult{}, err
	}

	return InitResult{ConfigPath: configPath, Host: record}, nil
}

//nolint:cyclop // Pairing flow coordinates config lookup, identity reuse, CSR generation, and state persistence.
func RunPair(ctx context.Context, cwd string, params PairParams) (clientstate.HostRecord, error) {
	preparePairIO(&params)

	cfg, err := loadPairConfig(cwd, params.ConfigPath)
	if err != nil {
		return clientstate.HostRecord{}, err
	}

	result, err := resolvePairTarget(ctx, cfg, &params)
	if err != nil {
		return clientstate.HostRecord{}, err
	}

	return completePair(ctx, result, &params)
}

func completePair(
	ctx context.Context,
	result discovery.Result,
	params *PairParams,
) (clientstate.HostRecord, error) {
	state, err := clientstate.Load()
	if err != nil {
		return clientstate.HostRecord{}, err
	}

	if strings.TrimSpace(state.Data.ClientLabel) == "" {
		state.Data.ClientLabel = clientstate.DefaultClientLabel()
	}

	label := strings.TrimSpace(params.ClientLabel)
	if label == "" {
		label = state.Data.ClientLabel
	}

	existing := existingPairRecord(state, result)
	clientKeyPEM := pairClientKey(state, result, existing)
	previousFingerprint := previousPairFingerprint(existing)

	csrPEM, clientKeyPEM, err := ensurePairCSR(clientKeyPEM, label)
	if err != nil {
		return clientstate.HostRecord{}, err
	}

	code, err := resolvePairCode(ctx, params, result, label)
	if err != nil {
		return clientstate.HostRecord{}, err
	}

	remoteHost := pairRemoteHost(result)

	pairResp, err := client.PairHost(ctx, client.PairOptions{
		Address:             result.Address,
		DiscoveryService:    result.Service,
		Host:                remoteHost,
		Code:                code,
		ClientLabel:         label,
		PreviousFingerprint: previousFingerprint,
		CSRPEM:              csrPEM,
		Stderr:              params.Stderr,
	})
	if err != nil {
		return clientstate.HostRecord{}, err
	}

	state.Data.ClientLabel = label
	record := pairedHostRecord(result, pairResp, clientKeyPEM)
	state.UpsertHost(record)

	if err := state.Save(); err != nil {
		return clientstate.HostRecord{}, err
	}

	return record, nil
}

func existingPairRecord(
	state *clientstate.Loaded,
	result discovery.Result,
) *clientstate.HostRecord {
	existing := state.FindHostByFingerprint(result.HostFingerprint)
	if existing != nil {
		return existing
	}

	return state.FindHostByAddress(result.Address)
}

func pairClientKey(
	state *clientstate.Loaded,
	result discovery.Result,
	existing *clientstate.HostRecord,
) []byte {
	if existing != nil && strings.TrimSpace(existing.ClientKeyPEM) != "" {
		return []byte(existing.ClientKeyPEM)
	}

	_, key := state.HostCredentials(result.Address, result.HostFingerprint)

	return []byte(key)
}

func previousPairFingerprint(existing *clientstate.HostRecord) string {
	if existing == nil {
		return ""
	}

	fingerprint := strings.TrimSpace(existing.LastPairedCert)
	if fingerprint != "" || strings.TrimSpace(existing.ClientCertPEM) == "" {
		return fingerprint
	}

	fingerprint, err := security.FingerprintPEM([]byte(existing.ClientCertPEM))
	if err != nil {
		return ""
	}

	return fingerprint
}

func ensurePairCSR(clientKeyPEM []byte, label string) ([]byte, []byte, error) {
	if len(clientKeyPEM) == 0 {
		_, keyPEM, csrPEM, err := client.GenerateClientIdentity(label)
		if err != nil {
			return nil, nil, err
		}

		return csrPEM, keyPEM, nil
	}

	csrPEM, err := client.GenerateCSR(clientKeyPEM, label)
	if err != nil {
		return nil, nil, err
	}

	return csrPEM, clientKeyPEM, nil
}

func pairRemoteHost(result discovery.Result) clientstate.HostRecord {
	return clientstate.HostRecord{
		Address:     result.Address,
		Name:        result.Instance,
		OS:          result.OS,
		Fingerprint: result.HostFingerprint,
	}
}

func resolvePairCode(
	ctx context.Context,
	params *PairParams,
	result discovery.Result,
	label string,
) (string, error) {
	code := strings.TrimSpace(params.Code)
	if code != "" {
		return code, nil
	}

	pairCodeResp, err := client.RequestPairCode(ctx, client.PairOptions{
		Address:          result.Address,
		DiscoveryService: result.Service,
		Host:             pairRemoteHost(result),
		ClientLabel:      label,
		Stderr:           params.Stderr,
	})
	if err != nil {
		return "", err
	}

	return promptForPairCode(params, pairCodeResp)
}

func pairedHostRecord(
	result discovery.Result,
	pairResp client.PairResult,
	clientKeyPEM []byte,
) clientstate.HostRecord {
	return clientstate.HostRecord{
		Address:        result.Address,
		Name:           result.Instance,
		OS:             result.OS,
		Fingerprint:    result.HostFingerprint,
		Paired:         true,
		LastPairedCert: pairResp.Fingerprint,
		ClientCertPEM:  pairResp.ClientCertPEM,
		ClientKeyPEM:   string(clientKeyPEM),
	}
}

func promptForPairCode(params *PairParams, response client.PairCodeResult) (string, error) {
	preparePairIO(params)

	hostName := empty(response.HostName, "host")

	_, _ = fmt.Fprintf(
		params.Stdout,
		"pair code requested from %s; expires %s\nEnter code: ",
		hostName,
		response.ExpiresAt.Format(time.RFC3339),
	)

	code, err := readPairLine(params, "read pairing code")
	if err != nil {
		return "", err
	}

	if code == "" {
		return "", errors.New("pairing code is required")
	}

	return code, nil
}

func preparePairIO(params *PairParams) {
	if params == nil {
		return
	}

	if params.Stdout == nil {
		params.Stdout = os.Stdout
	}

	if params.Stdin == nil {
		params.Stdin = os.Stdin
	}

	if _, ok := params.Stdin.(*bufio.Reader); ok {
		return
	}

	params.Stdin = bufio.NewReader(params.Stdin)
}

func readPairLine(params *PairParams, errContext string) (string, error) {
	preparePairIO(params)

	reader, ok := params.Stdin.(*bufio.Reader)
	if !ok {
		reader = bufio.NewReader(params.Stdin)
		params.Stdin = reader
	}

	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("%s: %w", errContext, err)
	}

	return strings.TrimSpace(line), nil
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

//nolint:cyclop // Remote resolution joins optional config lookup with paired-state validation.
func resolveRemoteTarget(
	ctx context.Context,
	cwd string,
	params RemoteParams,
) (string, string, *clientstate.HostRecord, *config.Loaded, error) {
	var (
		loaded *config.Loaded
		err    error
	)

	if strings.TrimSpace(params.ConfigPath) != "" {
		loaded, err = config.Load(params.ConfigPath)
		if err != nil {
			return "", "", nil, nil, err
		}
	} else {
		loaded, err = config.Search(cwd)
		if err != nil && !errors.Is(err, config.ErrConfigNotFound) {
			return "", "", nil, nil, err
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

	target, err := resolveRemoteHost(ctx, cfg, params.AddressOverride, params.DiscoveryTimeout)
	if err != nil {
		return "", "", nil, loaded, err
	}

	state, hostRecord, err := resolveClientHost(target.Address, target.Fingerprint)
	if err != nil {
		return "", "", nil, loaded, err
	}

	if hostRecord == nil || !hostRecord.Paired {
		return "", "", nil, loaded, errors.New("host not paired; run `rmtx pair`")
	}

	clientCertPEM, clientKeyPEM := state.HostCredentials(target.Address, hostRecord.Fingerprint)
	if strings.TrimSpace(clientCertPEM) == "" || strings.TrimSpace(clientKeyPEM) == "" {
		return "", "", nil, loaded, errors.New("client identity missing; run `rmtx pair`")
	}

	if pinned := strings.TrimSpace(
		cfg.TLS.HostFingerprint,
	); pinned != "" &&
		pinned != hostRecord.Fingerprint {
		return "", "", nil, loaded, fmt.Errorf(
			"configured host fingerprint %s does not match paired host %s",
			pinned,
			hostRecord.Fingerprint,
		)
	}

	return target.Address, target.DiscoveryService, hostRecord, loaded, nil
}

type resolvedRemoteHost struct {
	Address          string
	DiscoveryService string
	Fingerprint      string
}

//nolint:cyclop,nestif // Resolution order is policy-driven: override, env, config, then discovery variants.
func resolveRemoteHost(
	ctx context.Context,
	cfg config.Config,
	override string,
	timeout time.Duration,
) (resolvedRemoteHost, error) {
	pinnedFingerprint := strings.TrimSpace(cfg.TLS.HostFingerprint)

	if override = strings.TrimSpace(override); override != "" {
		return resolvedRemoteHost{
			Address:          discovery.NormalizeAddress(override, config.DefaultPort),
			DiscoveryService: cfg.Discovery.Service,
			Fingerprint:      pinnedFingerprint,
		}, nil
	}

	if env := strings.TrimSpace(os.Getenv("RMTX_HOST")); env != "" {
		return resolvedRemoteHost{
			Address:          discovery.NormalizeAddress(env, config.DefaultPort),
			DiscoveryService: cfg.Discovery.Service,
			Fingerprint:      pinnedFingerprint,
		}, nil
	}

	if cfgHost := strings.TrimSpace(cfg.Host); cfgHost != "" {
		return resolvedRemoteHost{
			Address:          discovery.NormalizeAddress(cfgHost, config.DefaultPort),
			DiscoveryService: cfg.Discovery.Service,
			Fingerprint:      pinnedFingerprint,
		}, nil
	}

	if !cfg.DiscoveryEnabled() {
		return resolvedRemoteHost{}, errors.New("no host configured and discovery disabled")
	}

	d := timeout
	if d <= 0 {
		d = cfg.DiscoveryTimeout()
	}

	if pinnedFingerprint != "" {
		return discoverPinnedRemoteHost(ctx, cfg, d, pinnedFingerprint)
	}

	result, err := discoverOneHost(ctx, cfg.Discovery.Service, d)
	if err != nil {
		return resolvedRemoteHost{}, err
	}

	return resolvedRemoteHost{
		Address:          result.Address,
		DiscoveryService: cfg.Discovery.Service,
		Fingerprint:      strings.TrimSpace(result.HostFingerprint),
	}, nil
}

func discoverPinnedRemoteHost(
	ctx context.Context,
	cfg config.Config,
	timeout time.Duration,
	pinnedFingerprint string,
) (resolvedRemoteHost, error) {
	results, err := discoverAllHosts(ctx, cfg.Discovery.Service, timeout)
	if err != nil {
		return resolvedRemoteHost{}, err
	}

	if len(results) == 0 {
		return resolvedRemoteHost{}, fmt.Errorf(
			"no host discovered via %s within %s",
			cfg.Discovery.Service,
			timeout,
		)
	}

	preferredAddress, err := preferredAddressForFingerprint(pinnedFingerprint)
	if err != nil {
		return resolvedRemoteHost{}, err
	}

	result, err := selectDiscoveredHost(results, pinnedFingerprint, preferredAddress)
	if err != nil {
		return resolvedRemoteHost{}, err
	}

	return resolvedRemoteHost{
		Address:          result.Address,
		DiscoveryService: cfg.Discovery.Service,
		Fingerprint:      strings.TrimSpace(result.HostFingerprint),
	}, nil
}

func preferredAddressForFingerprint(fingerprint string) (string, error) {
	state, err := clientstate.Load()
	if err != nil {
		return "", err
	}

	record := state.FindHostByFingerprint(fingerprint)
	if record == nil {
		return "", nil
	}

	return strings.TrimSpace(record.Address), nil
}

func selectDiscoveredHost(
	results []discovery.Result,
	pinnedFingerprint string,
	preferredAddress string,
) (discovery.Result, error) {
	filtered := results
	if pinnedFingerprint != "" {
		filtered = make([]discovery.Result, 0, len(results))
		for _, result := range results {
			if strings.TrimSpace(result.HostFingerprint) == pinnedFingerprint {
				filtered = append(filtered, result)
			}
		}

		if len(filtered) == 0 {
			return discovery.Result{}, fmt.Errorf(
				"no discovered host matched fingerprint %s",
				pinnedFingerprint,
			)
		}
	}

	preferredAddress = strings.TrimSpace(preferredAddress)
	if preferredAddress != "" {
		for _, result := range filtered {
			if strings.TrimSpace(result.Address) == preferredAddress {
				return result, nil
			}
		}
	}

	if len(filtered) > 1 {
		candidates := make([]string, 0, len(filtered))
		for _, result := range filtered {
			candidates = append(candidates, result.Address)
		}

		return discovery.Result{}, fmt.Errorf(
			"multiple hosts discovered: %s",
			strings.Join(candidates, ", "),
		)
	}

	return filtered[0], nil
}

func resolveClientHost(
	address, fingerprint string,
) (*clientstate.Loaded, *clientstate.HostRecord, error) {
	state, err := clientstate.Load()
	if err != nil {
		return nil, nil, err
	}

	if record := state.FindHostByFingerprint(fingerprint); record != nil {
		address = strings.TrimSpace(address)
		if address != "" && state.UpdateHostAddress(fingerprint, address) {
			if err := state.Save(); err != nil {
				return nil, nil, err
			}

			record = state.FindHostByFingerprint(fingerprint)
		}

		return state, record, nil
	}

	record := state.FindHostByAddress(address)
	if record == nil {
		return state, nil, nil
	}

	return state, record, nil
}

//nolint:cyclop // Init target resolution mixes discovery, optional filtering, and interactive selection.
func resolveInitTarget(
	ctx context.Context,
	cfg config.Config,
	params *PairParams,
) (discovery.Result, error) {
	preparePairIO(params)

	addressOverride := strings.TrimSpace(params.AddressOverride)
	fingerprint := strings.TrimSpace(params.Fingerprint)

	if addressOverride != "" && fingerprint != "" {
		return discovery.Result{
			Address:         discovery.NormalizeAddress(addressOverride, config.DefaultPort),
			Service:         cfg.Discovery.Service,
			Instance:        "manual-host",
			OS:              "",
			HostFingerprint: fingerprint,
			PairingEnabled:  true,
		}, nil
	}

	timeout := params.DiscoveryTimeout
	if timeout <= 0 {
		timeout = cfg.DiscoveryTimeout()
	}

	results, err := discoverAllHosts(ctx, cfg.Discovery.Service, timeout)
	if err != nil {
		return discovery.Result{}, err
	}

	results = filterPairableResults(results)
	if len(results) == 0 {
		return discovery.Result{}, errors.New("no pairable host discovered")
	}

	if addressOverride != "" {
		address := discovery.NormalizeAddress(addressOverride, config.DefaultPort)

		filtered := make([]discovery.Result, 0, len(results))
		for _, result := range results {
			if strings.TrimSpace(result.Address) == address {
				filtered = append(filtered, result)
			}
		}

		if len(filtered) == 0 {
			return discovery.Result{}, fmt.Errorf("no discovered host matched address %s", address)
		}

		results = filtered
	}

	if len(results) == 1 {
		return results[0], nil
	}

	index := params.SelectionIndex
	if index <= 0 {
		index, err = promptForPairSelection(params, results)
		if err != nil {
			return discovery.Result{}, err
		}
	}

	if index < 1 || index > len(results) {
		return discovery.Result{}, fmt.Errorf("host selection %d out of range", index)
	}

	return results[index-1], nil
}

func filterPairableResults(results []discovery.Result) []discovery.Result {
	filtered := make([]discovery.Result, 0, len(results))
	for _, result := range results {
		if result.PairingEnabled {
			filtered = append(filtered, result)
		}
	}

	return filtered
}

//nolint:cyclop // Pair target resolution supports manual, configured, discovery, and interactive selection flows.
func resolvePairTarget(
	ctx context.Context,
	cfg config.Config,
	params *PairParams,
) (discovery.Result, error) {
	preparePairIO(params)

	fingerprint := strings.TrimSpace(params.Fingerprint)
	if fingerprint == "" {
		fingerprint = strings.TrimSpace(cfg.TLS.HostFingerprint)
	}

	if fingerprint == "" {
		return discovery.Result{}, errors.New(
			"host fingerprint is required for pairing; run `rmtx init`, use config tls.host_fingerprint, or pass --fingerprint",
		)
	}

	if out := strings.TrimSpace(params.AddressOverride); out != "" {
		return discovery.Result{
			Address:         discovery.NormalizeAddress(out, config.DefaultPort),
			Service:         cfg.Discovery.Service,
			Instance:        "manual-host",
			OS:              "",
			HostFingerprint: fingerprint,
			PairingEnabled:  true,
		}, nil
	}

	if cfgHost := strings.TrimSpace(cfg.Host); cfgHost != "" {
		return discovery.Result{
			Address:         discovery.NormalizeAddress(cfgHost, config.DefaultPort),
			Service:         cfg.Discovery.Service,
			Instance:        "configured-host",
			OS:              "",
			HostFingerprint: fingerprint,
			PairingEnabled:  true,
		}, nil
	}

	timeout := params.DiscoveryTimeout
	if timeout <= 0 {
		timeout = cfg.DiscoveryTimeout()
	}

	results, err := discoverAllHosts(ctx, cfg.Discovery.Service, timeout)
	if err != nil {
		return discovery.Result{}, err
	}

	if len(results) == 0 {
		return discovery.Result{}, errors.New("no host discovered")
	}

	filtered := make([]discovery.Result, 0, len(results))
	for _, result := range results {
		if strings.TrimSpace(result.HostFingerprint) == fingerprint {
			filtered = append(filtered, result)
		}
	}

	if len(filtered) == 0 {
		return discovery.Result{}, fmt.Errorf(
			"no discovered host matched fingerprint %s",
			fingerprint,
		)
	}

	results = filtered
	if len(results) == 1 {
		return results[0], nil
	}

	index := params.SelectionIndex
	if index <= 0 {
		var err error

		index, err = promptForPairSelection(params, results)
		if err != nil {
			return discovery.Result{}, err
		}
	}

	if index < 1 || index > len(results) {
		return discovery.Result{}, fmt.Errorf("host selection %d out of range", index)
	}

	return results[index-1], nil
}

func promptForTrust(params *PairParams, result discovery.Result) error {
	preparePairIO(params)

	_, _ = fmt.Fprintf(
		params.Stdout,
		"Trust host %s %s %s? [y/N]: ",
		empty(result.Instance, "rmtx-host"),
		result.Address,
		result.HostFingerprint,
	)

	line, err := readPairLine(params, "read trust confirmation")
	if err != nil {
		return err
	}

	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return nil
	default:
		return errors.New("pairing cancelled")
	}
}

func promptForPairSelection(params *PairParams, results []discovery.Result) (int, error) {
	preparePairIO(params)

	for i, result := range results {
		_, _ = fmt.Fprintf(
			params.Stdout,
			"[%d] %s %s %s %s\n",
			i+1,
			empty(result.Instance, "rmtx-host"),
			result.OS,
			result.Address,
			result.HostFingerprint,
		)
	}

	_, _ = fmt.Fprint(params.Stdout, "Select host: ")

	line, err := readPairLine(params, "read host selection")
	if err != nil {
		return 0, err
	}

	index := 0
	if _, err := fmt.Sscanf(line, "%d", &index); err != nil {
		return 0, errors.New("invalid host selection")
	}

	return index, nil
}

func resolveInitConfigPath(cwd, explicitPath string) (string, error) {
	if strings.TrimSpace(explicitPath) != "" {
		path, err := filepath.Abs(explicitPath)
		if err != nil {
			return "", fmt.Errorf("resolve config path: %w", err)
		}

		if _, err := os.Stat(path); err == nil {
			return "", fmt.Errorf("config already exists at %s; run `rmtx pair`", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("stat config path: %w", err)
		}

		return path, nil
	}

	loaded, err := config.Search(cwd)
	switch {
	case err == nil:
		return "", fmt.Errorf("config already exists at %s; run `rmtx pair`", loaded.Path)
	case errors.Is(err, config.ErrConfigNotFound):
	default:
		return "", err
	}

	root, err := filepath.Abs(cwd)
	if err != nil {
		return "", fmt.Errorf("resolve init root: %w", err)
	}

	return filepath.Join(root, ".rmtx.json"), nil
}

func writeInitConfig(path, cwd, fingerprint, hostAddress string) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("config path is required")
	}

	root, err := filepath.Abs(cwd)
	if err != nil {
		return fmt.Errorf("resolve init root: %w", err)
	}

	name := config.Loaded{Root: root}.ContextName()
	payload := struct {
		Version         int                   `json:"version"`
		Host            string                `json:"host,omitempty"`
		Context         *config.ContextConfig `json:"context,omitempty"`
		TLS             *config.TLSConfig     `json:"tls,omitempty"`
		Mounts          []config.Mount        `json:"mounts,omitempty"`
		Ignore          []string              `json:"ignore,omitempty"`
		IgnoreGitignore bool                  `json:"ignore_gitignore,omitempty"`
	}{
		Version:         config.Default().Version,
		Host:            strings.TrimSpace(hostAddress),
		Context:         &config.ContextConfig{Name: name},
		TLS:             &config.TLSConfig{HostFingerprint: strings.TrimSpace(fingerprint)},
		Mounts:          []config.Mount{{Path: "."}},
		Ignore:          []string{".git/**", "node_modules/**"},
		IgnoreGitignore: true,
	}

	content, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal init config: %w", err)
	}

	content = append(content, '\n')

	if err := os.MkdirAll(filepath.Dir(path), initConfigDirMode); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	if err := os.WriteFile(path, content, initConfigFileMode); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	return nil
}

func empty(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value != "" {
		return value
	}

	return fallback
}
