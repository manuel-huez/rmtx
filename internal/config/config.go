package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	DefaultPort             = 33221
	DefaultService          = "rmtx"
	defaultDiscoveryTimeout = 750 * time.Millisecond
)

var ErrConfigNotFound = errors.New("config not found")

var fileNames = []string{
	".rmtx.json",
	"rmtx.json",
}

type Config struct {
	Version   int             `json:"version,omitempty"`
	Host      string          `json:"host,omitempty"`
	Token     string          `json:"token,omitempty"`
	TokenEnv  string          `json:"token_env,omitempty"`
	WorkDir   string          `json:"workdir,omitempty"`
	Discovery DiscoveryConfig `json:"discovery,omitempty"`
	Mounts    []Mount         `json:"mounts,omitempty"`
	Env       EnvConfig       `json:"env,omitempty"`
}

type Mount struct {
	Path    string   `json:"path"`
	Exclude []string `json:"exclude,omitempty"`
}

type EnvConfig struct {
	Forward []string `json:"forward,omitempty"`
}

type DiscoveryConfig struct {
	Enabled *bool  `json:"enabled,omitempty"`
	Service string `json:"service,omitempty"`
	Timeout string `json:"timeout,omitempty"`
}

type Loaded struct {
	Path   string
	Root   string
	Config Config
}

func Default() Config {
	return Config{
		Version: 1,
		Mounts:  []Mount{{Path: "."}},
		Discovery: DiscoveryConfig{
			Timeout: "750ms",
			Service: DefaultService,
		},
	}
}

func Search(startDir string) (*Loaded, error) {
	if startDir == "" {
		return nil, errors.New("start directory is required")
	}

	startDir, err := filepath.Abs(startDir)
	if err != nil {
		return nil, fmt.Errorf("resolve start directory: %w", err)
	}

	current := startDir

	for {
		for _, name := range fileNames {
			candidate := filepath.Join(current, name)
			if _, err := os.Stat(candidate); err == nil {
				return Load(candidate)
			} else if !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("stat %s: %w", candidate, err)
			}
		}

		parent := filepath.Dir(current)
		if parent == current {
			return nil, ErrConfigNotFound
		}

		current = parent
	}
}

func Load(path string) (*Loaded, error) {
	if path == "" {
		return nil, errors.New("config path is required")
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve config path: %w", err)
	}

	content, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := Default()
	if err := json.Unmarshal(content, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg = normalize(cfg)

	return &Loaded{Path: absPath, Root: filepath.Dir(absPath), Config: cfg}, nil
}

func Resolve(startDir, explicitConfig string) (*Loaded, error) {
	if strings.TrimSpace(explicitConfig) != "" {
		return Load(explicitConfig)
	}

	loaded, err := Search(startDir)
	if err != nil {
		if errors.Is(err, ErrConfigNotFound) {
			goto defaultConfig
		}

		return nil, err
	}

	if loaded != nil {
		return loaded, nil
	}

defaultConfig:
	root, err := filepath.Abs(startDir)

	if err != nil {
		return nil, fmt.Errorf("resolve default root: %w", err)
	}

	cfg := normalize(Default())

	return &Loaded{Path: "", Root: root, Config: cfg}, nil
}

func WithDefaults(cfg Config) Config {
	return normalize(cfg)
}

func (c Config) DiscoveryEnabled() bool {
	if c.Discovery.Enabled == nil {
		return true
	}

	return *c.Discovery.Enabled
}

func (c Config) DiscoveryTimeout() time.Duration {
	d, err := time.ParseDuration(c.Discovery.Timeout)
	if err != nil {
		return defaultDiscoveryTimeout
	}

	return d
}

func (c Config) TokenValue() string {
	if strings.TrimSpace(c.Token) != "" {
		return c.Token
	}

	envName := c.TokenEnv
	if strings.TrimSpace(envName) == "" {
		envName = "RMTX_TOKEN"
	}

	return os.Getenv(envName)
}

func normalize(cfg Config) Config {
	if cfg.Version == 0 {
		cfg.Version = 1
	}

	if strings.TrimSpace(cfg.TokenEnv) == "" {
		cfg.TokenEnv = "RMTX_TOKEN"
	}

	if strings.TrimSpace(cfg.WorkDir) == "" {
		cfg.WorkDir = "."
	}

	if len(cfg.Mounts) == 0 {
		cfg.Mounts = []Mount{{Path: "."}}
	}

	if strings.TrimSpace(cfg.Discovery.Service) == "" {
		cfg.Discovery.Service = DefaultService
	}

	if strings.TrimSpace(cfg.Discovery.Timeout) == "" {
		cfg.Discovery.Timeout = "750ms"
	}

	return cfg
}
