package config

import (
	"crypto/sha256"
	"encoding/hex"
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
	defaultContextName      = "context"
	contextHashLen          = 12
)

var ErrConfigNotFound = errors.New("config not found")

var fileNames = []string{
	".rmtx.json",
	"rmtx.json",
}

type ContextConfig struct {
	Name string `json:"name,omitempty"`
}

type Config struct {
	Version   int             `json:"version,omitempty"`
	Context   ContextConfig   `json:"context,omitempty"`
	Host      string          `json:"host,omitempty"`
	TLS       TLSConfig       `json:"tls,omitempty"`
	WorkDir   string          `json:"workdir,omitempty"`
	Discovery DiscoveryConfig `json:"discovery,omitempty"`
	Mounts    []Mount         `json:"mounts,omitempty"`
	Env       EnvConfig       `json:"env,omitempty"`
}

type TLSConfig struct {
	HostFingerprint string `json:"host_fingerprint,omitempty"`
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

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(content, &raw); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	for _, field := range []string{"token", "token_env"} {
		if _, ok := raw[field]; ok {
			return nil, fmt.Errorf("config field %q is unsupported; use `rmtx pair` and tls.host_fingerprint", field)
		}
	}

	cfg := Default()
	if err := json.Unmarshal(content, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg = normalize(cfg)

	return &Loaded{Path: absPath, Root: filepath.Dir(absPath), Config: cfg}, nil
}

func Resolve(startDir, explicitConfig string) (*Loaded, error) {
	loaded, err := loadExplicitOrSearch(startDir, explicitConfig)
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

func ResolveRequired(startDir, explicitConfig string) (*Loaded, error) {
	loaded, err := loadExplicitOrSearch(startDir, explicitConfig)
	if err != nil {
		if errors.Is(err, ErrConfigNotFound) {
			return nil, fmt.Errorf(
				"local config file is required; create one of %s",
				strings.Join(fileNames[:2], ", "),
			)
		}

		return nil, err
	}

	return loaded, nil
}

func loadExplicitOrSearch(startDir, explicitConfig string) (*Loaded, error) {
	if strings.TrimSpace(explicitConfig) != "" {
		return Load(explicitConfig)
	}

	return Search(startDir)
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

func (l Loaded) ContextName() string {
	if strings.TrimSpace(l.Config.Context.Name) != "" {
		return strings.TrimSpace(l.Config.Context.Name)
	}

	name := filepath.Base(l.Root)
	if name == "" || name == "." || name == string(filepath.Separator) {
		return defaultContextName
	}

	return name
}

func (l Loaded) ContextIdentity() string {
	if strings.TrimSpace(l.Config.Context.Name) != "" {
		return "name:" + strings.TrimSpace(l.Config.Context.Name)
	}

	root := l.Root
	if root == "" && l.Path != "" {
		root = filepath.Dir(l.Path)
	}

	return "root:" + filepath.Clean(root)
}

func (l Loaded) ContextID() string {
	name := slug(l.ContextName())
	if name == "" {
		name = defaultContextName
	}

	sum := sha256.Sum256([]byte(l.ContextIdentity()))

	return fmt.Sprintf("%s-%s", name, hex.EncodeToString(sum[:])[:contextHashLen])
}

func normalize(cfg Config) Config {
	if cfg.Version == 0 {
		cfg.Version = 1
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

func slug(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}

	var b strings.Builder

	lastDash := false

	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)

			lastDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)

			lastDash = false
		default:
			if !lastDash && b.Len() > 0 {
				b.WriteByte('-')

				lastDash = true
			}
		}
	}

	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "context"
	}

	return out
}
