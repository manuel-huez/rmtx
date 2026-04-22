package host

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/manuel-huez/rmtx/internal/security"
)

type PairCodeInfo struct {
	Code              string
	ExpiresAt         time.Time
	HostName          string
	HostFingerprint   string
	HostFingerprintID string
}

func CreatePairCodeInfo(stateDir string, ttl time.Duration) (PairCodeInfo, error) {
	if strings.TrimSpace(stateDir) == "" {
		home, _ := os.UserHomeDir()
		if home == "" {
			home = "."
		}
		stateDir = filepath.Join(home, ".local", "state", "rmtx")
	}

	serverName := "rmtx-host"
	if hostName, err := os.Hostname(); err == nil && strings.TrimSpace(hostName) != "" {
		serverName = hostName
	}

	pki, err := security.EnsureHostPKI(stateDir, serverName)
	if err != nil {
		return PairCodeInfo{}, err
	}
	fingerprint, err := security.HostIdentityFingerprint(pki.CACertPEM)
	if err != nil {
		return PairCodeInfo{}, err
	}

	record, err := CreatePairCode(stateDir, ttl)
	if err != nil {
		return PairCodeInfo{}, fmt.Errorf("create pair code: %w", err)
	}

	return PairCodeInfo{
		Code:              record.Code,
		ExpiresAt:         record.ExpiresAt,
		HostName:          serverName,
		HostFingerprint:   fingerprint,
		HostFingerprintID: security.ShortFingerprint(fingerprint),
	}, nil
}
