package clientstate

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

const (
	dirMode  = 0o700
	fileMode = 0o600
)

type HostRecord struct {
	Address        string `json:"address"`
	Name           string `json:"name,omitempty"`
	OS             string `json:"os,omitempty"`
	Fingerprint    string `json:"fingerprint"`
	Paired         bool   `json:"paired,omitempty"`
	LastPairedCert string `json:"last_paired_cert,omitempty"`
	ClientCertPEM  string `json:"client_cert_pem,omitempty"`
	ClientKeyPEM   string `json:"client_key_pem,omitempty"`
}

type State struct {
	ClientLabel   string       `json:"client_label,omitempty"`
	ClientCertPEM string       `json:"client_cert_pem,omitempty"`
	ClientKeyPEM  string       `json:"client_key_pem,omitempty"`
	Hosts         []HostRecord `json:"hosts,omitempty"`
}

type Loaded struct {
	Dir  string
	Path string
	Data State
}

func DefaultDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		if current, userErr := user.Current(); userErr == nil &&
			strings.TrimSpace(current.HomeDir) != "" {
			home = current.HomeDir
		}
	}

	if strings.TrimSpace(home) == "" {
		return "", errors.New("resolve home directory")
	}

	return filepath.Join(home, ".rmtx"), nil
}

func Load() (*Loaded, error) {
	dir, err := DefaultDir()
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, fmt.Errorf("create client state dir: %w", err)
	}

	path := filepath.Join(dir, "state.json")
	loaded := &Loaded{Dir: dir, Path: path}

	content, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return loaded, nil
		}

		return nil, fmt.Errorf("read client state: %w", err)
	}

	if err := json.Unmarshal(content, &loaded.Data); err != nil {
		return nil, fmt.Errorf("parse client state: %w", err)
	}

	return loaded, nil
}

func (l *Loaded) Save() error {
	if l == nil {
		return errors.New("client state is required")
	}

	if err := os.MkdirAll(l.Dir, dirMode); err != nil {
		return fmt.Errorf("create client state dir: %w", err)
	}

	content, err := json.MarshalIndent(l.Data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal client state: %w", err)
	}

	return os.WriteFile(l.Path, append(content, '\n'), fileMode)
}

func (l *Loaded) FindHostByAddress(address string) *HostRecord {
	if l == nil {
		return nil
	}

	address = strings.TrimSpace(address)
	for i := range l.Data.Hosts {
		if l.Data.Hosts[i].Address == address {
			return &l.Data.Hosts[i]
		}
	}

	return nil
}

func (l *Loaded) FindHostByFingerprint(fingerprint string) *HostRecord {
	if l == nil {
		return nil
	}

	fingerprint = strings.TrimSpace(fingerprint)
	if fingerprint == "" {
		return nil
	}

	for i := range l.Data.Hosts {
		if l.Data.Hosts[i].Fingerprint == fingerprint {
			return &l.Data.Hosts[i]
		}
	}

	return nil
}

func (l *Loaded) UpsertHost(record HostRecord) {
	if l == nil {
		return
	}

	record.Address = strings.TrimSpace(record.Address)

	record.Fingerprint = strings.TrimSpace(record.Fingerprint)
	if record.Fingerprint != "" {
		for i := range l.Data.Hosts {
			if l.Data.Hosts[i].Fingerprint == record.Fingerprint {
				l.Data.Hosts[i] = record
				return
			}
		}
	}

	for i := range l.Data.Hosts {
		if l.Data.Hosts[i].Address == record.Address &&
			(strings.TrimSpace(l.Data.Hosts[i].Fingerprint) == "" || record.Fingerprint == "") {
			l.Data.Hosts[i] = record
			return
		}
	}

	l.Data.Hosts = append(l.Data.Hosts, record)
}

func (l *Loaded) UpdateHostAddress(fingerprint, address string) bool {
	if l == nil {
		return false
	}

	fingerprint = strings.TrimSpace(fingerprint)

	address = strings.TrimSpace(address)
	if fingerprint == "" || address == "" {
		return false
	}

	index := l.hostIndexByFingerprint(fingerprint)
	if index < 0 || l.Data.Hosts[index].Address == address {
		return false
	}

	nextIndex, ok := l.removeUnpairedAddressAliases(index, address)
	if !ok {
		return false
	}

	l.Data.Hosts[nextIndex].Address = address

	return true
}

func (l *Loaded) hostIndexByFingerprint(fingerprint string) int {
	for i := range l.Data.Hosts {
		if l.Data.Hosts[i].Fingerprint == fingerprint {
			return i
		}
	}

	return -1
}

func (l *Loaded) removeUnpairedAddressAliases(index int, address string) (int, bool) {
	for i := 0; i < len(l.Data.Hosts); i++ {
		if i == index || l.Data.Hosts[i].Address != address {
			continue
		}

		if strings.TrimSpace(l.Data.Hosts[i].Fingerprint) != "" {
			return index, false
		}

		l.Data.Hosts = append(l.Data.Hosts[:i], l.Data.Hosts[i+1:]...)
		if i < index {
			index--
		}

		i--
	}

	return index, true
}

func (l *Loaded) HostCredentials(address, fingerprint string) (string, string) {
	if record := l.FindHostByFingerprint(fingerprint); record != nil {
		if cert, key, ok := recordCredentials(record); ok {
			return cert, key
		}

		return l.defaultCredentials()
	}

	record := l.FindHostByAddress(address)
	if record != nil {
		if cert, key, ok := recordCredentials(record); ok {
			return cert, key
		}
	}

	return l.defaultCredentials()
}

func recordCredentials(record *HostRecord) (string, string, bool) {
	if record == nil {
		return "", "", false
	}

	if strings.TrimSpace(record.ClientCertPEM) == "" ||
		strings.TrimSpace(record.ClientKeyPEM) == "" {
		return "", "", false
	}

	return record.ClientCertPEM, record.ClientKeyPEM, true
}

func (l *Loaded) defaultCredentials() (string, string) {
	if strings.TrimSpace(l.Data.ClientCertPEM) != "" &&
		strings.TrimSpace(l.Data.ClientKeyPEM) != "" {
		return l.Data.ClientCertPEM, l.Data.ClientKeyPEM
	}

	return "", ""
}

func DefaultClientLabel() string {
	if host, err := os.Hostname(); err == nil && strings.TrimSpace(host) != "" {
		return host
	}

	return "rmtx-client"
}
