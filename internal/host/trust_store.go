package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type trustedClient struct {
	Fingerprint string `json:"fingerprint"`
	Label       string `json:"label,omitempty"`
}

type trustStore struct {
	Clients []trustedClient `json:"clients"`
}

var trustStoreMu sync.Mutex

func (s *Server) trustStorePath() string {
	return filepath.Join(s.opts.StateDir, "trusted-clients.json")
}

func (s *Server) loadTrustStore() (trustStore, error) {
	trustStoreMu.Lock()
	defer trustStoreMu.Unlock()

	return s.loadTrustStoreUnlocked()
}

func (s *Server) loadTrustStoreUnlocked() (trustStore, error) {
	path := s.trustStorePath()
	content, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return trustStore{}, nil
		}
		return trustStore{}, fmt.Errorf("read trust store: %w", err)
	}

	var store trustStore
	if err := json.Unmarshal(content, &store); err != nil {
		return trustStore{}, fmt.Errorf("parse trust store: %w", err)
	}

	return store, nil
}

func (s *Server) saveTrustStore(store trustStore) error {
	if err := writeJSONAtomically(s.trustStorePath(), store); err != nil {
		return fmt.Errorf("write trust store: %w", err)
	}
	return nil
}

func (s *Server) trustClient(fingerprint, previousFingerprint, label string) error {
	trustStoreMu.Lock()
	defer trustStoreMu.Unlock()

	store, err := s.loadTrustStoreUnlocked()
	if err != nil {
		return err
	}

	fingerprint = strings.TrimSpace(fingerprint)
	previousFingerprint = strings.TrimSpace(previousFingerprint)
	label = strings.TrimSpace(label)

	filtered := make([]trustedClient, 0, len(store.Clients)+1)
	for _, client := range store.Clients {
		switch client.Fingerprint {
		case "", fingerprint, previousFingerprint:
			continue
		}
		filtered = append(filtered, client)
	}

	store.Clients = append(filtered, trustedClient{Fingerprint: fingerprint, Label: label})
	return s.saveTrustStore(store)
}

func (s *Server) clientTrusted(fingerprint string) (bool, error) {
	store, err := s.loadTrustStore()
	if err != nil {
		return false, err
	}

	for _, client := range store.Clients {
		if client.Fingerprint == fingerprint {
			return true, nil
		}
	}

	return false, nil
}
