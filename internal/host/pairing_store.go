package host

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const defaultPairCodeTTL = 5 * time.Minute
const pairCodeFileMode = 0o600
const pairCodeDirMode = 0o755
const pairCodeLockWait = 5 * time.Second
const pairCodeLockPoll = 10 * time.Millisecond
const pairCodeLockStaleAfter = 30 * time.Second
const pairCodeSpace = 1_000_000

var pairCodeStoreMu sync.Mutex

type pairCodeRecord struct {
	Code      string    `json:"code"`
	ExpiresAt time.Time `json:"expires_at"`
	Used      bool      `json:"used,omitempty"`
}

type pairCodeStore struct {
	Codes []pairCodeRecord `json:"codes"`
}

func pairCodePath(stateDir string) string {
	return filepath.Join(stateDir, "pair-codes.json")
}

func pairCodeLockPath(stateDir string) string {
	return filepath.Join(stateDir, "pair-codes.lock")
}

func acquirePairCodeStoreLock(stateDir string) (func(), error) {
	if err := os.MkdirAll(stateDir, pairCodeDirMode); err != nil {
		return nil, fmt.Errorf("create pairing state dir: %w", err)
	}

	lockPath := pairCodeLockPath(stateDir)
	deadline := time.Now().Add(pairCodeLockWait)

	for {
		lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, pairCodeFileMode)
		if err == nil {
			_, _ = fmt.Fprintf(lockFile, "%d\n", os.Getpid())
			_ = lockFile.Close()

			return func() { _ = os.Remove(lockPath) }, nil
		}

		if !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("lock pairing codes: %w", err)
		}

		info, statErr := os.Stat(lockPath)
		switch {
		case errors.Is(statErr, os.ErrNotExist):
			continue
		case statErr != nil:
			return nil, fmt.Errorf("stat pairing code lock: %w", statErr)
		case time.Since(info.ModTime()) > pairCodeLockStaleAfter:
			_ = os.Remove(lockPath)
			continue
		}

		if time.Now().After(deadline) {
			return nil, errors.New("timed out waiting for pairing code store lock")
		}

		time.Sleep(pairCodeLockPoll)
	}
}

func CreatePairCode(stateDir string, ttl time.Duration) (pairCodeRecord, error) {
	pairCodeStoreMu.Lock()
	defer pairCodeStoreMu.Unlock()

	release, err := acquirePairCodeStoreLock(stateDir)
	if err != nil {
		return pairCodeRecord{}, err
	}
	defer release()

	if ttl <= 0 {
		ttl = defaultPairCodeTTL
	}

	store, err := loadPairCodeStore(stateDir)
	if err != nil {
		return pairCodeRecord{}, err
	}

	now := time.Now()

	filtered := make([]pairCodeRecord, 0, len(store.Codes)+1)
	for _, code := range store.Codes {
		if code.Used || !code.ExpiresAt.After(now) {
			continue
		}

		filtered = append(filtered, code)
	}

	store.Codes = filtered

	code, err := randomPairCode()
	if err != nil {
		return pairCodeRecord{}, err
	}

	record := pairCodeRecord{
		Code:      code,
		ExpiresAt: now.Add(ttl).UTC(),
	}
	store.Codes = append(store.Codes, record)

	if err := savePairCodeStore(stateDir, store); err != nil {
		return pairCodeRecord{}, err
	}

	return record, nil
}

func ConsumePairCode(stateDir, code string) error {
	return withPairCode(stateDir, code, func() error { return nil })
}

func withPairCode(stateDir, code string, use func() error) error {
	pairCodeStoreMu.Lock()
	defer pairCodeStoreMu.Unlock()

	release, err := acquirePairCodeStoreLock(stateDir)
	if err != nil {
		return err
	}
	defer release()

	store, err := loadPairCodeStore(stateDir)
	if err != nil {
		return err
	}

	index, err := validPairCodeIndex(store, code, time.Now())
	if err != nil {
		return err
	}

	if err := use(); err != nil {
		return err
	}

	store.Codes[index].Used = true

	return savePairCodeStore(stateDir, store)
}

func validPairCodeIndex(store pairCodeStore, code string, now time.Time) (int, error) {
	code = strings.TrimSpace(code)
	for i := range store.Codes {
		if store.Codes[i].Code != code {
			continue
		}

		if store.Codes[i].Used {
			return -1, errors.New("pairing code already used")
		}

		if !store.Codes[i].ExpiresAt.After(now) {
			return -1, errors.New("pairing code expired")
		}

		return i, nil
	}

	return -1, errors.New("pairing code invalid")
}

func loadPairCodeStore(stateDir string) (pairCodeStore, error) {
	content, err := os.ReadFile(pairCodePath(stateDir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return pairCodeStore{}, nil
		}

		return pairCodeStore{}, fmt.Errorf("read pairing codes: %w", err)
	}

	var store pairCodeStore
	if err := json.Unmarshal(content, &store); err != nil {
		return pairCodeStore{}, fmt.Errorf("parse pairing codes: %w", err)
	}

	return store, nil
}

func savePairCodeStore(stateDir string, store pairCodeStore) error {
	content, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal pairing codes: %w", err)
	}

	return os.WriteFile(pairCodePath(stateDir), append(content, '\n'), pairCodeFileMode)
}

func randomPairCode() (string, error) {
	max := big.NewInt(pairCodeSpace)

	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		return "", fmt.Errorf("generate pairing code: %w", err)
	}

	return fmt.Sprintf("%06d", n.Int64()), nil
}
