//nolint:wsl_v5
package oci

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const dirMode = 0o755
const digestShardLen = 2
const storeFileMode = 0o644

type Store struct {
	root string
}

type Image struct {
	Reference      string       `json:"reference"`
	ManifestDigest string       `json:"manifest_digest"`
	ConfigDigest   string       `json:"config_digest,omitempty"`
	Env            []string     `json:"env,omitempty"`
	Layers         []Descriptor `json:"layers"`
}

type Descriptor struct {
	MediaType string    `json:"mediaType,omitempty"`
	Digest    string    `json:"digest"`
	Size      int64     `json:"size,omitempty"`
	Platform  *Platform `json:"platform,omitempty"`
}

type Platform struct {
	Architecture string `json:"architecture,omitempty"`
	OS           string `json:"os,omitempty"`
	Variant      string `json:"variant,omitempty"`
}

func NewStore(root string) *Store {
	return &Store{root: root}
}

func (s *Store) Ensure() error {
	for _, dir := range []string{
		s.BlobsDir(),
		filepath.Join(s.root, "manifests"),
		filepath.Join(s.root, "refs"),
	} {
		if err := os.MkdirAll(dir, dirMode); err != nil {
			return fmt.Errorf("create OCI cache dir: %w", err)
		}
	}

	return nil
}

func (s *Store) Root() string {
	return s.root
}

func (s *Store) BlobsDir() string {
	return filepath.Join(s.root, "blobs")
}

func (s *Store) BlobPath(digest string) string {
	algo, encoded := digestParts(digest)
	if len(encoded) >= digestShardLen {
		return filepath.Join(s.BlobsDir(), algo, encoded[:digestShardLen], encoded)
	}

	return filepath.Join(s.BlobsDir(), algo, encoded)
}

func (s *Store) ManifestPath(digest string) string {
	algo, encoded := digestParts(digest)
	return filepath.Join(s.root, "manifests", algo, encoded+".json")
}

func (s *Store) HasBlob(digest string) bool {
	path, err := s.blobPath(digest)
	if err != nil {
		return false
	}

	_, err = os.Stat(path)
	return err == nil
}

func (s *Store) ReadBlob(digest string) (*os.File, error) {
	path, err := s.blobPath(digest)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open OCI blob %s: %w", digest, err)
	}

	return f, nil
}

func (s *Store) StoreBlob(digest string, src io.Reader) error {
	path, err := s.blobPath(digest)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		return fmt.Errorf("create blob dir: %w", err)
	}

	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create blob temp: %w", err)
	}
	tmp := f.Name()

	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, h), src); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write blob: %w", err)
	}

	if err := f.Chmod(storeFileMode); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("chmod blob temp: %w", err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close blob: %w", err)
	}

	got := "sha256:" + hex.EncodeToString(h.Sum(nil))
	if got != digest {
		_ = os.Remove(tmp)
		return fmt.Errorf("blob digest mismatch: got %s want %s", got, digest)
	}

	if err := commitImmutableTemp(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("commit blob: %w", err)
	}

	return nil
}

func (s *Store) StoreManifest(digest string, raw []byte) error {
	path, err := s.manifestPath(digest)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		return fmt.Errorf("create manifest dir: %w", err)
	}

	tmp, err := writeImmutableTemp(path, raw)
	if err != nil {
		return fmt.Errorf("write manifest temp: %w", err)
	}

	if err := commitImmutableTemp(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("commit manifest: %w", err)
	}

	return nil
}

func (s *Store) StoreRef(ref Reference, image Image) error {
	path := filepath.Join(s.root, "refs", safeRefName(ref.Normalized())+".json")
	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		return err
	}

	content, err := json.MarshalIndent(image, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, append(content, '\n'), storeFileMode)
}

func (s *Store) LoadRef(ref Reference) (Image, error) {
	path := filepath.Join(s.root, "refs", safeRefName(ref.Normalized())+".json")
	content, err := os.ReadFile(path)
	if err != nil {
		return Image{}, err
	}

	var image Image
	if err := json.Unmarshal(content, &image); err != nil {
		return Image{}, err
	}

	if len(image.Env) == 0 && image.ConfigDigest != "" {
		image.Env, _ = s.ImageConfigEnv(image.ConfigDigest)
	}

	return image, nil
}

func (s *Store) ImageConfigEnv(digest string) ([]string, error) {
	if digest == "" {
		return nil, nil
	}

	f, err := s.ReadBlob(digest)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	var cfg imageConfig
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse OCI image config %s: %w", digest, err)
	}

	return append([]string(nil), cfg.Config.Env...), nil
}

func (s *Store) ImageComplete(image Image) bool {
	manifestPath, err := s.manifestPath(image.ManifestDigest)
	if err != nil || !fileExists(manifestPath) {
		return false
	}

	if image.ConfigDigest != "" && !s.HasBlob(image.ConfigDigest) {
		return false
	}

	for _, layer := range image.Layers {
		if !s.HasBlob(layer.Digest) {
			return false
		}
	}

	return true
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func writeImmutableTemp(path string, content []byte) (string, error) {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return "", err
	}

	tmp := f.Name()
	if _, err := f.Write(content); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", err
	}

	if err := f.Chmod(storeFileMode); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", err
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}

	return tmp, nil
}

func commitImmutableTemp(tmp, path string) error {
	if err := os.Rename(tmp, path); err != nil {
		if fileExists(path) {
			_ = os.Remove(tmp)

			return nil
		}

		return err
	}

	return nil
}

func DigestBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (s *Store) blobPath(digest string) (string, error) {
	if err := validateDigest(digest, "blob"); err != nil {
		return "", err
	}

	return s.BlobPath(digest), nil
}

func (s *Store) manifestPath(digest string) (string, error) {
	if err := validateDigest(digest, "manifest"); err != nil {
		return "", err
	}

	return s.ManifestPath(digest), nil
}

func validateDigest(digest string, field string) error {
	if !digestPattern.MatchString(digest) {
		return fmt.Errorf("invalid %s digest %q", field, digest)
	}

	return nil
}

func digestParts(digest string) (string, string) {
	if !digestPattern.MatchString(digest) {
		return "invalid", safeRefName(digest)
	}

	algo, encoded, ok := strings.Cut(digest, ":")
	if !ok {
		return "unknown", digest
	}

	return algo, encoded
}

func safeRefName(ref string) string {
	sum := sha256.Sum256([]byte(ref))

	return hex.EncodeToString(sum[:])
}
