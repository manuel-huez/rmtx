//nolint:wsl_v5
package oci

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUnpackImageRejectsPathTraversal(t *testing.T) {
	layer := tarGzip(t, tarEntry{Name: "../../escape.txt", Body: "bad"})
	digest := storeTestBlob(t, layer)

	store := NewStore(filepath.Join(t.TempDir(), "oci"))
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBlob(digest, bytes.NewReader(layer)); err != nil {
		t.Fatal(err)
	}

	err := store.UnpackImage(t.TempDir(), Image{
		ManifestDigest: "sha256:" + strings.Repeat("a", 64),
		Layers:         []Descriptor{{Digest: digest}},
	})
	if err == nil {
		t.Fatal("expected path traversal error")
	}
}

func TestUnpackImageAppliesWhiteout(t *testing.T) {
	first := tarGzip(t, tarEntry{Name: "gone.txt", Body: "gone"})
	second := tarGzip(t, tarEntry{Name: ".wh.gone.txt"})
	firstDigest := storeTestBlob(t, first)
	secondDigest := storeTestBlob(t, second)

	store := NewStore(filepath.Join(t.TempDir(), "oci"))
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBlob(firstDigest, bytes.NewReader(first)); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBlob(secondDigest, bytes.NewReader(second)); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	if err := store.UnpackImage(root, Image{
		ManifestDigest: "sha256:" + strings.Repeat("b", 64),
		Layers: []Descriptor{
			{Digest: firstDigest},
			{Digest: secondDigest},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(root, "gone.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected whiteout to remove file, err=%v", err)
	}
}

func TestApplyTarEntryUnpacksLegacyRegularFile(t *testing.T) {
	root := t.TempDir()
	if err := applyTarEntry(context.Background(), root, &tar.Header{
		Name:     "legacy.txt",
		Typeflag: legacyTarRegularFile,
		Mode:     0o644,
		Size:     int64(len("legacy")),
	}, strings.NewReader("legacy")); err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(filepath.Join(root, "legacy.txt"))
	if err != nil {
		t.Fatal(err)
	}

	if string(content) != "legacy" {
		t.Fatalf("legacy regular file content=%q", string(content))
	}
}

func TestApplyTarEntryPreservesExecutableMode(t *testing.T) {
	root := t.TempDir()
	if err := applyTarEntry(context.Background(), root, &tar.Header{
		Name:     "bin/tool",
		Typeflag: tar.TypeReg,
		Mode:     0o755,
		Size:     int64(len("#!/bin/sh\n")),
	}, strings.NewReader("#!/bin/sh\n")); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(filepath.Join(root, "bin", "tool"))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o755 {
		t.Fatalf("mode=%#o want 0755", got)
	}
}

type tarEntry struct {
	Name string
	Body string
}

func tarGzip(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, entry := range entries {
		body := []byte(entry.Body)
		if err := tw.WriteHeader(&tar.Header{
			Name: entry.Name,
			Mode: 0o644,
			Size: int64(len(body)),
		}); err != nil {
			t.Fatal(err)
		}
		if len(body) > 0 {
			if _, err := tw.Write(body); err != nil {
				t.Fatal(err)
			}
		}
	}

	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}

	return buf.Bytes()
}

func storeTestBlob(t *testing.T, content []byte) string {
	t.Helper()
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}
