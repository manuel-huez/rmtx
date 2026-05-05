//nolint:wsl_v5
package oci

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const testManifestPath = "/v2/repo/manifests/latest"

//nolint:cyclop // Test server branches model registry endpoints.
func TestPullFetchesManifestAndBlobsWithBearerChallenge(t *testing.T) {
	layer := testLayer(t, map[string]string{"hello.txt": "hello\n"})
	layerDigest := digest(layer)
	config := []byte(
		`{"architecture":"amd64","os":"linux","config":{"Env":["PATH=/usr/local/go/bin:/usr/bin","GOTOOLCHAIN=local"]}}`,
	)
	configDigest := digest(config)
	manifest := fmt.Appendf(nil, `{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config": {"mediaType":"application/vnd.oci.image.config.v1+json","digest":%q,"size":%d},
  "layers": [{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":%q,"size":%d}]
}`, configDigest, len(config), layerDigest, len(layer))
	manifestDigest := digest(manifest)

	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_, _ = w.Write([]byte(`{"token":"test-token"}`))
			return
		}

		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.Header().Set(
				"WWW-Authenticate",
				`Bearer realm="`+server.URL+`/token",service="test",scope="repository:repo:pull"`,
			)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		switch r.URL.Path {
		case testManifestPath:
			w.Header().Set("Content-Type", mediaOCIManifest)
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			_, _ = w.Write(manifest)
		case "/v2/repo/blobs/" + configDigest:
			_, _ = w.Write(config)
		case "/v2/repo/blobs/" + layerDigest:
			_, _ = w.Write(layer)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	ref, err := ParseReference(strings.TrimPrefix(server.URL, "https://") + "/repo:latest")
	if err != nil {
		t.Fatal(err)
	}

	store := NewStore(filepath.Join(t.TempDir(), "oci"))
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	client := NewClient(server.Client())
	image, err := client.Pull(context.Background(), ref, store, PullOptions{
		PlatformOS:   "linux",
		Architecture: "amd64",
	})
	if err != nil {
		t.Fatal(err)
	}

	if image.ManifestDigest != manifestDigest {
		t.Fatalf("manifest digest=%s want %s", image.ManifestDigest, manifestDigest)
	}

	if len(image.Env) != 2 || image.Env[0] != "PATH=/usr/local/go/bin:/usr/bin" {
		t.Fatalf("image env=%#v", image.Env)
	}

	cached, err := store.LoadRef(ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(cached.Env) != 2 || cached.Env[1] != "GOTOOLCHAIN=local" {
		t.Fatalf("cached image env=%#v", cached.Env)
	}

	if !store.HasBlob(layerDigest) || !store.HasBlob(configDigest) {
		t.Fatalf("expected blobs in store")
	}
}

func TestPullRejectsManifestDigestMismatch(t *testing.T) {
	manifest := []byte(`{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config": {"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":2},
  "layers": []
}`)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testManifestPath {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", mediaOCIManifest)
		w.Header().Set(
			"Docker-Content-Digest",
			"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		)
		_, _ = w.Write(manifest)
	}))
	defer server.Close()

	ref, err := ParseReference(strings.TrimPrefix(server.URL, "https://") + "/repo:latest")
	if err != nil {
		t.Fatal(err)
	}

	store := NewStore(filepath.Join(t.TempDir(), "oci"))
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	client := NewClient(server.Client())
	_, err = client.Pull(context.Background(), ref, store, PullOptions{
		PlatformOS:   "linux",
		Architecture: "amd64",
	})
	if err == nil || !strings.Contains(err.Error(), "manifest digest mismatch") {
		t.Fatalf("err=%v want manifest digest mismatch", err)
	}
}

func TestPullRejectsDigestTargetMismatch(t *testing.T) {
	configDigest := "sha256:" + strings.Repeat("a", 64)
	layerDigest := "sha256:" + strings.Repeat("b", 64)
	manifest := fmt.Appendf(nil, `{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config": {"mediaType":"application/vnd.oci.image.config.v1+json","digest":%q,"size":2},
  "layers": [{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":%q,"size":2}]
}`, configDigest, layerDigest)
	actualDigest := digest(manifest)
	requestedDigest := "sha256:" + strings.Repeat("c", 64)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/repo/manifests/"+requestedDigest {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", mediaOCIManifest)
		w.Header().Set("Docker-Content-Digest", actualDigest)
		_, _ = w.Write(manifest)
	}))
	defer server.Close()

	ref, err := ParseReference(
		strings.TrimPrefix(server.URL, "https://") + "/repo@" + requestedDigest,
	)
	if err != nil {
		t.Fatal(err)
	}

	store := NewStore(filepath.Join(t.TempDir(), "oci"))
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	client := NewClient(server.Client())
	_, err = client.Pull(context.Background(), ref, store, PullOptions{
		PlatformOS:   "linux",
		Architecture: "amd64",
	})
	if err == nil || !strings.Contains(err.Error(), "manifest digest mismatch") {
		t.Fatalf("err=%v want manifest digest mismatch", err)
	}
}

func TestPullRejectsUnsafeDescriptorDigest(t *testing.T) {
	layerDigest := "sha256:" + strings.Repeat("b", 64)
	manifest := fmt.Appendf(nil, `{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config": {"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:../../escape","size":2},
  "layers": [{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":%q,"size":2}]
}`, layerDigest)
	manifestDigest := digest(manifest)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != testManifestPath {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", mediaOCIManifest)
		w.Header().Set("Docker-Content-Digest", manifestDigest)
		_, _ = w.Write(manifest)
	}))
	defer server.Close()

	ref, err := ParseReference(strings.TrimPrefix(server.URL, "https://") + "/repo:latest")
	if err != nil {
		t.Fatal(err)
	}

	store := NewStore(filepath.Join(t.TempDir(), "oci"))
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	client := NewClient(server.Client())
	_, err = client.Pull(context.Background(), ref, store, PullOptions{
		PlatformOS:   "linux",
		Architecture: "amd64",
	})
	if err == nil || !strings.Contains(err.Error(), "invalid config digest") {
		t.Fatalf("err=%v want invalid config digest", err)
	}
}

func TestStoreBlobRejectsInvalidDigestBeforePath(t *testing.T) {
	root := t.TempDir()
	store := NewStore(filepath.Join(root, "oci"))
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	err := store.StoreBlob("sha256:../../escape", bytes.NewReader([]byte("x")))
	if err == nil || !strings.Contains(err.Error(), "invalid blob digest") {
		t.Fatalf("err=%v want invalid blob digest", err)
	}

	if _, err := os.Stat(filepath.Join(root, "escape")); !os.IsNotExist(err) {
		t.Fatalf("invalid digest should not write outside store, err=%v", err)
	}
}

func TestStoreBlobConcurrentSameDigest(t *testing.T) {
	content := []byte("shared blob")
	blobDigest := digest(content)

	store := NewStore(filepath.Join(t.TempDir(), "oci"))
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errs := make(chan error, 8)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-start
			errs <- store.StoreBlob(blobDigest, bytes.NewReader(content))
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	if !store.HasBlob(blobDigest) {
		t.Fatal("expected blob in store")
	}
}

func TestStoreRefUsesCollisionFreeCacheKeys(t *testing.T) {
	store := NewStore(filepath.Join(t.TempDir(), "oci"))
	if err := store.Ensure(); err != nil {
		t.Fatal(err)
	}

	firstRef, err := ParseReference("example.com/foo_bar/baz:1")
	if err != nil {
		t.Fatal(err)
	}

	secondRef, err := ParseReference("example.com/foo/bar_baz:1")
	if err != nil {
		t.Fatal(err)
	}

	first := Image{Reference: firstRef.Normalized(), ManifestDigest: "sha256:first"}
	second := Image{Reference: secondRef.Normalized(), ManifestDigest: "sha256:second"}

	if err := store.StoreRef(firstRef, first); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreRef(secondRef, second); err != nil {
		t.Fatal(err)
	}

	gotFirst, err := store.LoadRef(firstRef)
	if err != nil {
		t.Fatal(err)
	}
	gotSecond, err := store.LoadRef(secondRef)
	if err != nil {
		t.Fatal(err)
	}

	if gotFirst.ManifestDigest != first.ManifestDigest {
		t.Fatalf("first ref digest=%s want %s", gotFirst.ManifestDigest, first.ManifestDigest)
	}
	if gotSecond.ManifestDigest != second.ManifestDigest {
		t.Fatalf("second ref digest=%s want %s", gotSecond.ManifestDigest, second.ManifestDigest)
	}
}

func testLayer(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	for name, content := range files {
		body := []byte(content)
		if err := tw.WriteHeader(&tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(body)),
		}); err != nil {
			t.Fatal(err)
		}

		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
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

func digest(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}
