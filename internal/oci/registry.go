//nolint:wsl_v5
package oci

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"
)

const (
	mediaOCIIndex           = "application/vnd.oci.image.index.v1+json"
	mediaDockerManifestList = "application/vnd.docker.distribution.manifest.list.v2+json"
	mediaOCIManifest        = "application/vnd.oci.image.manifest.v1+json"
	mediaDockerManifest     = "application/vnd.docker.distribution.manifest.v2+json"
	maxManifestSize         = 8 << 20
	maxTokenResponseSize    = 1 << 20
	registryRequestTimeout  = 30 * time.Minute
	defaultPlatformOS       = "linux"
)

type PullOptions struct {
	PlatformOS   string
	Architecture string
	Progress     func(format string, args ...any)
}

type Client struct {
	http *http.Client
}

func NewClient(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: registryRequestTimeout}
	} else if httpClient.Timeout <= 0 {
		configured := *httpClient
		configured.Timeout = registryRequestTimeout
		httpClient = &configured
	}

	return &Client{http: httpClient}
}

//nolint:cyclop // Pull follows OCI index/manifest/config/layer resolution in one flow.
func (c *Client) Pull(
	ctx context.Context,
	ref Reference,
	store *Store,
	opts PullOptions,
) (Image, error) {
	if opts.PlatformOS == "" {
		opts.PlatformOS = defaultPlatformOS
	}

	if opts.Architecture == "" {
		opts.Architecture = runtime.GOARCH
	}

	if opts.Progress != nil {
		opts.Progress("fetching image manifest: %s", ref.Reference)
	}

	raw, digest, mediaType, err := c.fetchManifest(ctx, ref, ref.Reference)
	if err != nil {
		return Image{}, err
	}

	if isIndex(mediaType, raw) {
		if opts.Progress != nil {
			opts.Progress("manifest is multi-arch index: %s", digest)
		}

		desc, err := selectManifest(raw, opts)
		if err != nil {
			return Image{}, err
		}

		if opts.Progress != nil {
			opts.Progress(
				"selected manifest %s for platform %s/%s",
				desc.Digest,
				opts.PlatformOS,
				opts.Architecture,
			)
		}

		raw, digest, mediaType, err = c.fetchManifest(ctx, ref, desc.Digest)
		if err != nil {
			return Image{}, err
		}
	}

	if !isManifest(mediaType, raw) {
		return Image{}, fmt.Errorf("unsupported manifest media type %q", mediaType)
	}

	if err := store.StoreManifest(digest, raw); err != nil {
		return Image{}, err
	}

	manifest, err := parseManifest(raw)
	if err != nil {
		return Image{}, err
	}

	image := Image{
		Reference:      ref.Normalized(),
		ManifestDigest: digest,
		ConfigDigest:   manifest.Config.Digest,
		Layers:         manifest.Layers,
	}

	if manifest.Config.Digest != "" {
		if opts.Progress != nil {
			opts.Progress("fetching config blob %s", manifest.Config.Digest)
		}

		if err := c.ensureBlob(ctx, ref, manifest.Config, store, opts); err != nil {
			return Image{}, err
		}

		env, err := store.ImageConfigEnv(manifest.Config.Digest)
		if err != nil {
			return Image{}, err
		}
		image.Env = env
	}

	for _, layer := range manifest.Layers {
		if opts.Progress != nil {
			opts.Progress("fetching layer blob %s", layer.Digest)
		}

		if err := c.ensureBlob(ctx, ref, layer, store, opts); err != nil {
			return Image{}, err
		}
	}

	if err := store.StoreRef(ref, image); err != nil {
		return Image{}, err
	}

	return image, nil
}

func (c *Client) ensureBlob(
	ctx context.Context,
	ref Reference,
	desc Descriptor,
	store *Store,
	opts PullOptions,
) error {
	if desc.Size < 0 {
		return fmt.Errorf("blob %s has negative descriptor size", desc.Digest)
	}
	if store.HasBlob(desc.Digest, desc.Size) {
		if opts.Progress != nil {
			opts.Progress("blob already present in cache: %s", desc.Digest)
		}

		return nil
	}

	if opts.Progress != nil {
		opts.Progress("downloading blob %s (%d bytes)", desc.Digest, desc.Size)
	}

	resp, err := c.registryRequest(ctx, ref, "GET", "/v2/"+ref.Repository+"/blobs/"+desc.Digest, "")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch blob %s: registry returned %s", desc.Digest, resp.Status)
	}
	if resp.ContentLength >= 0 && resp.ContentLength != desc.Size {
		return fmt.Errorf(
			"blob %s content length %d does not match descriptor size %d",
			desc.Digest,
			resp.ContentLength,
			desc.Size,
		)
	}

	if err := store.StoreBlob(desc.Digest, desc.Size, resp.Body); err != nil {
		return err
	}

	if opts.Progress != nil {
		opts.Progress("stored blob %s", desc.Digest)
	}

	return nil
}

func (c *Client) fetchManifest(
	ctx context.Context,
	ref Reference,
	target string,
) ([]byte, string, string, error) {
	accept := strings.Join([]string{
		mediaOCIIndex,
		mediaDockerManifestList,
		mediaOCIManifest,
		mediaDockerManifest,
	}, ", ")

	resp, err := c.registryRequest(
		ctx,
		ref,
		http.MethodGet,
		"/v2/"+ref.Repository+"/manifests/"+target,
		accept,
	)
	if err != nil {
		return nil, "", "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, "", "", fmt.Errorf(
			"fetch manifest %s: registry returned %s",
			target,
			resp.Status,
		)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxManifestSize+1))
	if err != nil {
		return nil, "", "", fmt.Errorf("read manifest: %w", err)
	}
	if len(raw) > maxManifestSize {
		return nil, "", "", fmt.Errorf("manifest exceeds %d bytes", maxManifestSize)
	}

	digest := strings.TrimSpace(resp.Header.Get("Docker-Content-Digest"))
	if digest == "" {
		digest = DigestBytes(raw)
	} else if got := DigestBytes(raw); got != digest {
		return nil, "", "", fmt.Errorf("manifest digest mismatch: got %s want %s", got, digest)
	}
	if err := validateDigest(digest, "manifest"); err != nil {
		return nil, "", "", err
	}
	if digestPattern.MatchString(target) && !strings.EqualFold(digest, target) {
		return nil, "", "", fmt.Errorf("manifest digest mismatch: got %s want %s", digest, target)
	}

	mediaType := resp.Header.Get("Content-Type")
	if parsed, _, err := mime.ParseMediaType(mediaType); err == nil {
		mediaType = parsed
	}

	return raw, digest, mediaType, nil
}

func (c *Client) registryRequest(
	ctx context.Context,
	ref Reference,
	method string,
	path string,
	accept string,
) (*http.Response, error) {
	u := url.URL{Scheme: "https", Host: ref.RegistryHost(), Path: path}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return nil, err
	}

	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}

	challenge := resp.Header.Get("WWW-Authenticate")
	_ = resp.Body.Close()

	token, err := c.bearerToken(ctx, challenge)
	if err != nil {
		return nil, err
	}

	req, err = http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return nil, err
	}

	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	req.Header.Set("Authorization", "Bearer "+token)

	return c.http.Do(req)
}

//nolint:cyclop // Bearer challenge handling stays readable as a single parser/request step.
func (c *Client) bearerToken(ctx context.Context, challenge string) (string, error) {
	scheme, params, ok := strings.Cut(challenge, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", fmt.Errorf("unsupported registry auth challenge %q", challenge)
	}

	values := parseAuthParams(params)
	realm := values.Get("realm")
	if realm == "" {
		return "", errors.New("registry auth challenge missing realm")
	}

	u, err := url.Parse(realm)
	if err != nil {
		return "", err
	}
	if u.Scheme != "https" || u.Host == "" {
		return "", fmt.Errorf("registry auth realm must use https: %q", realm)
	}

	q := u.Query()
	for _, key := range []string{"service", "scope"} {
		if value := values.Get(key); value != "" {
			q.Set(key, value)
		}
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("registry token request returned %s", resp.Status)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenResponseSize+1))
	if err != nil {
		return "", fmt.Errorf("read registry token response: %w", err)
	}
	if len(raw) > maxTokenResponseSize {
		return "", fmt.Errorf("registry token response exceeds %d bytes", maxTokenResponseSize)
	}

	var body struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return "", err
	}

	if body.Token != "" {
		return body.Token, nil
	}

	if body.AccessToken != "" {
		return body.AccessToken, nil
	}

	return "", errors.New("registry token response missing token")
}

func parseAuthParams(value string) url.Values {
	out := url.Values{}

	for _, part := range splitAuthParams(value) {
		key, raw, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}

		out.Set(strings.TrimSpace(key), strings.Trim(strings.TrimSpace(raw), `"`))
	}

	return out
}

func splitAuthParams(value string) []string {
	var parts []string

	var b strings.Builder

	quoted := false

	for _, r := range value {
		switch r {
		case '"':
			quoted = !quoted
			b.WriteRune(r)
		case ',':
			if quoted {
				b.WriteRune(r)
				continue
			}

			parts = append(parts, b.String())
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}

	if b.Len() > 0 {
		parts = append(parts, b.String())
	}

	return parts
}

type manifestIndex struct {
	Manifests []Descriptor `json:"manifests"`
}

type imageManifest struct {
	Config Descriptor   `json:"config"`
	Layers []Descriptor `json:"layers"`
}

type imageConfig struct {
	Config struct {
		Env []string `json:"Env,omitempty"`
	} `json:"config"`
}

func selectManifest(raw []byte, opts PullOptions) (Descriptor, error) {
	var index manifestIndex
	if err := json.Unmarshal(raw, &index); err != nil {
		return Descriptor{}, err
	}

	for _, manifest := range index.Manifests {
		if manifest.Platform == nil {
			continue
		}

		if manifest.Platform.OS == opts.PlatformOS &&
			manifest.Platform.Architecture == opts.Architecture {
			if err := validateDigest(manifest.Digest, "manifest descriptor"); err != nil {
				return Descriptor{}, err
			}

			return manifest, nil
		}
	}

	return Descriptor{}, fmt.Errorf(
		"no image manifest for platform %s/%s",
		opts.PlatformOS,
		opts.Architecture,
	)
}

func parseManifest(raw []byte) (imageManifest, error) {
	var manifest imageManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return imageManifest{}, err
	}

	if manifest.Config.Digest != "" {
		if err := validateDigest(manifest.Config.Digest, "config"); err != nil {
			return imageManifest{}, err
		}
		if manifest.Config.Size < 0 {
			return imageManifest{}, errors.New("config descriptor has negative size")
		}
	}

	if len(manifest.Layers) == 0 {
		return imageManifest{}, errors.New("image manifest has no layers")
	}

	for i, layer := range manifest.Layers {
		if err := validateDigest(layer.Digest, fmt.Sprintf("layer %d", i)); err != nil {
			return imageManifest{}, err
		}
		if layer.Size < 0 {
			return imageManifest{}, fmt.Errorf("layer %d has negative size", i)
		}
	}

	return manifest, nil
}

func isIndex(mediaType string, raw []byte) bool {
	if mediaType == mediaOCIIndex || mediaType == mediaDockerManifestList {
		return true
	}

	return bytes.Contains(raw, []byte(`"manifests"`))
}

func isManifest(mediaType string, raw []byte) bool {
	if mediaType == mediaOCIManifest || mediaType == mediaDockerManifest {
		return true
	}

	return bytes.Contains(raw, []byte(`"layers"`))
}
