//nolint:wsl_v5
package oci

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	defaultRegistry = "docker.io"
	dockerHubHost   = "registry-1.docker.io"
	defaultTag      = "latest"
)

var digestPattern = regexp.MustCompile(`^[A-Za-z0-9_+.-]+:[0-9a-fA-F]+$`)

type Reference struct {
	Registry   string
	Repository string
	Reference  string
	Digest     string
	Tag        string
	Original   string
}

//nolint:cyclop // Docker-compatible reference normalization has several compact branches.
func ParseReference(value string) (Reference, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return Reference{}, errors.New("image reference is required")
	}

	ref := Reference{Original: value}
	name, digest, ok := strings.Cut(value, "@")
	if ok {
		if !digestPattern.MatchString(digest) {
			return Reference{}, fmt.Errorf("invalid image digest %q", digest)
		}

		ref.Digest = digest
	}

	lastSlash := strings.LastIndex(name, "/")
	lastColon := strings.LastIndex(name, ":")
	if lastColon > lastSlash {
		ref.Tag = name[lastColon+1:]
		name = name[:lastColon]
	}

	if ref.Digest == "" && ref.Tag == "" {
		ref.Tag = defaultTag
	}

	parts := strings.Split(name, "/")
	switch {
	case len(parts) == 1:
		ref.Registry = defaultRegistry
		ref.Repository = "library/" + parts[0]
	case strings.Contains(parts[0], ".") || strings.Contains(parts[0], ":") || parts[0] == "localhost":
		ref.Registry = parts[0]
		ref.Repository = strings.Join(parts[1:], "/")
	default:
		ref.Registry = defaultRegistry
		ref.Repository = strings.Join(parts, "/")
	}

	if ref.Repository == "" || strings.Contains(ref.Repository, "..") {
		return Reference{}, fmt.Errorf("invalid image repository %q", ref.Repository)
	}

	ref.Reference = ref.Tag
	if ref.Digest != "" {
		ref.Reference = ref.Digest
	}

	return ref, nil
}

func (r Reference) RegistryHost() string {
	if r.Registry == defaultRegistry {
		return dockerHubHost
	}

	return r.Registry
}

func (r Reference) Normalized() string {
	s := r.Registry + "/" + r.Repository
	if r.Digest != "" {
		return s + "@" + r.Digest
	}

	return s + ":" + r.Tag
}
