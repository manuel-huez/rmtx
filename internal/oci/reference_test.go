package oci

import "testing"

func TestParseReferenceDefaultsDockerHubLibraryAndLatest(t *testing.T) {
	ref, err := ParseReference("node")
	if err != nil {
		t.Fatal(err)
	}

	if ref.Registry != "docker.io" {
		t.Fatalf("registry=%s", ref.Registry)
	}

	if ref.RegistryHost() != "registry-1.docker.io" {
		t.Fatalf("registry host=%s", ref.RegistryHost())
	}

	if ref.Repository != "library/node" || ref.Tag != "latest" {
		t.Fatalf("unexpected ref: %#v", ref)
	}

	if ref.Normalized() != "docker.io/library/node:latest" {
		t.Fatalf("normalized=%s", ref.Normalized())
	}
}

func TestParseReferenceCustomRegistryDigest(t *testing.T) {
	ref, err := ParseReference("localhost:5000/team/app@sha256:abcdef")
	if err != nil {
		t.Fatal(err)
	}

	if ref.Registry != "localhost:5000" {
		t.Fatalf("registry=%s", ref.Registry)
	}

	if ref.Repository != "team/app" || ref.Digest != "sha256:abcdef" {
		t.Fatalf("unexpected ref: %#v", ref)
	}
}
