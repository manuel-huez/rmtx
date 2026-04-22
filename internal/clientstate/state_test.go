package clientstate

import "testing"

func TestUpsertHostMatchesByFingerprint(t *testing.T) {
	loaded := &Loaded{
		Data: State{
			Hosts: []HostRecord{{
				Address:     "10.0.0.1:33221",
				Fingerprint: "sha256:host",
				Name:        "old-name",
			}},
		},
	}

	loaded.UpsertHost(HostRecord{
		Address:        "10.0.0.2:33221",
		Fingerprint:    "sha256:host",
		Name:           "new-name",
		Paired:         true,
		ClientCertPEM:  "cert",
		ClientKeyPEM:   "key",
		LastPairedCert: "sha256:client",
	})

	if len(loaded.Data.Hosts) != 1 {
		t.Fatalf("expected 1 host record, got %d", len(loaded.Data.Hosts))
	}

	record := loaded.FindHostByFingerprint("sha256:host")
	if record == nil {
		t.Fatal("expected host record by fingerprint")
	}
	if record.Address != "10.0.0.2:33221" {
		t.Fatalf("unexpected address: got %s", record.Address)
	}
	if record.Name != "new-name" {
		t.Fatalf("unexpected name: got %s", record.Name)
	}
}

func TestUpsertHostKeepsDistinctFingerprintAtSameAddress(t *testing.T) {
	loaded := &Loaded{
		Data: State{
			Hosts: []HostRecord{{
				Address:       "10.0.0.1:33221",
				Fingerprint:   "sha256:host-a",
				ClientCertPEM: "cert-a",
				ClientKeyPEM:  "key-a",
			}},
		},
	}

	loaded.UpsertHost(HostRecord{
		Address:       "10.0.0.1:33221",
		Fingerprint:   "sha256:host-b",
		ClientCertPEM: "cert-b",
		ClientKeyPEM:  "key-b",
	})

	if len(loaded.Data.Hosts) != 2 {
		t.Fatalf("expected 2 host records, got %d", len(loaded.Data.Hosts))
	}
	if loaded.FindHostByFingerprint("sha256:host-a") == nil {
		t.Fatal("expected first host record to remain")
	}
	if loaded.FindHostByFingerprint("sha256:host-b") == nil {
		t.Fatal("expected second host record to be added")
	}
}

func TestHostCredentialsPrefersFingerprintOverAddress(t *testing.T) {
	loaded := &Loaded{
		Data: State{
			Hosts: []HostRecord{
				{
					Address:       "10.0.0.1:33221",
					Fingerprint:   "sha256:host-a",
					ClientCertPEM: "cert-a",
					ClientKeyPEM:  "key-a",
				},
				{
					Address:       "10.0.0.1:33221",
					Fingerprint:   "sha256:host-b",
					ClientCertPEM: "cert-b",
					ClientKeyPEM:  "key-b",
				},
			},
		},
	}

	cert, key := loaded.HostCredentials("10.0.0.1:33221", "sha256:host-b")
	if cert != "cert-b" || key != "key-b" {
		t.Fatalf("unexpected credentials: got %q/%q", cert, key)
	}
}
