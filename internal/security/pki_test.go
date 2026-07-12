//nolint:goconst // Repeated fixture literals keep each test case self-contained.
package security

import (
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEnsureHostPKIRecoversPendingTransaction(t *testing.T) {
	stateDir := t.TempDir()

	pki, err := generateHostPKI("test-host")
	if err != nil {
		t.Fatal(err)
	}

	content, err := json.Marshal(pki)
	if err != nil {
		t.Fatal(err)
	}

	paths := resolveHostPKIPaths(stateDir)
	if err := os.MkdirAll(filepath.Dir(paths.Pending), dirMode); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(paths.Pending, content, privateMode); err != nil {
		t.Fatal(err)
	}

	recovered, err := EnsureHostPKI(stateDir, "test-host")
	if err != nil {
		t.Fatal(err)
	}

	if string(recovered.CACertPEM) != string(pki.CACertPEM) {
		t.Fatal("recovery changed host identity")
	}

	if _, err := os.Stat(paths.Pending); !os.IsNotExist(err) {
		t.Fatalf("pending transaction remains: %v", err)
	}
}

//nolint:cyclop // Certificate rotation test builds custom PKI fixtures and validates post-rotation state.
func TestEnsureHostPKIRotatesExpiredServerCertificate(t *testing.T) {
	stateDir := t.TempDir()

	pki, err := generateHostPKI("test-host")
	if err != nil {
		t.Fatal(err)
	}

	caCert, err := parseCertPEM(pki.CACertPEM)
	if err != nil {
		t.Fatal(err)
	}

	caKey, err := parsePrivateKeyPEM(pki.CAKeyPEM)
	if err != nil {
		t.Fatal(err)
	}

	serverKey, err := parsePrivateKeyPEM(pki.ServerKeyPEM)
	if err != nil {
		t.Fatal(err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}

	expiredDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "test-host",
		},
		NotBefore:             time.Now().Add(-48 * time.Hour),
		NotAfter:              time.Now().Add(-24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	paths := resolveHostPKIPaths(stateDir)
	if err := writeHostPKI(paths, HostPKI{
		CACertPEM:     pki.CACertPEM,
		CAKeyPEM:      pki.CAKeyPEM,
		ServerCertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: expiredDER}),
		ServerKeyPEM:  pki.ServerKeyPEM,
	}); err != nil {
		t.Fatal(err)
	}

	rotated, err := EnsureHostPKI(stateDir, "test-host")
	if err != nil {
		t.Fatal(err)
	}

	if string(rotated.CACertPEM) != string(pki.CACertPEM) {
		t.Fatal("expected host ca to stay stable when only server cert is expired")
	}

	cert, err := parseCertPEM(rotated.ServerCertPEM)
	if err != nil {
		t.Fatal(err)
	}

	if !cert.NotAfter.After(time.Now()) {
		t.Fatalf("expected rotated server cert to be valid, got %s", cert.NotAfter)
	}

	if cert.NotAfter.Equal(time.Unix(0, 0)) {
		t.Fatal("expected non-zero server cert expiry")
	}
}

//nolint:cyclop // End-to-end PKI rotation check needs full certificate setup and verification chain.
func TestHostIdentityFingerprintSurvivesServerCertificateRotation(t *testing.T) {
	stateDir := t.TempDir()

	pki, err := generateHostPKI("test-host")
	if err != nil {
		t.Fatal(err)
	}

	identityFingerprint, err := HostIdentityFingerprint(pki.CACertPEM)
	if err != nil {
		t.Fatal(err)
	}

	caCert, err := parseCertPEM(pki.CACertPEM)
	if err != nil {
		t.Fatal(err)
	}

	caKey, err := parsePrivateKeyPEM(pki.CAKeyPEM)
	if err != nil {
		t.Fatal(err)
	}

	serverKey, err := parsePrivateKeyPEM(pki.ServerKeyPEM)
	if err != nil {
		t.Fatal(err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}

	expiredDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "test-host",
		},
		NotBefore:             time.Now().Add(-48 * time.Hour),
		NotAfter:              time.Now().Add(-24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	paths := resolveHostPKIPaths(stateDir)
	if err := writeHostPKI(paths, HostPKI{
		CACertPEM:     pki.CACertPEM,
		CAKeyPEM:      pki.CAKeyPEM,
		ServerCertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: expiredDER}),
		ServerKeyPEM:  pki.ServerKeyPEM,
	}); err != nil {
		t.Fatal(err)
	}

	rotated, err := EnsureHostPKI(stateDir, "test-host")
	if err != nil {
		t.Fatal(err)
	}

	serverTLS, advertisedFingerprint, err := ServerTLSConfig(rotated)
	if err != nil {
		t.Fatal(err)
	}

	if advertisedFingerprint != identityFingerprint {
		t.Fatalf(
			"expected stable host fingerprint, got %s want %s",
			advertisedFingerprint,
			identityFingerprint,
		)
	}

	clientTLS, err := ClientTLSConfig(nil, nil, identityFingerprint)
	if err != nil {
		t.Fatal(err)
	}

	if err := clientTLS.VerifyPeerCertificate(
		serverTLS.Certificates[0].Certificate,
		nil,
	); err != nil {
		t.Fatalf("expected rotated server cert to verify with original host fingerprint: %v", err)
	}
}
