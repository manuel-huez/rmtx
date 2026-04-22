package security

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	dirMode               = 0o700
	privateMode           = 0o600
	shortFingerprintHead  = 16
	shortFingerprintTail  = 12
	rsaKeyBits            = 3072
	certificateSerialBits = 128
)

type HostPKI struct {
	CACertPEM     []byte
	CAKeyPEM      []byte
	ServerCertPEM []byte
	ServerKeyPEM  []byte
}

type hostPKIPaths struct {
	CACert     string
	CAKey      string
	ServerCert string
	ServerKey  string
}

func FingerprintDER(der []byte) string {
	sum := sha256.Sum256(der)
	return "sha256:" + strings.ToLower(hex.EncodeToString(sum[:]))
}

func FingerprintCert(cert *x509.Certificate) string {
	if cert == nil {
		return ""
	}

	return FingerprintDER(cert.Raw)
}

func FingerprintPEM(certPEM []byte) (string, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return "", errors.New("decode pem certificate: missing PEM block")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("parse certificate: %w", err)
	}

	return FingerprintCert(cert), nil
}

func ShortFingerprint(fp string) string {
	if len(fp) <= shortFingerprintHead {
		return fp
	}

	return fp[len(fp)-shortFingerprintTail:]
}

func HostIdentityFingerprint(caCertPEM []byte) (string, error) {
	return FingerprintPEM(caCertPEM)
}

func EnsureHostPKI(stateDir, serverName string) (HostPKI, error) {
	if err := os.MkdirAll(filepath.Join(stateDir, "pki"), dirMode); err != nil {
		return HostPKI{}, fmt.Errorf("create pki dir: %w", err)
	}

	paths := resolveHostPKIPaths(stateDir)

	current, err := loadOrCreateHostPKI(paths, serverName)
	if err != nil {
		return HostPKI{}, err
	}

	return rotateHostPKI(paths, current, serverName)
}

func loadOrCreateHostPKI(paths hostPKIPaths, serverName string) (HostPKI, error) {
	current, err := readHostPKI(paths)
	if errors.Is(err, os.ErrNotExist) {
		pki, err := generateHostPKI(serverName)
		if err != nil {
			return HostPKI{}, err
		}

		if err := writeHostPKI(paths, pki); err != nil {
			return HostPKI{}, err
		}

		return pki, nil
	}

	if err != nil {
		return HostPKI{}, err
	}

	return current, nil
}

func rotateHostPKI(paths hostPKIPaths, current HostPKI, serverName string) (HostPKI, error) {
	rotateCA, rotateServer, err := hostPKIRotation(current)
	if err != nil {
		return HostPKI{}, err
	}

	if rotateCA {
		return regenerateHostPKI(paths, serverName)
	}

	if !rotateServer {
		return current, nil
	}

	serverCertPEM, serverKeyPEM, err := generateServerKeyPair(
		current.CACertPEM,
		current.CAKeyPEM,
		serverName,
	)
	if err != nil {
		return HostPKI{}, err
	}

	current.ServerCertPEM = serverCertPEM

	current.ServerKeyPEM = serverKeyPEM
	if err := writeServerPKI(paths, serverCertPEM, serverKeyPEM); err != nil {
		return HostPKI{}, err
	}

	return current, nil
}

func regenerateHostPKI(paths hostPKIPaths, serverName string) (HostPKI, error) {
	pki, err := generateHostPKI(serverName)
	if err != nil {
		return HostPKI{}, err
	}

	if err := writeHostPKI(paths, pki); err != nil {
		return HostPKI{}, err
	}

	return pki, nil
}

func resolveHostPKIPaths(stateDir string) hostPKIPaths {
	return hostPKIPaths{
		CACert:     filepath.Join(stateDir, "pki", "ca-cert.pem"),
		CAKey:      filepath.Join(stateDir, "pki", "ca-key.pem"),
		ServerCert: filepath.Join(stateDir, "pki", "server-cert.pem"),
		ServerKey:  filepath.Join(stateDir, "pki", "server-key.pem"),
	}
}

func readHostPKI(paths hostPKIPaths) (HostPKI, error) {
	out := HostPKI{}

	var err error
	if out.CACertPEM, err = os.ReadFile(paths.CACert); err != nil {
		return HostPKI{}, fmt.Errorf("read ca cert: %w", err)
	}

	if out.CAKeyPEM, err = os.ReadFile(paths.CAKey); err != nil {
		return HostPKI{}, fmt.Errorf("read ca key: %w", err)
	}

	if out.ServerCertPEM, err = os.ReadFile(paths.ServerCert); err != nil {
		return HostPKI{}, fmt.Errorf("read server cert: %w", err)
	}

	if out.ServerKeyPEM, err = os.ReadFile(paths.ServerKey); err != nil {
		return HostPKI{}, fmt.Errorf("read server key: %w", err)
	}

	return out, nil
}

func writeHostPKI(paths hostPKIPaths, pki HostPKI) error {
	for _, item := range []struct {
		path string
		data []byte
	}{
		{paths.CACert, pki.CACertPEM},
		{paths.CAKey, pki.CAKeyPEM},
		{paths.ServerCert, pki.ServerCertPEM},
		{paths.ServerKey, pki.ServerKeyPEM},
	} {
		if err := writePrivateFileAtomically(item.path, item.data); err != nil {
			return fmt.Errorf("write %s: %w", filepath.Base(item.path), err)
		}
	}

	return nil
}

func writeServerPKI(paths hostPKIPaths, serverCertPEM, serverKeyPEM []byte) error {
	for _, item := range []struct {
		path string
		data []byte
	}{
		{paths.ServerCert, serverCertPEM},
		{paths.ServerKey, serverKeyPEM},
	} {
		if err := writePrivateFileAtomically(item.path, item.data); err != nil {
			return fmt.Errorf("write %s: %w", filepath.Base(item.path), err)
		}
	}

	return nil
}

func writePrivateFileAtomically(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, privateMode); err != nil {
		return err
	}

	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	return nil
}

func hostPKIRotation(pki HostPKI) (bool, bool, error) {
	now := time.Now()

	caCert, caUsable, err := hostCARotationState(pki, now)
	if err != nil {
		return false, false, err
	}

	if !caUsable {
		return true, false, nil
	}

	serverUsable, err := hostServerRotationState(pki, caCert, now)
	if err != nil {
		return false, false, err
	}

	if !serverUsable {
		return false, true, nil
	}

	return false, false, nil
}

func hostCARotationState(
	pki HostPKI,
	now time.Time,
) (*x509.Certificate, bool, error) {
	caCert, ok := parseCertPEMOrNil(pki.CACertPEM)
	if !ok {
		return nil, false, nil
	}

	caKey, ok := parsePrivateKeyPEMOrNil(pki.CAKeyPEM)
	if !ok {
		return nil, false, nil
	}

	if !certUsableNow(caCert, now) {
		return nil, false, nil
	}

	matches, err := publicKeysMatch(caCert.PublicKey, &caKey.PublicKey)
	if err != nil {
		return nil, false, err
	}

	if !matches {
		return nil, false, nil
	}

	return caCert, true, nil
}

func hostServerRotationState(pki HostPKI, caCert *x509.Certificate, now time.Time) (bool, error) {
	serverCert, ok := parseCertPEMOrNil(pki.ServerCertPEM)
	if !ok {
		return false, nil
	}

	if !certUsableNow(serverCert, now) {
		return false, nil
	}

	if !validTLSKeyPair(pki.ServerCertPEM, pki.ServerKeyPEM) {
		return false, nil
	}

	if !certSignedBy(serverCert, caCert) {
		return false, nil
	}

	return true, nil
}

func parseCertPEMOrNil(certPEM []byte) (*x509.Certificate, bool) {
	cert, err := parseCertPEM(certPEM)
	return cert, err == nil
}

func parsePrivateKeyPEMOrNil(keyPEM []byte) (*rsa.PrivateKey, bool) {
	key, err := parsePrivateKeyPEM(keyPEM)
	return key, err == nil
}

func validTLSKeyPair(certPEM, keyPEM []byte) bool {
	_, err := tls.X509KeyPair(certPEM, keyPEM)
	return err == nil
}

func certSignedBy(cert, parent *x509.Certificate) bool {
	return cert.CheckSignatureFrom(parent) == nil
}

func certUsableNow(cert *x509.Certificate, now time.Time) bool {
	if cert == nil {
		return false
	}

	return !now.Before(cert.NotBefore) && !now.After(cert.NotAfter)
}

func publicKeysMatch(left, right any) (bool, error) {
	leftDER, err := x509.MarshalPKIXPublicKey(left)
	if err != nil {
		return false, fmt.Errorf("marshal left public key: %w", err)
	}

	rightDER, err := x509.MarshalPKIXPublicKey(right)
	if err != nil {
		return false, fmt.Errorf("marshal right public key: %w", err)
	}

	return bytes.Equal(leftDER, rightDER), nil
}

func ServerTLSConfig(pki HostPKI) (*tls.Config, string, error) {
	chainPEM := append(append([]byte{}, pki.ServerCertPEM...), pki.CACertPEM...)

	cert, err := tls.X509KeyPair(chainPEM, pki.ServerKeyPEM)
	if err != nil {
		return nil, "", fmt.Errorf("load server keypair: %w", err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(pki.CACertPEM) {
		return nil, "", errors.New("append host ca cert")
	}

	fingerprint, err := HostIdentityFingerprint(pki.CACertPEM)
	if err != nil {
		return nil, "", err
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.VerifyClientCertIfGiven,
		ClientCAs:    caPool,
	}, fingerprint, nil
}

func ClientTLSConfig(certPEM, keyPEM []byte, expectedFingerprint string) (*tls.Config, error) {
	expectedFingerprint = strings.TrimSpace(expectedFingerprint)
	cfg := &tls.Config{
		MinVersion:            tls.VersionTLS13,
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: verifyPeerCertificate(expectedFingerprint),
	}

	if len(certPEM) > 0 || len(keyPEM) > 0 {
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("load client keypair: %w", err)
		}

		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

func verifyPeerCertificate(expectedFingerprint string) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		certs, err := parsePeerCertificates(rawCerts)
		if err != nil {
			return err
		}

		return verifyPeerChain(certs, expectedFingerprint)
	}
}

func parsePeerCertificates(rawCerts [][]byte) ([]*x509.Certificate, error) {
	if len(rawCerts) == 0 {
		return nil, errors.New("host certificate missing")
	}

	certs := make([]*x509.Certificate, 0, len(rawCerts))
	for i, rawCert := range rawCerts {
		cert, err := x509.ParseCertificate(rawCert)
		if err != nil {
			return nil, fmt.Errorf("parse host certificate %d: %w", i, err)
		}

		certs = append(certs, cert)
	}

	return certs, nil
}

func verifyPeerChain(certs []*x509.Certificate, expectedFingerprint string) error {
	if expectedFingerprint == "" {
		return errors.New("host fingerprint is required")
	}

	leaf := certs[0]

	now := time.Now()
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return errors.New("host certificate expired or not yet valid")
	}

	if FingerprintCert(leaf) == expectedFingerprint {
		return nil
	}

	roots, intermediates, matchedIdentity := peerPools(certs[1:], expectedFingerprint)
	if !matchedIdentity {
		return fmt.Errorf(
			"host fingerprint mismatch: got %s want %s",
			FingerprintCert(leaf),
			expectedFingerprint,
		)
	}

	_, err := leaf.Verify(x509.VerifyOptions{
		CurrentTime:   now,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		Roots:         roots,
	})
	if err != nil {
		return fmt.Errorf("verify host certificate: %w", err)
	}

	return nil
}

func peerPools(
	certs []*x509.Certificate,
	expectedFingerprint string,
) (*x509.CertPool, *x509.CertPool, bool) {
	roots := x509.NewCertPool()
	intermediates := x509.NewCertPool()
	matchedIdentity := false

	for _, cert := range certs {
		if FingerprintCert(cert) == expectedFingerprint {
			roots.AddCert(cert)

			matchedIdentity = true

			continue
		}

		intermediates.AddCert(cert)
	}

	return roots, intermediates, matchedIdentity
}

func GenerateClientIdentity(label string) (certPEM, keyPEM, csrPEM []byte, err error) {
	key, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate client key: %w", err)
	}

	subject := pkix.Name{CommonName: defaultCommonName(label, "rmtx-client")}

	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject:            subject,
		SignatureAlgorithm: x509.SHA256WithRSA,
	}, key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create csr: %w", err)
	}

	keyPEM = pem.EncodeToMemory(
		&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)},
	)
	csrPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})

	return nil, keyPEM, csrPEM, nil
}

func GenerateCSRFromKey(keyPEM []byte, label string) ([]byte, error) {
	key, err := parsePrivateKeyPEM(keyPEM)
	if err != nil {
		return nil, err
	}

	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName: defaultCommonName(label, "rmtx-client"),
		},
		SignatureAlgorithm: x509.SHA256WithRSA,
	}, key)
	if err != nil {
		return nil, fmt.Errorf("create csr: %w", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}), nil
}

func SignClientCSR(caCertPEM, caKeyPEM, csrPEM []byte, label string) ([]byte, string, error) {
	caCert, err := parseCertPEM(caCertPEM)
	if err != nil {
		return nil, "", fmt.Errorf("parse ca cert: %w", err)
	}

	caKey, err := parsePrivateKeyPEM(caKeyPEM)
	if err != nil {
		return nil, "", fmt.Errorf("parse ca key: %w", err)
	}

	csrBlock, _ := pem.Decode(csrPEM)
	if csrBlock == nil {
		return nil, "", errors.New("decode csr: missing PEM block")
	}

	csr, err := x509.ParseCertificateRequest(csrBlock.Bytes)
	if err != nil {
		return nil, "", fmt.Errorf("parse csr: %w", err)
	}

	if err := csr.CheckSignature(); err != nil {
		return nil, "", fmt.Errorf("verify csr signature: %w", err)
	}

	serial, err := certificateSerialNumber()
	if err != nil {
		return nil, "", fmt.Errorf("generate serial: %w", err)
	}

	tpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: defaultCommonName(label, csr.Subject.CommonName),
		},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, caCert, csr.PublicKey, caKey)
	if err != nil {
		return nil, "", fmt.Errorf("sign client certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	return certPEM, FingerprintDER(der), nil
}

func parseCertPEM(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, errors.New("decode pem certificate: missing PEM block")
	}

	return x509.ParseCertificate(block.Bytes)
}

func parsePrivateKeyPEM(keyPEM []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, errors.New("decode pem private key: missing PEM block")
	}

	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

func generateHostPKI(serverName string) (HostPKI, error) {
	caKey, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		return HostPKI{}, fmt.Errorf("generate ca key: %w", err)
	}

	now := time.Now()

	caSerial, err := certificateSerialNumber()
	if err != nil {
		return HostPKI{}, fmt.Errorf("generate ca serial: %w", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: caSerial,
		Subject: pkix.Name{
			CommonName: defaultCommonName(serverName, "rmtx-host-ca"),
		},
		NotBefore:             now.Add(-1 * time.Minute),
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
	}

	caDER, err := x509.CreateCertificate(
		rand.Reader,
		caTemplate,
		caTemplate,
		&caKey.PublicKey,
		caKey,
	)
	if err != nil {
		return HostPKI{}, fmt.Errorf("create ca cert: %w", err)
	}

	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return HostPKI{}, fmt.Errorf("parse ca cert: %w", err)
	}

	serverCertPEM, serverKeyPEM, err := generateServerCertificate(caCert, caKey, serverName)
	if err != nil {
		return HostPKI{}, err
	}

	return HostPKI{
		CACertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		CAKeyPEM: pem.EncodeToMemory(
			&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(caKey)},
		),
		ServerCertPEM: serverCertPEM,
		ServerKeyPEM:  serverKeyPEM,
	}, nil
}

func generateServerKeyPair(caCertPEM, caKeyPEM []byte, serverName string) ([]byte, []byte, error) {
	caCert, err := parseCertPEM(caCertPEM)
	if err != nil {
		return nil, nil, fmt.Errorf("parse ca cert: %w", err)
	}

	caKey, err := parsePrivateKeyPEM(caKeyPEM)
	if err != nil {
		return nil, nil, fmt.Errorf("parse ca key: %w", err)
	}

	return generateServerCertificate(caCert, caKey, serverName)
}

func generateServerCertificate(
	caCert *x509.Certificate,
	caKey *rsa.PrivateKey,
	serverName string,
) ([]byte, []byte, error) {
	serverKey, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		return nil, nil, fmt.Errorf("generate server key: %w", err)
	}

	serverSerial, err := certificateSerialNumber()
	if err != nil {
		return nil, nil, fmt.Errorf("generate server serial: %w", err)
	}

	now := time.Now()
	serverTemplate := &x509.Certificate{
		SerialNumber: serverSerial,
		Subject: pkix.Name{
			CommonName: defaultCommonName(serverName, "rmtx-host"),
		},
		NotBefore:             now.Add(-1 * time.Minute),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              uniqueNonEmpty(serverName, "localhost"),
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	serverDER, err := x509.CreateCertificate(
		rand.Reader,
		serverTemplate,
		caCert,
		&serverKey.PublicKey,
		caKey,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create server cert: %w", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		pem.EncodeToMemory(
			&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)},
		),
		nil
}

func defaultCommonName(primary, fallback string) string {
	if strings.TrimSpace(primary) != "" {
		return strings.TrimSpace(primary)
	}

	return fallback
}

func uniqueNonEmpty(values ...string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}

	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}

		if _, ok := seen[value]; ok {
			continue
		}

		seen[value] = struct{}{}
		out = append(out, value)
	}

	return out
}

func certificateSerialNumber() (*big.Int, error) {
	return rand.Int(
		rand.Reader,
		new(big.Int).Lsh(big.NewInt(1), certificateSerialBits),
	)
}
