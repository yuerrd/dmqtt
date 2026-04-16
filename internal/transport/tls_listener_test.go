package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func generateSelfSignedCert(certPath, keyPath string) error {
	// Generate private key using ECDSA
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Test"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:    []string{"localhost"},
	}

	// Create certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return err
	}

	// Write certificate
	certOut, err := os.Create(certPath)
	if err != nil {
		return err
	}
	defer certOut.Close()

	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if err != nil {
		return err
	}

	// Write private key
	keyOut, err := os.Create(keyPath)
	if err != nil {
		return err
	}
	defer keyOut.Close()

	privBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return err
	}

	err = pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privBytes})
	if err != nil {
		return err
	}

	return nil
}

func TestTLSListener_AcceptAndClose(t *testing.T) {
	// Create temporary directory for certs
	tempDir, err := os.MkdirTemp("", "tls_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	certPath := filepath.Join(tempDir, "cert.pem")
	keyPath := filepath.Join(tempDir, "key.pem")

	// Generate self-signed certificate
	err = generateSelfSignedCert(certPath, keyPath)
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Create TLS listener
	l, err := NewTLSListener("127.0.0.1:0", certPath, keyPath)
	if err != nil {
		t.Fatalf("NewTLSListener error: %v", err)
	}
	defer l.Close()

	addr := l.Addr()
	if addr == "" {
		t.Fatal("Addr() returned empty string")
	}

	// Test connection in separate goroutines to avoid deadlock
	done := make(chan error, 1)

	go func() {
		// Accept a connection
		conn, err := l.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()

		// Try to read a byte - this tests that the TLS handshake worked
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, err = conn.Read(buf) // This will likely timeout, but that's OK
		done <- nil             // Success - we accepted a TLS connection
	}()

	// Small delay to ensure Accept is called first
	time.Sleep(10 * time.Millisecond)

	// Connect with TLS client
	config := &tls.Config{
		InsecureSkipVerify: true,
	}

	conn, err := tls.Dial("tcp", addr, config)
	if err != nil {
		t.Fatalf("TLS Dial error: %v", err)
	}
	conn.Close()

	// Wait for Accept to complete
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Accept/handshake error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for Accept")
	}
}

func TestNewTLSListener_BadCert(t *testing.T) {
	// Test with nonexistent cert file
	_, err := NewTLSListener("127.0.0.1:0", "nonexistent.pem", "nonexistent.key")
	if err == nil {
		t.Fatal("Expected error with nonexistent cert files, got nil")
	}

	// Test with cert file but no key file
	tempDir, err := os.MkdirTemp("", "tls_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	certPath := filepath.Join(tempDir, "cert.pem")

	// Create empty cert file
	f, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("Failed to create cert file: %v", err)
	}
	f.Close()

	_, err = NewTLSListener("127.0.0.1:0", certPath, "nonexistent.key")
	if err == nil {
		t.Fatal("Expected error with nonexistent key file, got nil")
	}
}
