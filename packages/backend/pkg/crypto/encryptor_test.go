package crypto

import (
	"bytes"
	"testing"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	enc, err := NewAESGCMEncryptor("my-secret")
	if err != nil {
		t.Fatalf("new encryptor: %v", err)
	}

	plaintext := []byte("super-secret-password")
	ciphertext, err := enc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if bytes.Equal(ciphertext, plaintext) {
		t.Fatal("ciphertext must differ from plaintext")
	}

	decrypted, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(decrypted, plaintext) {
		t.Errorf("round trip mismatch: got %q", decrypted)
	}
}

func TestEncryptProducesUniqueCiphertext(t *testing.T) {
	enc, _ := NewAESGCMEncryptor("my-secret")

	a, _ := enc.Encrypt([]byte("same"))
	b, _ := enc.Encrypt([]byte("same"))
	if bytes.Equal(a, b) {
		t.Error("expected random nonce to produce different ciphertexts")
	}
}

func TestDecryptRejectsTamperedCiphertext(t *testing.T) {
	enc, _ := NewAESGCMEncryptor("my-secret")

	ciphertext, _ := enc.Encrypt([]byte("data"))
	ciphertext[len(ciphertext)-1] ^= 0xff

	if _, err := enc.Decrypt(ciphertext); err == nil {
		t.Error("expected error for tampered ciphertext")
	}
	if _, err := enc.Decrypt([]byte("short")); err == nil {
		t.Error("expected error for short ciphertext")
	}
}
