package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
)

// Encryptor encrypts and decrypts secrets at rest (e.g. source DB passwords).
type Encryptor interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

// AESGCMEncryptor implements Encryptor using AES-256-GCM. The key is derived
// from an arbitrary-length secret via SHA-256.
type AESGCMEncryptor struct {
	aead cipher.AEAD
}

func NewAESGCMEncryptor(secret string) (*AESGCMEncryptor, error) {
	key := sha256.Sum256([]byte(secret))
	aead, err := newAEAD(key[:])
	if err != nil {
		return nil, err
	}
	return &AESGCMEncryptor{aead: aead}, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}
	return aead, nil
}

// Encrypt returns nonce || ciphertext.
func (e *AESGCMEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}
	return e.aead.Seal(nonce, nonce, plaintext, nil), nil
}

func (e *AESGCMEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < e.aead.NonceSize() {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, ciphertext := ciphertext[:e.aead.NonceSize()], ciphertext[e.aead.NonceSize():]
	plaintext, err := e.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}
	return plaintext, nil
}
