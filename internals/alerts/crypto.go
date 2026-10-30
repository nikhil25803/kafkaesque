package alerts

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

type webhookCipher struct {
	aead cipher.AEAD
}

func newWebhookCipher(encodedKey string) (*webhookCipher, error) {
	key, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("alert encryption key must be a base64-encoded 32-byte key")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &webhookCipher{aead: aead}, nil
}

func (c *webhookCipher) encrypt(value string) ([]byte, []byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, err
	}
	return c.aead.Seal(nil, nonce, []byte(value), nil), nonce, nil
}

func (c *webhookCipher) decrypt(ciphertext, nonce []byte) (string, error) {
	plaintext, err := c.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt Slack webhook: %w", err)
	}
	return string(plaintext), nil
}
