package alerts

import (
	"encoding/base64"
	"testing"
)

func TestWebhookCipherRoundTripAndTamperDetection(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	cipher, err := newWebhookCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, nonce, err := cipher.encrypt("https://hooks.slack.com/triggers/T/1/secret")
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err := cipher.decrypt(ciphertext, nonce)
	if err != nil || plaintext != "https://hooks.slack.com/triggers/T/1/secret" {
		t.Fatalf("plaintext=%q error=%v", plaintext, err)
	}
	ciphertext[0] ^= 1
	if _, err := cipher.decrypt(ciphertext, nonce); err == nil {
		t.Fatal("tampered ciphertext was accepted")
	}
}

func TestWebhookCipherRejectsInvalidKeys(t *testing.T) {
	for _, key := range []string{"invalid", base64.StdEncoding.EncodeToString(make([]byte, 31))} {
		if _, err := newWebhookCipher(key); err == nil {
			t.Fatalf("key %q was accepted", key)
		}
	}
}
