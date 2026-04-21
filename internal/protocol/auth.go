package protocol

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

func SignToken(token, nonce string) string {
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write([]byte(nonce))

	return hex.EncodeToString(mac.Sum(nil))
}

func VerifyToken(token, nonce, signature string) bool {
	expected := SignToken(token, nonce)
	return hmac.Equal([]byte(expected), []byte(signature))
}

func RandomNonce() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}

	return hex.EncodeToString(buf), nil
}
