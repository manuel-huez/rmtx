package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

const nonceSize = 32

func RandomNonce() (string, error) {
	buf := make([]byte, nonceSize)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}

	return hex.EncodeToString(buf), nil
}
