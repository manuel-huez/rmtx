package pathutil

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func SecureJoin(root, rel string) (string, error) {
	clean := filepath.Clean(rel)
	joined := filepath.Join(root, clean)

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}

	absJoined, err := filepath.Abs(joined)
	if err != nil {
		return "", err
	}

	if absJoined != absRoot && !strings.HasPrefix(absJoined, absRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("path %s escapes root %s", rel, root)
	}

	return absJoined, nil
}
