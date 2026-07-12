package pathutil

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func SecureJoinExisting(root, rel string) (string, error) {
	clean := filepath.Clean(rel)
	joined := filepath.Join(root, clean)

	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}

	absRoot, err := filepath.Abs(resolvedRoot)
	if err != nil {
		return "", err
	}

	resolvedJoined, err := filepath.EvalSymlinks(joined)
	if err != nil {
		return "", err
	}

	absJoined, err := filepath.Abs(resolvedJoined)
	if err != nil {
		return "", err
	}

	if absJoined != absRoot && !strings.HasPrefix(absJoined, absRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("path %s escapes root %s", rel, root)
	}

	return absJoined, nil
}
