package pathutil

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

// WSLPath is a Linux path inside a named WSL distro.
type WSLPath struct {
	Distro    string
	LinuxPath string
}

const wslHostShare = `\\wsl.localhost`

// WSLUNCPath maps an absolute Linux path in a WSL distro to its Windows UNC path.
func WSLUNCPath(distro, linuxPath string) (string, error) {
	distro = strings.TrimSpace(distro)
	if distro == "" {
		return "", errors.New("WSL distro is empty")
	}
	if strings.ContainsAny(distro, `/\`) {
		return "", fmt.Errorf("invalid WSL distro name %q", distro)
	}

	cleaned := path.Clean(linuxPath)
	if !strings.HasPrefix(cleaned, "/") {
		return "", fmt.Errorf("WSL path %q is not absolute", linuxPath)
	}
	if cleaned == "/" {
		return wslHostShare + `\` + distro, nil
	}

	return wslHostShare + `\` + distro + `\` +
		strings.ReplaceAll(strings.TrimPrefix(cleaned, "/"), "/", `\`), nil
}

// ParseWSLUNCPath parses \\wsl.localhost\<distro>\... and \\wsl$\<distro>\... paths.
func ParseWSLUNCPath(value string) (WSLPath, bool, error) {
	normalized := strings.ReplaceAll(value, `\`, "/")
	lower := strings.ToLower(normalized)

	var rest string
	switch {
	case strings.HasPrefix(lower, "//wsl.localhost/"):
		rest = normalized[len("//wsl.localhost/"):]
	case strings.HasPrefix(lower, "//wsl$/"):
		rest = normalized[len("//wsl$/"):]
	default:
		return WSLPath{}, false, nil
	}

	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return WSLPath{}, true, errors.New("WSL UNC path missing distro")
	}
	if len(parts) == 1 || parts[1] == "" {
		return WSLPath{Distro: parts[0], LinuxPath: "/"}, true, nil
	}

	return WSLPath{
		Distro:    parts[0],
		LinuxPath: path.Clean("/" + parts[1]),
	}, true, nil
}
