package wslconfig

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

const (
	SectionWSL2         = "wsl2"
	DefaultFileMode     = 0o644
	HalfProfileFraction = 0.5
	FullProfileFraction = 1
	MinResourceLimit    = 1
	OneGiB              = 1024 * 1024 * 1024
)

var ErrUnsupported = errors.New("WSL configuration is only supported on Windows")

type SystemSpecs struct {
	LogicalProcessors int
	TotalMemoryBytes  uint64
}

type File struct {
	Path     string
	Exists   bool
	Settings map[string]string
	Content  string
}

type Profile struct {
	Name     string
	Fraction float64
	Settings map[string]string
}

func DefaultPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(home, ".wslconfig"), nil
}

func Read(path string) (File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return File{Path: path, Settings: map[string]string{}}, nil
		}

		return File{}, err
	}

	content := string(data)

	return File{
		Path:     path,
		Exists:   true,
		Settings: ParseSection(content, SectionWSL2),
		Content:  content,
	}, nil
}

func Write(path string, content string) error {
	return os.WriteFile(path, []byte(content), DefaultFileMode)
}

func Shutdown(ctx context.Context) error {
	if runtime.GOOS != "windows" {
		return ErrUnsupported
	}

	cmd := exec.CommandContext(ctx, "wsl.exe", "--shutdown")
	out, err := cmd.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(out))
		if message != "" {
			return fmt.Errorf("%w: %s", err, message)
		}

		return err
	}

	return nil
}

func Profiles(specs SystemSpecs) ([]Profile, error) {
	half, err := ProfileSettings(specs, HalfProfileFraction)
	if err != nil {
		return nil, err
	}

	all, err := ProfileSettings(specs, FullProfileFraction)
	if err != nil {
		return nil, err
	}

	return []Profile{
		{Name: "50%", Fraction: HalfProfileFraction, Settings: half},
		{Name: "100%", Fraction: FullProfileFraction, Settings: all},
	}, nil
}

func ProfileSettings(specs SystemSpecs, fraction float64) (map[string]string, error) {
	if specs.LogicalProcessors <= 0 {
		return nil, errors.New("logical processor count must be greater than zero")
	}

	if specs.TotalMemoryBytes == 0 {
		return nil, errors.New("total memory must be greater than zero")
	}

	if fraction <= 0 || fraction > FullProfileFraction {
		return nil, fmt.Errorf("profile fraction %.2f is out of range", fraction)
	}

	processors := int(float64(specs.LogicalProcessors) * fraction)
	processors = max(processors, MinResourceLimit)

	memoryGiB := int(float64(specs.TotalMemoryBytes/OneGiB) * fraction)
	memoryGiB = max(memoryGiB, MinResourceLimit)

	return map[string]string{
		"processors": strconv.Itoa(processors),
		"memory":     fmt.Sprintf("%dGB", memoryGiB),
	}, nil
}

func Apply(content string, section string, updates map[string]string) string {
	newline := "\n"
	if strings.Contains(content, "\r\n") {
		newline = "\r\n"
		content = strings.ReplaceAll(content, "\r\n", "\n")
	}

	lines := splitLines(content)
	start, end := sectionRange(lines, section)
	if start == -1 {
		return appendSection(lines, section, updates, newline)
	}

	managed := make(map[string]string, len(updates))
	for key, value := range updates {
		managed[strings.ToLower(key)] = value
	}

	seen := make(map[string]bool, len(managed))
	for i := start + 1; i < end; i++ {
		key, ok := settingKey(lines[i])
		if !ok {
			continue
		}

		lower := strings.ToLower(key)
		value, exists := managed[lower]
		if !exists {
			continue
		}

		lines[i] = key + "=" + value
		seen[lower] = true
	}

	remaining := make(map[string]string, len(managed)-len(seen))
	for key, value := range managed {
		if !seen[key] {
			remaining[key] = value
		}
	}

	if len(remaining) > 0 {
		insert := orderedSettings(remaining)
		lines = append(lines[:end], append(insert, lines[end:]...)...)
	}

	return strings.Join(lines, newline)
}

func ParseSection(content string, section string) map[string]string {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	lines := splitLines(content)
	start, end := sectionRange(lines, section)
	settings := map[string]string{}
	if start == -1 {
		return settings
	}

	for _, line := range lines[start+1 : end] {
		key, ok := settingKey(line)
		if !ok {
			continue
		}

		_, value, _ := strings.Cut(line, "=")
		settings[strings.ToLower(key)] = strings.TrimSpace(value)
	}

	return settings
}

func splitLines(content string) []string {
	if content == "" {
		return nil
	}

	lines := strings.Split(content, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		return lines[:len(lines)-1]
	}

	return lines
}

func appendSection(lines []string, section string, updates map[string]string, newline string) string {
	if len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) != "" {
		lines = append(lines, "")
	}

	lines = append(lines, "["+section+"]")
	lines = append(lines, orderedSettings(updates)...)

	return strings.Join(lines, newline)
}

func orderedSettings(settings map[string]string) []string {
	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		lines = append(lines, key+"="+settings[key])
	}

	return lines
}

func sectionRange(lines []string, section string) (int, int) {
	start := -1

	for i, line := range lines {
		name, ok := sectionName(line)
		if ok && strings.EqualFold(name, section) {
			start = i
			break
		}
	}

	if start == -1 {
		return -1, -1
	}

	end := len(lines)
	for i := start + 1; i < len(lines); i++ {
		if _, ok := sectionName(lines[i]); ok {
			end = i
			break
		}
	}

	return start, end
}

func sectionName(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "[") || !strings.HasSuffix(trimmed, "]") {
		return "", false
	}

	name := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "["), "]"))
	if name == "" {
		return "", false
	}

	return name, true
}

func settingKey(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, ";") {
		return "", false
	}

	key, _, ok := strings.Cut(trimmed, "=")
	if !ok {
		return "", false
	}

	key = strings.TrimSpace(key)
	if key == "" {
		return "", false
	}

	return key, true
}
