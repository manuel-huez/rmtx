package version

import (
	"strconv"
	"strings"
	"unicode"
)

const releaseCoreParts = 3

type releaseVersion struct {
	major int
	minor int
	patch int
	pre   []string
}

func ValidRelease(v string) bool {
	_, ok := parseRelease(v)

	return ok
}

func CompareRelease(a, b string) (int, bool) {
	av, ok := parseRelease(a)
	if !ok {
		return 0, false
	}

	bv, ok := parseRelease(b)
	if !ok {
		return 0, false
	}

	for _, pair := range [][2]int{
		{av.major, bv.major},
		{av.minor, bv.minor},
		{av.patch, bv.patch},
	} {
		if pair[0] < pair[1] {
			return -1, true
		}

		if pair[0] > pair[1] {
			return 1, true
		}
	}

	return comparePrerelease(av.pre, bv.pre), true
}

func parseRelease(v string) (releaseVersion, bool) {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, "v") {
		return releaseVersion{}, false
	}

	core, metadata, hasBuild := strings.Cut(v[1:], "+")
	if core == "" {
		return releaseVersion{}, false
	}

	base, pre, hasPre := strings.Cut(core, "-")

	parts := strings.Split(base, ".")
	if len(parts) != releaseCoreParts {
		return releaseVersion{}, false
	}

	major, ok := parseNumericPart(parts[0])
	if !ok {
		return releaseVersion{}, false
	}

	minor, ok := parseNumericPart(parts[1])
	if !ok {
		return releaseVersion{}, false
	}

	patch, ok := parseNumericPart(parts[2])
	if !ok {
		return releaseVersion{}, false
	}

	var preParts []string

	if hasPre {
		if !validDotIdentifiers(pre) {
			return releaseVersion{}, false
		}

		preParts = strings.Split(pre, ".")
	}

	if hasBuild {
		if !validDotIdentifiers(metadata) {
			return releaseVersion{}, false
		}
	}

	return releaseVersion{major: major, minor: minor, patch: patch, pre: preParts}, true
}

func parseNumericPart(value string) (int, bool) {
	if len(value) > 1 && value[0] == '0' {
		return 0, false
	}

	return parseDigits(value)
}

func validDotIdentifiers(value string) bool {
	if value == "" {
		return false
	}

	for part := range strings.SplitSeq(value, ".") {
		if part == "" {
			return false
		}

		for _, r := range part {
			if unicode.IsDigit(r) ||
				(r >= 'A' && r <= 'Z') ||
				(r >= 'a' && r <= 'z') ||
				r == '-' {
				continue
			}

			return false
		}
	}

	return true
}

func comparePrerelease(a, b []string) int {
	switch {
	case len(a) == 0 && len(b) == 0:
		return 0
	case len(a) == 0:
		return 1
	case len(b) == 0:
		return -1
	}

	limit := min(len(a), len(b))

	for i := range limit {
		cmp := comparePrereleaseIdentifier(a[i], b[i])
		if cmp != 0 {
			return cmp
		}
	}

	if len(a) < len(b) {
		return -1
	}

	if len(a) > len(b) {
		return 1
	}

	return 0
}

func comparePrereleaseIdentifier(a, b string) int {
	aNum, aIsNum := parsePrereleaseNumber(a)
	bNum, bIsNum := parsePrereleaseNumber(b)

	switch {
	case aIsNum && bIsNum:
		if aNum < bNum {
			return -1
		}

		if aNum > bNum {
			return 1
		}

		return 0
	case aIsNum:
		return -1
	case bIsNum:
		return 1
	default:
		return strings.Compare(a, b)
	}
}

func parsePrereleaseNumber(value string) (int, bool) {
	return parseDigits(value)
}

func parseDigits(value string) (int, bool) {
	if value == "" {
		return 0, false
	}

	for _, r := range value {
		if !unicode.IsDigit(r) {
			return 0, false
		}
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, false
	}

	return parsed, true
}
