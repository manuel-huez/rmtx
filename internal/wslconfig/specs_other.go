//go:build !windows

package wslconfig

func DetectSystemSpecs() (SystemSpecs, error) {
	return SystemSpecs{}, ErrUnsupported
}
