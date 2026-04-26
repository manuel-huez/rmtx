//go:build !windows

package app

func testIsWindows() bool {
	return false
}
