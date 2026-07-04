package pathutil

import "testing"

func TestWSLUNCPath(t *testing.T) {
	got, err := WSLUNCPath("Ubuntu", "/home/me/.local/state/rmtx")
	if err != nil {
		t.Fatal(err)
	}

	want := `\\wsl.localhost\Ubuntu\home\me\.local\state\rmtx`
	if got != want {
		t.Fatalf("WSLUNCPath=%q want %q", got, want)
	}
}

func TestParseWSLUNCPath(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		wantOK     bool
		wantDistro string
		wantPath   string
	}{
		{
			name:       "localhost path",
			value:      `\\wsl.localhost\Ubuntu\home\me\rmtx`,
			wantOK:     true,
			wantDistro: "Ubuntu",
			wantPath:   "/home/me/rmtx",
		},
		{
			name:       "legacy share path",
			value:      `\\wsl$\Debian\var\lib\rmtx`,
			wantOK:     true,
			wantDistro: "Debian",
			wantPath:   "/var/lib/rmtx",
		},
		{
			name:   "non WSL UNC",
			value:  `\\server\share\path`,
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok, err := ParseWSLUNCPath(tt.value)
			if err != nil {
				t.Fatal(err)
			}
			if ok != tt.wantOK {
				t.Fatalf("ok=%t want %t", ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if got.Distro != tt.wantDistro || got.LinuxPath != tt.wantPath {
				t.Fatalf("parsed=%#v want distro=%s path=%s", got, tt.wantDistro, tt.wantPath)
			}
		})
	}
}
