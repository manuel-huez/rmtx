package version

import "runtime/debug"

const devVersion = "dev"
const CommandPackage = "github.com/manuel-huez/rmtx/cmd/rmtx"

func String() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || info.Main.Version == "" || info.Main.Version == "(devel)" {
		return devVersion
	}

	return info.Main.Version
}
