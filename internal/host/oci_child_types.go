package host

type commandCleanup func() error

func noopCommandCleanup() error {
	return nil
}

type ociBind struct {
	Source   string `json:"source"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"read_only,omitempty"`
}

type ociChildSpec struct {
	RootFS    string    `json:"rootfs"`
	WorkDir   string    `json:"workdir"`
	Command   []string  `json:"command"`
	Env       []string  `json:"env"`
	Binds     []ociBind `json:"binds,omitempty"`
	Network   string    `json:"network,omitempty"`
	GPU       string    `json:"gpu,omitempty"`
	WSLDistro string    `json:"wsl_distro,omitempty"`
}
