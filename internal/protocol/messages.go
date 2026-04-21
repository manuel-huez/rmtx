package protocol

import "github.com/manuel-huez/rmtx/internal/syncfs"

const (
	MsgAuthHello      = "auth_hello"
	MsgAuthResponse   = "auth_response"
	MsgAuthOK         = "auth_ok"
	MsgError          = "error"
	MsgRunRequest     = "run_request"
	MsgNeedBlobs      = "need_blobs"
	MsgBlob           = "blob"
	MsgSyncComplete   = "sync_complete"
	MsgWorkspaceReady = "workspace_ready"
	MsgStdinData      = "stdin_data"
	MsgStdinClose     = "stdin_close"
	MsgExecOutput     = "exec_output"
	MsgExecExit       = "exec_exit"
	MsgChangeSet      = "change_set"
	MsgChangeBlob     = "change_blob"
	MsgChangesDone    = "changes_done"
)

type AuthHello struct {
	Nonce   string `json:"nonce"`
	Version string `json:"version"`
}

type AuthResponse struct {
	MAC string `json:"mac"`
}

type ErrorMessage struct {
	Message string `json:"message"`
}

type BlobInfo struct {
	Path string `json:"path,omitempty"`
	Hash string `json:"hash"`
	Size int64  `json:"size,omitempty"`
	Mode uint32 `json:"mode,omitempty"`
}

type OutputInfo struct {
	Stream string `json:"stream"`
}

type ExitInfo struct {
	Code  int    `json:"code"`
	Error string `json:"error,omitempty"`
}

type NeedBlobs struct {
	Hashes []string `json:"hashes"`
}

type ChangeSet struct {
	Entries []syncfs.Entry `json:"entries"`
	Deleted []string       `json:"deleted"`
}

type RunRequest struct {
	WorkDir  string             `json:"work_dir"`
	Command  []string           `json:"command"`
	Env      map[string]string  `json:"env"`
	Mounts   []syncfs.MountSpec `json:"mounts"`
	Manifest []syncfs.Entry     `json:"manifest"`
	Session  string             `json:"session"`
	Project  string             `json:"project,omitempty"`
	RootHint string             `json:"root_hint,omitempty"`
}
