package protocol

import (
	"time"

	"github.com/manuel-huez/rmtx/internal/syncfs"
)

const (
	MsgAuthHello              = "auth_hello"
	MsgAuthResponse           = "auth_response"
	MsgAuthOK                 = "auth_ok"
	MsgError                  = "error"
	MsgRunRequest             = "run_request"
	MsgNeedBlobs              = "need_blobs"
	MsgBlob                   = "blob"
	MsgSyncComplete           = "sync_complete"
	MsgWorkspaceReady         = "workspace_ready"
	MsgStdinData              = "stdin_data"
	MsgStdinClose             = "stdin_close"
	MsgResizeTTY              = "resize_tty"
	MsgExecOutput             = "exec_output"
	MsgExecExit               = "exec_exit"
	MsgChangeSet              = "change_set"
	MsgChangeBlob             = "change_blob"
	MsgChangesDone            = "changes_done"
	MsgPingRequest            = "ping_request"
	MsgPingResponse           = "ping_response"
	MsgListContextsRequest    = "list_contexts_request"
	MsgListContextsResponse   = "list_contexts_response"
	MsgDeleteContextsRequest  = "delete_contexts_request"
	MsgDeleteContextsResponse = "delete_contexts_response"
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

type TTYSize struct {
	Rows int `json:"rows"`
	Cols int `json:"cols"`
}

type WorkspaceReady struct {
	ContextID string `json:"context_id,omitempty"`
	Created   bool   `json:"created,omitempty"`
	Workspace string `json:"workspace,omitempty"`
}

type RunRequest struct {
	ContextID   string             `json:"context_id"`
	ContextName string             `json:"context_name,omitempty"`
	WorkDir     string             `json:"work_dir"`
	Command     []string           `json:"command"`
	Env         map[string]string  `json:"env"`
	Mounts      []syncfs.MountSpec `json:"mounts"`
	Manifest    []syncfs.Entry     `json:"manifest"`
	Session     string             `json:"session"`
	Project     string             `json:"project,omitempty"`
	RootHint    string             `json:"root_hint,omitempty"`
	TTY         bool               `json:"tty,omitempty"`
	TTYRows     int                `json:"tty_rows,omitempty"`
	TTYCols     int                `json:"tty_cols,omitempty"`
}

type PingRequest struct{}

type PingResponse struct {
	Online       bool      `json:"online"`
	Version      string    `json:"version"`
	Name         string    `json:"name,omitempty"`
	Address      string    `json:"address,omitempty"`
	Now          time.Time `json:"now"`
	ContextCount int       `json:"context_count,omitempty"`
}

type ListContextsRequest struct{}

type ContextSummary struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Path      string    `json:"path"`
	Workspace string    `json:"workspace"`
	RootHint  string    `json:"root_hint,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
	Active    bool      `json:"active,omitempty"`
}

type ListContextsResponse struct {
	Contexts []ContextSummary `json:"contexts"`
}

type DeleteContextsRequest struct {
	IDs       []string `json:"ids,omitempty"`
	All       bool     `json:"all,omitempty"`
	OlderThan string   `json:"older_than,omitempty"`
}

type DeleteContextsResponse struct {
	Deleted  []ContextSummary `json:"deleted,omitempty"`
	NotFound []string         `json:"not_found,omitempty"`
}
