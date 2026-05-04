package protocol

import (
	"time"

	"github.com/manuel-huez/rmtx/internal/config"
	"github.com/manuel-huez/rmtx/internal/syncfs"
)

const (
	MsgError                    = "error"
	MsgPairCodeRequest          = "pair_code_request"
	MsgPairCodeResponse         = "pair_code_response"
	MsgPairRequest              = "pair_request"
	MsgPairResponse             = "pair_response"
	MsgRunRequest               = "run_request"
	MsgNeedBlobs                = "need_blobs"
	MsgBlobUploadRequest        = "blob_upload_request"
	MsgBlob                     = "blob"
	MsgSyncComplete             = "sync_complete"
	MsgWorkspaceReady           = "workspace_ready"
	MsgStdinData                = "stdin_data"
	MsgStdinClose               = "stdin_close"
	MsgResizeTTY                = "resize_tty"
	MsgExecOutput               = "exec_output"
	MsgExecExit                 = "exec_exit"
	MsgChangeSet                = "change_set"
	MsgChangeBlob               = "change_blob"
	MsgChangesDone              = "changes_done"
	MsgSyncCompressionStart     = "sync_compression_start"
	MsgPingRequest              = "ping_request"
	MsgPingResponse             = "ping_response"
	MsgListContextsRequest      = "list_contexts_request"
	MsgListContextsResponse     = "list_contexts_response"
	MsgDeleteContextsRequest    = "delete_contexts_request"
	MsgDeleteContextsResponse   = "delete_contexts_response"
	MsgContextArtifactsRequest  = "context_artifacts_request"
	MsgContextArtifactsResponse = "context_artifacts_response"
	MsgCachePruneRequest        = "cache_prune_request"
	MsgCachePruneResponse       = "cache_prune_response"
)

type PairCodeRequest struct {
	ClientLabel string `json:"client_label,omitempty"`
}

type PairCodeResponse struct {
	HostName  string    `json:"host_name,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
}

type PairRequest struct {
	Code                string `json:"code"`
	ClientLabel         string `json:"client_label,omitempty"`
	PreviousFingerprint string `json:"previous_fingerprint,omitempty"`
	CSRPEM              string `json:"csr_pem"`
}

type PairResponse struct {
	ClientCertPEM string `json:"client_cert_pem"`
	Fingerprint   string `json:"fingerprint"`
}

type ErrorMessage struct {
	Message string `json:"message"`
}

type BlobInfo struct {
	Path    string `json:"path,omitempty"`
	Hash    string `json:"hash"`
	Size    int64  `json:"size,omitempty"`
	Mode    uint32 `json:"mode,omitempty"`
	ModTime int64  `json:"mod_time,omitempty"`
}

type OutputInfo struct {
	Stream string `json:"stream"`
}

type ExitInfo struct {
	Code  int    `json:"code"`
	Error string `json:"error,omitempty"`
}

type NeedBlobs struct {
	Hashes      []string `json:"hashes"`
	UploadToken string   `json:"upload_token,omitempty"`
	Parallel    int      `json:"parallel,omitempty"`
}

type BlobUploadRequest struct {
	ContextID   string `json:"context_id"`
	Session     string `json:"session"`
	Token       string `json:"token"`
	Compression string `json:"compression,omitempty"`
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
	Runtime     RuntimeSpec        `json:"runtime,omitempty"`
	Mounts      []syncfs.MountSpec `json:"mounts"`
	Manifest    []syncfs.Entry     `json:"manifest"`
	SyncBack    []string           `json:"sync_back"`
	Session     string             `json:"session"`
	Project     string             `json:"project,omitempty"`
	RootHint    string             `json:"root_hint,omitempty"`
	TTY         bool               `json:"tty,omitempty"`
	TTYRows     int                `json:"tty_rows,omitempty"`
	TTYCols     int                `json:"tty_cols,omitempty"`
}

type RuntimeSpec = config.RuntimeConfig
type RuntimeSetup = config.RuntimeSetup
type RuntimeVolume = config.RuntimeVolume

type PingRequest struct{}

type PingResponse struct {
	Online       bool      `json:"online"`
	Version      string    `json:"version"`
	Name         string    `json:"name,omitempty"`
	Address      string    `json:"address,omitempty"`
	Fingerprint  string    `json:"fingerprint,omitempty"`
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

type ContextArtifactsRequest struct {
	ContextID string `json:"context_id,omitempty"`
	Prune     bool   `json:"prune,omitempty"`
	Delete    bool   `json:"delete,omitempty"`
	Volume    string `json:"volume,omitempty"`
}

type ContextArtifactsResponse struct {
	ContextID string            `json:"context_id,omitempty"`
	Artifacts []ContextArtifact `json:"artifacts,omitempty"`
	Deleted   []ContextArtifact `json:"deleted,omitempty"`
}

type ContextArtifact struct {
	Kind   string `json:"kind"`
	Name   string `json:"name,omitempty"`
	Path   string `json:"path,omitempty"`
	Ref    string `json:"ref,omitempty"`
	Size   int64  `json:"size,omitempty"`
	Detail string `json:"detail,omitempty"`
}

type CachePruneRequest struct{}

type CachePruneResponse struct {
	Deleted []ContextArtifact `json:"deleted,omitempty"`
	Bytes   int64             `json:"bytes,omitempty"`
}
