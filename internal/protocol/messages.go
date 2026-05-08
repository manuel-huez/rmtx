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
	MsgBlobTransferRequest      = "blob_transfer_request"
	MsgBlobChunk                = "blob_chunk"
	MsgSyncComplete             = "sync_complete"
	MsgWorkspaceReady           = "workspace_ready"
	MsgStdinData                = "stdin_data"
	MsgStdinClose               = "stdin_close"
	MsgRunCancel                = "run_cancel"
	MsgResizeTTY                = "resize_tty"
	MsgExecOutput               = "exec_output"
	MsgExecExit                 = "exec_exit"
	MsgChangeSet                = "change_set"
	MsgChangesDone              = "changes_done"
	MsgHeartbeat                = "heartbeat"
	MsgPingRequest              = "ping_request"
	MsgPingResponse             = "ping_response"
	MsgHostStatsRequest         = "host_stats_request"
	MsgHostStatsResponse        = "host_stats_response"
	MsgHostUpdateRequest        = "host_update_request"
	MsgHostUpdateResponse       = "host_update_response"
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
	Hashes        []string `json:"hashes"`
	TransferToken string   `json:"transfer_token,omitempty"`
	Parallel      int      `json:"parallel,omitempty"`
	ChunkSize     int64    `json:"chunk_size,omitempty"`
}

type BlobTransferRequest struct {
	ContextID string         `json:"context_id"`
	Session   string         `json:"session"`
	Token     string         `json:"token"`
	Direction string         `json:"direction"`
	Chunk     *BlobChunkInfo `json:"chunk,omitempty"`
}

const (
	BlobTransferUpload   = "upload"
	BlobTransferDownload = "download"
)

type BlobDescriptor = syncfs.BlobDescriptor
type BlobChunkInfo = syncfs.BlobChunkInfo

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

type HostStatsRequest struct {
	ContextID string `json:"context_id,omitempty"`
}

type HostStatsResponse struct {
	Version            string          `json:"version"`
	Name               string          `json:"name,omitempty"`
	Address            string          `json:"address,omitempty"`
	Fingerprint        string          `json:"fingerprint,omitempty"`
	Now                time.Time       `json:"now"`
	OS                 string          `json:"os"`
	Arch               string          `json:"arch"`
	CPU                HostCPUStats    `json:"cpu"`
	Memory             HostMemoryStats `json:"memory"`
	ContextCount       int             `json:"context_count,omitempty"`
	ContextID          string          `json:"context_id,omitempty"`
	ContextDiskBytes   int64           `json:"context_disk_bytes,omitempty"`
	ActiveRuns         int             `json:"active_runs,omitempty"`
	ActiveContextCount int             `json:"active_context_count,omitempty"`
	Warnings           []string        `json:"warnings,omitempty"`
}

type HostCPUStats struct {
	LogicalCores       int       `json:"logical_cores"`
	PhysicalCores      int       `json:"physical_cores,omitempty"`
	UsedPercent        float64   `json:"used_percent"`
	UsedCores          float64   `json:"used_cores"`
	PerCoreUsedPercent []float64 `json:"per_core_used_percent,omitempty"`
}

type HostMemoryStats struct {
	TotalBytes     uint64  `json:"total_bytes"`
	AvailableBytes uint64  `json:"available_bytes"`
	UsedBytes      uint64  `json:"used_bytes"`
	UsedPercent    float64 `json:"used_percent"`
}

type HostUpdateRequest struct {
	Version string `json:"version"`
}

type HostUpdateResponse struct {
	Updated       bool   `json:"updated"`
	Restarting    bool   `json:"restarting,omitempty"`
	Version       string `json:"version"`
	InstallTarget string `json:"install_target,omitempty"`
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
