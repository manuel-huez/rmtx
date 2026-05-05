package host

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/version"
)

type updateRunner func(context.Context, *log.Logger, string, string, io.Writer) (updateResult, error)

type updateResult struct {
	InstallTarget string
	Executable    string
}

func defaultUpdateRunner(
	ctx context.Context,
	logger *log.Logger,
	targetVersion string,
	installDir string,
	live io.Writer,
) (updateResult, error) {
	target := updateInstallTarget(targetVersion)
	result := updateResult{
		InstallTarget: target,
		Executable:    updateExecutablePath(installDir),
	}

	if err := os.MkdirAll(installDir, defaultDirMode); err != nil {
		return result, fmt.Errorf("prepare update install dir: %w", err)
	}

	cmd := exec.CommandContext(ctx, "go", "install", target)
	cmd.Env = environWith("GOBIN", installDir)

	out, err := runCommandWithLiveOutput(logger, cmd, "host update", live, live)
	if err != nil {
		return result, fmt.Errorf(
			"go install %s: %s: %w",
			target,
			strings.TrimSpace(string(out)),
			err,
		)
	}

	if _, err := os.Stat(result.Executable); err != nil {
		return result, fmt.Errorf("find installed executable %s: %w", result.Executable, err)
	}

	return result, nil
}

func updateInstallTarget(targetVersion string) string {
	return version.CommandPackage + "@" + targetVersion
}

func updateInstallDir(stateDir, targetVersion string) (string, error) {
	return filepath.Abs(filepath.Join(stateDir, "updates", targetVersion))
}

func updateExecutablePath(installDir string) string {
	return filepath.Join(installDir, path.Base(version.CommandPackage)+updateExecutableSuffix)
}

func environWith(key, value string) []string {
	env := os.Environ()
	out := make([]string, 0, len(env)+1)

	for _, entry := range env {
		before, _, ok := strings.Cut(entry, "=")
		if ok && strings.EqualFold(before, key) {
			continue
		}

		out = append(out, entry)
	}

	return append(out, key+"="+value)
}

func (s *Server) handleHostUpdateRequest(
	parent context.Context,
	conn *protocol.Conn,
	req protocol.HostUpdateRequest,
	requestLogs *hostLogSubscription,
) error {
	targetVersion := strings.TrimSpace(req.Version)
	if !version.ValidRelease(targetVersion) {
		return fmt.Errorf("invalid host update version %q", req.Version)
	}

	s.updateMu.Lock()
	defer s.updateMu.Unlock()

	currentVersion := version.String()
	if cmp, ok := version.CompareRelease(currentVersion, targetVersion); ok && cmp >= 0 {
		return writeJSONAfterLogs(
			conn,
			requestLogs,
			protocol.MsgHostUpdateResponse,
			protocol.HostUpdateResponse{
				Updated: false,
				Version: currentVersion,
			},
		)
	}

	if restart, restarting := s.restartRequest(); restarting {
		return writeJSONAfterLogs(
			conn,
			requestLogs,
			protocol.MsgHostUpdateResponse,
			protocol.HostUpdateResponse{
				Updated:       true,
				Restarting:    true,
				Version:       restart.Version,
				InstallTarget: restart.InstallTarget,
			},
		)
	}

	ctx, cancel := context.WithTimeout(parent, hostUpdateTimeout)
	defer cancel()

	installDir, err := updateInstallDir(s.opts.StateDir, targetVersion)
	if err != nil {
		return fmt.Errorf("resolve update install dir: %w", err)
	}

	runner := s.updateRunner
	if runner == nil {
		runner = defaultUpdateRunner
	}

	s.logHostUpdateProgress(
		requestLogs,
		"host update installing: target=%s install_dir=%s",
		updateInstallTarget(targetVersion),
		installDir,
	)

	result, err := runner(ctx, s.logger, targetVersion, installDir, requestLogs)
	if err != nil {
		return fmt.Errorf("update host to %s: %w", targetVersion, err)
	}

	if strings.TrimSpace(result.InstallTarget) == "" {
		result.InstallTarget = updateInstallTarget(targetVersion)
	}

	if strings.TrimSpace(result.Executable) == "" {
		return fmt.Errorf("update host to %s: missing restart executable", targetVersion)
	}

	s.logHostUpdateProgress(
		requestLogs,
		"host update installed: target=%s executable=%s",
		result.InstallTarget,
		result.Executable,
	)

	if !s.beginRestart(result.Executable, targetVersion, result.InstallTarget) {
		return fmt.Errorf("host restart already in progress")
	}

	if err := s.waitForActiveRuns(ctx); err != nil {
		s.cancelRestart()

		return err
	}

	if err := writeJSONAfterLogs(
		conn,
		requestLogs,
		protocol.MsgHostUpdateResponse,
		protocol.HostUpdateResponse{
			Updated:       true,
			Restarting:    true,
			Version:       targetVersion,
			InstallTarget: result.InstallTarget,
		},
	); err != nil {
		s.cancelRestart()

		return err
	}

	s.finishRestart()

	return nil
}

func (s *Server) logHostUpdateProgress(
	requestLogs *hostLogSubscription,
	format string,
	args ...any,
) {
	s.logger.Printf(format, args...)

	if requestLogs != nil {
		_, _ = fmt.Fprintf(requestLogs, "rmtx: "+format+"\n", args...)
	}
}
