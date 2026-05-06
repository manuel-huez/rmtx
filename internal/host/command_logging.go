package host

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strings"
	"sync"
)

type commandOutputCollector struct {
	logger *log.Logger
	prefix string
	live   io.Writer
	logMu  sync.Mutex
	line   strings.Builder
	outMu  *sync.Mutex
	output *bytes.Buffer
}

func (w *commandOutputCollector) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	w.log(p)

	if w.output != nil {
		if w.outMu != nil {
			w.outMu.Lock()
			_, _ = w.output.Write(p)
			w.outMu.Unlock()
		} else {
			_, _ = w.output.Write(p)
		}
	}

	if w.live != nil {
		_, _ = w.live.Write(p)
	}

	return len(p), nil
}

func (w *commandOutputCollector) log(p []byte) {
	if w.logger == nil {
		return
	}

	w.logMu.Lock()
	defer w.logMu.Unlock()

	text := strings.ReplaceAll(string(p), "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")

	w.line.WriteString(text)

	for {
		current := w.line.String()

		before, after, ok := strings.Cut(current, "\n")
		if !ok {
			return
		}

		w.logLine(before)
		w.line.Reset()
		w.line.WriteString(after)
	}
}

func (w *commandOutputCollector) Flush() {
	if w.logger == nil {
		return
	}

	w.logMu.Lock()
	defer w.logMu.Unlock()

	if w.line.Len() == 0 {
		return
	}

	w.logLine(w.line.String())
	w.line.Reset()
}

func (w *commandOutputCollector) logLine(line string) {
	line = strings.TrimRight(line, " \t")
	if strings.TrimSpace(line) == "" {
		return
	}

	w.logger.Printf("%s: %s", w.prefix, line)
}

func runCommandWithLiveOutput(
	logger *log.Logger,
	cmd *exec.Cmd,
	prefix string,
	stdoutLive io.Writer,
	stderrLive io.Writer,
) ([]byte, error) {
	output := &bytes.Buffer{}
	outputMu := &sync.Mutex{}

	stdout := &commandOutputCollector{
		logger: logger,
		prefix: prefix,
		live:   stdoutLive,
		outMu:  outputMu,
		output: output,
	}

	stderr := &commandOutputCollector{
		logger: logger,
		prefix: prefix + " [stderr]",
		live:   stderrLive,
		outMu:  outputMu,
		output: output,
	}

	cmd.Stdout = stdout
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		return output.Bytes(), err
	}

	if err := cmd.Wait(); err != nil {
		stdout.Flush()
		stderr.Flush()

		return output.Bytes(), err
	}

	stdout.Flush()
	stderr.Flush()

	return output.Bytes(), nil
}

func writeRunLogLine(w io.Writer, format string, args ...any) {
	if w == nil {
		return
	}

	_, _ = fmt.Fprintf(w, "rmtx: "+format+"\n", args...)
}

func (s *Server) hostOnlyLogger() *log.Logger {
	if s != nil && s.opts.Logger != nil {
		return s.opts.Logger
	}
	if s != nil {
		return s.logger
	}

	return nil
}

func (s *Server) logRun(runLogs io.Writer, format string, args ...any) {
	s.logger.Printf(format, args...)
	if _, streamedByHostLogger := runLogs.(*hostLogSubscription); streamedByHostLogger {
		return
	}
	writeRunLogLine(runLogs, format, args...)
}
