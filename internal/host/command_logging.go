package host

import (
	"bytes"
	"log"
	"os/exec"
	"strings"
	"sync"
)

type commandOutputCollector struct {
	logger *log.Logger
	prefix string
	mu     *sync.Mutex
	output *bytes.Buffer
}

func (w *commandOutputCollector) Write(p []byte) (int, error) {
	if len(p) > 0 && w.logger != nil {
		line := strings.TrimSpace(string(p))
		if line != "" {
			w.logger.Printf("%s: %s", w.prefix, line)
		}
	}

	if w.output == nil {
		return len(p), nil
	}

	if w.mu == nil {
		return w.output.Write(p)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	return w.output.Write(p)
}

func runCommandWithLiveOutput(
	logger *log.Logger,
	cmd *exec.Cmd,
	prefix string,
) ([]byte, error) {
	output := &bytes.Buffer{}
	mu := &sync.Mutex{}

	cmd.Stdout = &commandOutputCollector{
		logger: logger,
		prefix: prefix,
		mu:     mu,
		output: output,
	}
	cmd.Stderr = &commandOutputCollector{
		logger: logger,
		prefix: prefix + " [stderr]",
		mu:     mu,
		output: output,
	}

	if err := cmd.Start(); err != nil {
		return output.Bytes(), err
	}

	if err := cmd.Wait(); err != nil {
		return output.Bytes(), err
	}

	return output.Bytes(), nil
}
