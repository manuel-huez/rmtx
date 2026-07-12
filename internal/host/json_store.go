package host

import (
	"encoding/json"
	"fmt"
	"io/fs"

	"github.com/manuel-huez/rmtx/internal/pathutil"
)

func writeJSONAtomically(path string, value any, mode fs.FileMode) error {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}

	if err := pathutil.WriteFileAtomically(path, append(content, '\n'), mode); err != nil {
		return fmt.Errorf("write json: %w", err)
	}

	return nil
}
