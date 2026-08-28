//go:build !linux && !darwin

package atomic

import (
	"fmt"
	"os"
)

func commitFile(tempPath, finalPath string) error {
	if _, err := os.Stat(finalPath); err == nil {
		return ErrTargetExists
	}

	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("rename failed: %w", err)
	}

	return nil
}

func syncDir(dirPath string) error {
	return nil
}
