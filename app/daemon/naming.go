package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
)

// CanonicalPartPath returns the deterministic temporary .part path for a given task ID in baseDir.
// taskID is canonicalized as "chatID:messageID" or string ID.
func CanonicalPartPath(baseDir, taskID string) string {
	hash := sha256.Sum256([]byte(taskID))
	return filepath.Join(baseDir, fmt.Sprintf(".tdl-part-%s.part", hex.EncodeToString(hash[:8])))
}

// CanonicalTaskID formats chatID and messageID into the standard task ID.
func CanonicalTaskID(chatID string, messageID int) string {
	return fmt.Sprintf("%s:%d", chatID, messageID)
}
