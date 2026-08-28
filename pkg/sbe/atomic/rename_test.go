package atomic

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAtomicCommit_Success(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sbe_atomic_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tempFile := filepath.Join(tmpDir, "video.mp4.part.123")
	finalFile := filepath.Join(tmpDir, "video.mp4")

	err = os.WriteFile(tempFile, []byte("final_data_bytes"), 0644)
	require.NoError(t, err)

	err = CommitFile(tempFile, finalFile)
	require.NoError(t, err)

	assert.NoFileExists(t, tempFile)
	assert.FileExists(t, finalFile)

	data, err := os.ReadFile(finalFile)
	require.NoError(t, err)
	assert.Equal(t, "final_data_bytes", string(data))
}

func TestAtomicCommit_TargetExistsError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sbe_atomic_exist_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tempFile := filepath.Join(tmpDir, "video.mp4.part.456")
	finalFile := filepath.Join(tmpDir, "video.mp4")

	err = os.WriteFile(tempFile, []byte("temp_data"), 0644)
	require.NoError(t, err)

	err = os.WriteFile(finalFile, []byte("existing_data"), 0644)
	require.NoError(t, err)

	// Commit should fail with ErrTargetExists
	err = CommitFile(tempFile, finalFile)
	assert.Error(t, err)
	assert.Equal(t, ErrTargetExists, err)

	// Existing file should remain untouched
	existingData, err := os.ReadFile(finalFile)
	require.NoError(t, err)
	assert.Equal(t, "existing_data", string(existingData))
	assert.FileExists(t, tempFile)
}
