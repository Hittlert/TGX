package fscommit

// GetDiskSpace returns (freeBytes, totalBytes, err) for the given filesystem path.
// It queries real filesystem capacity on Linux, macOS and Windows.
// Returning a fabricated value for unsupported platforms is forbidden.
func GetDiskSpace(path string) (uint64, uint64, error) {
	return getDiskSpace(path)
}
