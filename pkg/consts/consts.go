package consts

import (
	"os"
	"path/filepath"
)

func init() {
	dir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}

	HomeDir = dir
	DataDir = filepath.Join(dir, ".tgx")
	tdlDir := filepath.Join(dir, ".tdl")
	if _, err := os.Stat(tdlDir); err == nil {
		DataDir = tdlDir
	}
	LogPath = filepath.Join(DataDir, "log")

	for _, p := range []string{DataDir, LogPath} {
		if err = os.MkdirAll(p, 0o755); err != nil {
			panic(err)
		}
	}
}
