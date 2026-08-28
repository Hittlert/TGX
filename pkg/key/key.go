package key

import (
	"github.com/Hittlert/TGX/core/storage/keygen"
)

func App() string {
	return keygen.New("app")
}

func Resume(fingerprint string) string {
	return keygen.New("resume", fingerprint)
}
