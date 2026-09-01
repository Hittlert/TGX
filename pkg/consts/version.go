package consts

// vars below are set by '-X' flag
var (
	Version    = "dev"
	Commit     = "unknown"
	CommitDate = "unknown"
)

const FallbackVersion = "v4.4.18"

// EffectiveVersion returns the current runtime version string.
func EffectiveVersion() string {
	if Version != "" && Version != "dev" {
		return Version
	}
	return FallbackVersion
}
