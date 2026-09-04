package cmd

import (
	"bytes"
	_ "embed"
	"runtime"
	"runtime/debug"
	"text/template"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/Hittlert/TGX/pkg/consts"
)

//go:embed version.tmpl
var version string

func isSourceDirty() string {
	if consts.Dirty != "" {
		if consts.Dirty == "true" {
			return "true"
		}
		if consts.Dirty == "false" {
			return "false"
		}
		return consts.Dirty
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, s := range bi.Settings {
		if s.Key == "vcs.modified" {
			if s.Value == "true" {
				return "true"
			}
			if s.Value == "false" {
				return "false"
			}
		}
	}
	return "unknown"
}

func NewVersion() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Check the version info",
		RunE: func(cmd *cobra.Command, args []string) error {
			buf := &bytes.Buffer{}
			if err := template.Must(template.New("version").Parse(version)).Execute(buf, map[string]interface{}{
				"Version":   consts.Version,
				"Commit":    consts.Commit,
				"Date":      consts.CommitDate,
				"Dirty":     isSourceDirty(),
				"GoVersion": runtime.Version(),
				"GOOS":      runtime.GOOS,
				"GOARCH":    runtime.GOARCH,
			}); err != nil {
				return err
			}
			color.Blue(buf.String())
			return nil
		},
	}
}
