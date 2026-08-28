package cmd

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

func NewHealthcheck() *cobra.Command {
	var url string
	cmd := &cobra.Command{
		Use:   "healthcheck",
		Short: "Perform HTTP healthcheck for daemon container",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := http.Client{Timeout: 3 * time.Second}
			resp, err := client.Get(url)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "healthcheck failed: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				_, _ = fmt.Fprintf(os.Stderr, "healthcheck status code: %d\n", resp.StatusCode)
				os.Exit(1)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&url, "url", "http://127.0.0.1:18080/healthz", "healthcheck target URL")
	return cmd
}
