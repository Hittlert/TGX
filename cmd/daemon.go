package cmd

import (
	"context"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/Hittlert/TGX/app/daemon"
	"github.com/Hittlert/TGX/core/logctx"
	"github.com/Hittlert/TGX/core/storage"
)

func NewDaemon() *cobra.Command {
	opts := daemon.DefaultOptions()
	cmd := &cobra.Command{
		Use:     "daemon",
		Aliases: []string{"serve"},
		Short:   "Accept download tasks through an HTTP API",
		RunE: func(cmd *cobra.Command, _ []string) error {
			for {
				if cmd.Context().Err() != nil {
					return cmd.Context().Err()
				}

				logctx.From(cmd.Context()).Info("Starting Telegram MTProto download daemon session...")
				err := tRun(cmd.Context(), func(ctx context.Context, client *telegram.Client, kvd storage.Storage) error {
					return daemon.Run(logctx.Named(ctx, "daemon"), client, kvd, opts)
				})

				if cmd.Context().Err() != nil {
					return cmd.Context().Err()
				}

				logctx.From(cmd.Context()).Warn("daemon MTProto session closed/disconnected, auto-reconnecting in 2 seconds...", zap.Error(err))
				select {
				case <-cmd.Context().Done():
					return cmd.Context().Err()
				case <-time.After(2 * time.Second):
				}
			}
		},
	}

	cmd.Flags().StringVar(&opts.Listen, "listen", opts.Listen, "HTTP API listen address")
	cmd.Flags().StringVarP(&opts.OutputDir, "dir", "d", opts.OutputDir, "final download root")
	cmd.Flags().StringVar(&opts.TempDir, "temp-dir", opts.TempDir, "temporary download root")
	cmd.Flags().StringVar(&opts.DBPath, "db-path", opts.DBPath, "SQLite database path")
	cmd.Flags().StringVar(&opts.Password, "password", opts.Password, "Web UI login password")
	cmd.Flags().StringVar(&opts.SingboxURL, "singbox-url", opts.SingboxURL, "sing-box REST API URL")
	cmd.Flags().IntVar(&opts.QueueCapacity, "queue-capacity", opts.QueueCapacity, "maximum queued tasks")
	cmd.Flags().IntVar(&opts.TerminalLimit, "terminal-limit", opts.TerminalLimit, "completed task snapshots to retain")
	cmd.Flags().IntVar(&opts.FileConcurrency, "file-concurrency", opts.FileConcurrency, "concurrent files")
	cmd.Flags().IntVar(&opts.Threads, "download-threads", opts.Threads, "maximum threads per file")
	cmd.Flags().IntVar(&opts.PoolSize, "dc-pool-size", opts.PoolSize, "connections per active DC")
	cmd.Flags().BoolVar(&opts.StartPaused, "start-paused", opts.StartPaused, "start without dequeuing tasks")
	cmd.Flags().DurationVar(&opts.ReconnectTimeout, "daemon-reconnect-timeout", opts.ReconnectTimeout, "DC reconnect backoff timeout, zero is unlimited")
	cmd.Flags().DurationVar(&opts.PeerSyncTimeout, "peer-sync-timeout", opts.PeerSyncTimeout, "maximum duration for one dialog cache refresh")
	_ = cmd.MarkFlagDirname("dir")
	_ = cmd.MarkFlagDirname("temp-dir")
	return cmd
}
