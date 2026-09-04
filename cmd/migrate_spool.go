package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/Hittlert/TGX/app/daemon/migration"
	"github.com/Hittlert/TGX/core/logctx"
)

func NewMigrateSpool() *cobra.Command {
	var opts migration.MigrationOptions
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:     "migrate-spool",
		Aliases: []string{"migrate"},
		Short:   "Audit and migrate legacy Spool database records and artifacts to Direct SSD Download model",
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger := logctx.From(cmd.Context())
			logger.Info("Starting legacy Spool migration audit...",
				zap.String("db_path", opts.DBPath),
				zap.String("target_dir", opts.TargetDir),
				zap.Bool("dry_run", opts.DryRun),
			)

			report, err := migration.Run(cmd.Context(), opts)
			if err != nil {
				return fmt.Errorf("migration failed: %w", err)
			}

			if jsonOutput {
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				return enc.Encode(report)
			}

			// Format human-readable output
			fmt.Printf("=== TGX Legacy Spool Migration Report ===\n")
			fmt.Printf("Database:         %s\n", report.DBPath)
			if report.BackupPath != "" {
				fmt.Printf("Backup Verified:  %s\n", report.BackupPath)
			}
			fmt.Printf("Dry Run Mode:     %v\n", report.DryRun)
			fmt.Printf("Legacy Tables:    %v\n", report.LegacyTablesFound)
			fmt.Printf("Total Legacy Rows:%d\n", report.TotalLegacyRows)
			fmt.Printf("Imported Success: %d\n", report.ImportedSuccess)
			fmt.Printf("Reset to Pending: %d\n", report.ResetPending)
			if len(report.DroppedTables) > 0 {
				fmt.Printf("Dropped Tables:   %v\n", report.DroppedTables)
			}
			if len(report.PlannedCleanFiles) > 0 {
				fmt.Printf("Planned Buffer Clean: %d\n", len(report.PlannedCleanFiles))
			}
			if len(report.CleanedFiles) > 0 {
				fmt.Printf("Cleaned Files:    %d\n", len(report.CleanedFiles))
			}
			fmt.Println("----------------------------------------")
			for _, item := range report.Items {
				fmt.Printf("[%s] %s (Chat: %s, Msg: %d): %s\n", item.Disposition, item.SourceTable, item.ChatID, item.MessageID, item.Reason)
			}
			fmt.Println("========================================")
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.DBPath, "db-path", "/app/state/download_records.sqlite3", "path to SQLite database")
	cmd.Flags().StringVarP(&opts.TargetDir, "dir", "d", "/app/downloads", "target download directory")
	cmd.Flags().StringVar(&opts.ArchiveDir, "archive-dir", "", "archive directory (optional)")
	cmd.Flags().StringVar(&opts.BufferDir, "buffer-dir", "", "legacy buffer directory to inspect and clean")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "report inventory and planned dispositions without applying changes")
	cmd.Flags().BoolVar(&opts.CreateBackup, "backup", true, "create a verified database backup before mutating")
	cmd.Flags().BoolVar(&opts.DropLegacyTables, "drop-legacy-tables", true, "drop legacy Spool tables after migration")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "output migration report as JSON")

	return cmd
}
