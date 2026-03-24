package main

import (
	"fmt"

	"github.com/migration-tools/influx-migrator/internal/checkpoint"
	"github.com/migration-tools/influx-migrator/internal/config"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show migration status",
	Long:  `Show the status of migration tasks.`,
	Args:  cobra.ExactArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
		configPath, _ := cmd.Flags().GetString("config")
		if configPath == "" {
			return fmt.Errorf("config file is required (use --config)")
		}

		cfg, err := config.Load(configPath)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		checkpointMgr, err := checkpoint.NewManager(cfg.Global.CheckpointDir)
		if err != nil {
			return fmt.Errorf("failed to create checkpoint manager: %w", err)
		}
		defer checkpointMgr.Close()

		pending, err := checkpointMgr.GetPendingTasks(cmd.Context())
		if err != nil {
			return fmt.Errorf("failed to get pending tasks: %w", err)
		}

		inProgress, err := checkpointMgr.GetInProgressTasks(cmd.Context())
		if err != nil {
			return fmt.Errorf("failed to get in-progress tasks: %w", err)
		}

		failed, err := checkpointMgr.GetFailedTasks(cmd.Context())
		if err != nil {
			return fmt.Errorf("failed to get failed tasks: %w", err)
		}

		fmt.Printf("Migration: %s\n\n", cfg.Global.Name)
		fmt.Printf("Pending: %d\n", len(pending))
		fmt.Printf("In Progress: %d\n", len(inProgress))
		fmt.Printf("Failed: %d\n", len(failed))

		if len(inProgress) > 0 {
			fmt.Println("\nIn Progress Tasks:")
			for _, cp := range inProgress {
				fmt.Printf("  - %s (%s): %d rows processed\n",
					cp.TaskID, cp.SourceTable, cp.ProcessedRows)
			}
		}

		if len(failed) > 0 {
			fmt.Println("\nFailed Tasks:")
			for _, cp := range failed {
				fmt.Printf("  - %s (%s): %s\n",
					cp.TaskID, cp.SourceTable, cp.ErrorMessage)
			}
		}

		if len(pending) == 0 && len(inProgress) == 0 && len(failed) == 0 {
			fmt.Println("\nNo migration tasks found. Run 'migrate run' to start.")
		}

		return nil
	},
}

func init() {
	statusCmd.Flags().StringP("config", "c", "", "Path to configuration file (required)")
	statusCmd.MarkFlagRequired("config")
}
