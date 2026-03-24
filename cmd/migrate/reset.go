package main

import (
	"fmt"

	"github.com/migration-tools/influx-migrator/internal/checkpoint"
	"github.com/migration-tools/influx-migrator/internal/config"
	"github.com/spf13/cobra"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset migration checkpoints",
	Long: `Reset all checkpoints to start fresh migration.
This will delete all checkpoint data, allowing you to start migration from the beginning.
Use with caution - this cannot be undone.`,
	Args: cobra.ExactArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
		configPath, _ := cmd.Flags().GetString("config")
		if configPath == "" {
			return fmt.Errorf("config file is required (use --config)")
		}

		force, _ := cmd.Flags().GetBool("force")
		if !force {
			fmt.Println("WARNING: This will delete all checkpoint data and start migration from scratch.")
			fmt.Println("Use --force to confirm.")
			return nil
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

		if err := checkpointMgr.ResetAll(cmd.Context()); err != nil {
			return fmt.Errorf("failed to reset checkpoints: %w", err)
		}

		fmt.Println("All checkpoints have been reset. Migration will start from the beginning.")
		return nil
	},
}

func init() {
	resetCmd.Flags().StringP("config", "c", "", "Path to configuration file (required)")
	resetCmd.MarkFlagRequired("config")
	resetCmd.Flags().BoolP("force", "f", false, "Force reset without confirmation")
}
