package main

import (
	"context"
	"fmt"
	"time"

	"github.com/migration-tools/influx-migrator/internal/checkpoint"
	"github.com/migration-tools/influx-migrator/internal/config"
	"github.com/migration-tools/influx-migrator/internal/engine"
	"github.com/migration-tools/influx-migrator/internal/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var resumeCmd = &cobra.Command{
	Use:   "resume",
	Short: "Resume failed or interrupted migration",
	Long: `Resume a failed or interrupted migration from the last checkpoint.
This will pick up both failed tasks and tasks that were in-progress when the migration was interrupted.`,
	Args: cobra.ExactArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
		configPath, _ := cmd.Flags().GetString("config")
		if configPath == "" {
			return fmt.Errorf("config file is required (use --config)")
		}

		cfg, err := config.Load(configPath)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		validator := config.NewValidator(cfg)
		if err := validator.Validate(); err != nil {
			return fmt.Errorf("config validation failed: %w", err)
		}

		if err := logger.Init(&logger.Config{
			Level:  cfg.Logging.Level,
			Output: cfg.Logging.Output,
			File: logger.FileConfig{
				Path:       cfg.Logging.File.Path,
				MaxSize:    cfg.Logging.File.MaxSize,
				MaxBackups: cfg.Logging.File.MaxBackups,
				MaxAge:     cfg.Logging.File.MaxAge,
				Compress:   cfg.Logging.File.Compress,
			},
		}); err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}
		defer logger.Sync()

		checkpointMgr, err := checkpoint.NewManager(cfg.Global.CheckpointDir)
		if err != nil {
			return fmt.Errorf("failed to create checkpoint manager: %w", err)
		}
		defer checkpointMgr.Close()

		failed, err := checkpointMgr.GetFailedTasks(cmd.Context())
		if err != nil {
			return fmt.Errorf("failed to get failed tasks: %w", err)
		}

		inProgress, err := checkpointMgr.GetInProgressTasks(cmd.Context())
		if err != nil {
			return fmt.Errorf("failed to get in-progress tasks: %w", err)
		}

		if len(failed) == 0 && len(inProgress) == 0 {
			fmt.Println("No failed or interrupted tasks to resume")
			return nil
		}

		fmt.Printf("Found %d failed tasks and %d interrupted tasks to resume\n", len(failed), len(inProgress))

		ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
		defer cancel()

		migrationEngine := engine.NewMigrationEngine(cfg, checkpointMgr)

		if err := migrationEngine.Resume(ctx); err != nil {
			logger.Error("resume failed", zap.Error(err))
			return fmt.Errorf("resume failed: %w", err)
		}

		logger.Info("resume completed successfully")
		return nil
	},
}

func init() {
	resumeCmd.Flags().StringP("config", "c", "", "Path to configuration file (required)")
	resumeCmd.MarkFlagRequired("config")
}
