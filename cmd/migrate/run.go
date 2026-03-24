package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/migration-tools/influx-migrator/internal/checkpoint"
	"github.com/migration-tools/influx-migrator/internal/config"
	"github.com/migration-tools/influx-migrator/internal/engine"
	"github.com/migration-tools/influx-migrator/internal/logger"
	"github.com/migration-tools/influx-migrator/internal/report"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run migration",
	Long:  `Run a migration task based on the configuration file.`,
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

		logger.Info("starting migration",
			zap.String("name", cfg.Global.Name),
			zap.String("checkpoint_dir", cfg.Global.CheckpointDir),
			zap.Int("parallel_tasks", cfg.Migration.ParallelTasks),
			zap.Int("chunk_size", cfg.Migration.ChunkSize))

		checkpointMgr, err := checkpoint.NewManager(cfg.Global.CheckpointDir)
		if err != nil {
			return fmt.Errorf("failed to create checkpoint manager: %w", err)
		}
		defer checkpointMgr.Close()

		migrationEngine := engine.NewMigrationEngine(cfg, checkpointMgr)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			sig := <-sigChan
			logger.Info("received shutdown signal", zap.String("signal", sig.String()))
			logger.Info("marking in-progress tasks for resume...")
			migrationEngine.MarkInProgressAsInterrupted(ctx)
			cancel()
		}()

		if err := migrationEngine.Run(ctx); err != nil {
			logger.Error("migration failed", zap.Error(err))
			return fmt.Errorf("migration failed: %w", err)
		}

		reportGen := report.NewGenerator(checkpointMgr, cfg.Global.ReportDir)
		migrationReport, err := reportGen.Generate(ctx, cfg.Global.Name, cfg.Global.Name)
		if err != nil {
			logger.Warn("failed to generate report", zap.Error(err))
		} else {
			if err := reportGen.SaveJSON(migrationReport); err != nil {
				logger.Warn("failed to save JSON report", zap.Error(err))
			}
			if err := reportGen.SaveMarkdown(migrationReport); err != nil {
				logger.Warn("failed to save Markdown report", zap.Error(err))
			}
		}

		logger.Info("migration completed successfully")
		return nil
	},
}

func init() {
	runCmd.Flags().StringP("config", "c", "", "Path to configuration file (required)")
	runCmd.MarkFlagRequired("config")
}
