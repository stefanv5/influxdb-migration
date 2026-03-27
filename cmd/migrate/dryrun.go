package main

import (
	"fmt"

	"github.com/migration-tools/influx-migrator/internal/config"
	"github.com/spf13/cobra"
)

var dryrunCmd = &cobra.Command{
	Use:   "dry-run",
	Short: "Preview migration without executing",
	Long: `Preview what data would be migrated without actually writing to target.
This shows the source and target configurations and estimated data to be migrated.`,
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

		fmt.Printf("=== Migration Dry Run Preview ===\n\n")
		fmt.Printf("Migration Name: %s\n", cfg.Global.Name)
		fmt.Printf("Checkpoint Dir: %s\n", cfg.Global.CheckpointDir)
		fmt.Printf("Report Dir: %s\n\n", cfg.Global.ReportDir)

		fmt.Printf("=== Migration Settings ===\n")
		fmt.Printf("Parallel Tasks: %d\n", cfg.Migration.ParallelTasks)
		fmt.Printf("Chunk Size: %d\n", cfg.Migration.ChunkSize)
		fmt.Printf("Chunk Interval: %s\n", cfg.Migration.ChunkInterval)
		fmt.Printf("Rate Limited: %v\n", cfg.RateLimit.Enabled)
		if cfg.RateLimit.Enabled {
			fmt.Printf("Rate Limit: %.2f points/sec, burst %d\n",
				cfg.RateLimit.PointsPerSecond, cfg.RateLimit.BurstSize)
		}

		fmt.Printf("\n=== Source Adapters ===\n")
		for _, src := range cfg.Sources {
			fmt.Printf("- %s (%s)\n", src.Name, src.Type)
		}

		fmt.Printf("\n=== Target Adapters ===\n")
		for _, tgt := range cfg.Targets {
			fmt.Printf("- %s (%s)\n", tgt.Name, tgt.Type)
		}

		fmt.Printf("\n=== Migration Tasks ===\n")
		for _, taskConfig := range cfg.Tasks {
			fmt.Printf("\nTask: %s\n", taskConfig.Name)
			fmt.Printf("  Source: %s\n", taskConfig.Source)
			fmt.Printf("  Target: %s\n", taskConfig.Target)
			fmt.Printf("  Mappings:\n")

			for i, mapping := range taskConfig.Mappings {
				fmt.Printf("    [%d] %s -> %s\n", i+1, mapping.SourceTable, mapping.TargetMeasurement)
				if len(mapping.Schema.Fields) > 0 {
					fmt.Printf("         Fields: %d configured\n", len(mapping.Schema.Fields))
				}
				if len(mapping.Schema.Tags) > 0 {
					fmt.Printf("         Tags: %d configured\n", len(mapping.Schema.Tags))
				}
				if mapping.TimeRange.Start != "" {
					fmt.Printf("         Time Range: %s to %s\n", mapping.TimeRange.Start, mapping.TimeRange.End)
				}
			}
		}

		fmt.Printf("\nNo actual migration was performed.\n")
		fmt.Printf("Run 'migrate run --config %s' to start migration.\n", configPath)

		return nil
	},
}

func init() {
	dryrunCmd.Flags().StringP("config", "c", "", "Path to configuration file (required)")
	dryrunCmd.MarkFlagRequired("config")
}
