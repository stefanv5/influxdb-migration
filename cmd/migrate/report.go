package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var reportCmd = &cobra.Command{
	Use:   "report",
	Short: "Generate migration report",
	Long:  `Generate a migration report in JSON, HTML, or Markdown format.`,
	Args:  cobra.ExactArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
		format, _ := cmd.Flags().GetString("format")
		output, _ := cmd.Flags().GetString("output")
		taskID, _ := cmd.Flags().GetString("task-id")

		if taskID == "" {
			return fmt.Errorf("task-id is required (use --task-id)")
		}

		fmt.Printf("Generating %s report for task: %s\n", format, taskID)
		fmt.Printf("Output: %s\n", output)

		return nil
	},
}

func init() {
	reportCmd.Flags().StringP("format", "f", "json", "Report format (json, html, markdown)")
	reportCmd.Flags().StringP("output", "o", "", "Output file path")
	reportCmd.Flags().StringP("task-id", "t", "", "Task ID to generate report for")
	reportCmd.MarkFlagRequired("task-id")
}
