package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify data integrity",
	Long:  `Verify the integrity of migrated data by comparing source and target.`,
	Args:  cobra.ExactArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
		taskID, _ := cmd.Flags().GetString("task-id")
		if taskID == "" {
			return fmt.Errorf("task-id is required (use --task-id)")
		}

		sampleSize, _ := cmd.Flags().GetInt("sample-size")
		if sampleSize == 0 {
			sampleSize = 100
		}

		fmt.Printf("Verifying data integrity for task: %s\n", taskID)
		fmt.Printf("Sample size: %d\n", sampleSize)

		return nil
	},
}

func init() {
	verifyCmd.Flags().StringP("task-id", "t", "", "Task ID to verify")
	verifyCmd.MarkFlagRequired("task-id")
	verifyCmd.Flags().IntP("sample-size", "s", 100, "Number of records to sample for verification")
}
