package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Multi-source InfluxDB migration tool",
	Long: `A migration tool for transferring data from MySQL, TDengine, and InfluxDB to InfluxDB.
Features:
  - Plugin-based architecture
  - Checkpoint/resume support
  - Incremental sync
  - Rate limiting
  - Migration reports (JSON/HTML/Markdown)`,
	Version: "0.1.0",
}

func main() {
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(resumeCmd)
	rootCmd.AddCommand(reportCmd)
	rootCmd.AddCommand(verifyCmd)
	rootCmd.AddCommand(resetCmd)
	rootCmd.AddCommand(dryrunCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
