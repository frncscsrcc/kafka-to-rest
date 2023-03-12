/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"kafka-to-rest/pkg/config"
	"kafka-to-rest/pkg/daemon"
	"log"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
)

// startCmd represents the play command
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a proxy in daemon mode",
	Run: func(cmd *cobra.Command, args []string) {

		configFile, _ := cmd.Flags().GetString("config")
		cnf, cnfErr := config.NewConfigFromFile(configFile)
		if cnfErr != nil {
			panic(cnfErr)
		}

		var stopChan = make(chan os.Signal, 2)
		initiateShutdown := make(chan struct{})
		signal.Notify(stopChan, os.Interrupt, os.Kill)

		go func() {
			s, _ := <-stopChan
			log.Printf("Received signal %d", s)
			initiateShutdown <- struct{}{}
		}()

		d := daemon.NewDaemonFromConfig(cnf)

		os.Exit(d.Start(initiateShutdown))
	},
}

func init() {
	rootCmd.AddCommand(startCmd)
	startCmd.PersistentFlags().StringP("config", "", "", "Config (json)")
	startCmd.MarkPersistentFlagRequired("config")
}
