package main

import (
	"github.com/cdesiniotis/chord"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strconv"
	"time"
)

func Create(cfg *chord.Config) error {
	chord.CreateChord(cfg)
	return nil
}

func Join(cfg *chord.Config, ip string, port int) error {
	_, err := chord.JoinChord(cfg, ip, port)
	return err
}

func readConfig(filename string, defaults map[string]interface{}) (*viper.Viper, error) {
	v := viper.New()
	for key, value := range defaults {
		v.SetDefault(key, value)
	}
	v.SetConfigName(filename) // name of config file without extensions
	v.AddConfigPath(".")
	v.AddConfigPath("./server")
	v.AutomaticEnv()
	err := v.ReadInConfig()
	return v, err
}

func defaults() map[string]interface{} {
	return map[string]interface{}{
		"keysize":                  8,
		"addr":                     "0.0.0.0",
		"port":                     8000,
		"timeout":                  2000,
		"stabilizeinterval":        250,
		"fixfingerinterval":        50,
		"checkpredecessorinterval": 150,
		"successorlistsize":		2,
		"logging":					true,
	}
}

func main() {

	// read config file
	v, err := readConfig("config", defaults())
	if err != nil {
		log.Fatalf("error when reading config: %v\n", err)
	}

	// unmarshal to chord config struct
	var cfg *chord.Config
	err = v.Unmarshal(&cfg)
	if err != nil {
		log.Fatalf("error unmarshalling config: %v\n", err)
	}
	cfg = chord.SetDefaultGrpcOpts(cfg)

	var cmdCreate = &cobra.Command{
		Use:   "create",
		Short: "Create a new chord dht ring",
		Long:  `create is for creating a new chord distributed hash table`,
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			err := Create(cfg)
			if err != nil {
				log.Fatalf("error calling Create(cfg): %v\n", err)
			}
			for {
				time.Sleep(5 * time.Second)
			}
		},
	}

	var cmdJoin = &cobra.Command{
		Use:   "join [ip] [port]",
		Short: "Join an existing chord dht ring",
		Long:  `join is for joining an existing chord dht ring by contacting the node at ip:port`,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			port, err := strconv.Atoi(args[1])
			if err != nil {
				log.Fatalf("port field in config is not valid\n")
			}

			err = Join(cfg, args[0], port)
			if err != nil {
				log.Fatalf("error calling Join(cfg, ip, port): %v\n", err)
			}
			for {
				time.Sleep(5 * time.Second)
			}
		},
	}

	var rootCmd = &cobra.Command{Use: "chord"}
	rootCmd.AddCommand(cmdCreate, cmdJoin)
	rootCmd.Execute()
}
