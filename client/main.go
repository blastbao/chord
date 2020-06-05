package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/cdesiniotis/chord"
	"github.com/cdesiniotis/chord/chordpb"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"time"
)

func GetChordClient(addr string) (chordpb.ChordClient, error) {
	//ctx, cancel := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dialOpts := make([]grpc.DialOption, 0, 5)
	dialOpts = append(dialOpts, grpc.WithInsecure(), grpc.WithBlock(), grpc.FailOnNonTempDialError(true))
	conn, err := grpc.DialContext(ctx, addr, dialOpts...)
	//conn, err := grpc.Dial(target, n.grpcOpts.dialOpts...)
	if err != nil {
		return nil, err
	}

	client := chordpb.NewChordClient(conn)
	return client, nil
}

func Get(contact string, key string) (*chordpb.Value, error) {
	cc, err := GetChordClient(contact)
	if err != nil {
		//log.Fatalf("error dialing %s\n", contact)
		return nil, errors.New(fmt.Sprintf("error dialing %s - %s\n", contact, err))
	}

	req := &chordpb.Key{Key: key}

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	val, err := cc.Get(ctx, req)
	return val, err
}

func Put(contact string, key string, val []byte) error {
	cc, err := GetChordClient(contact)
	if err != nil {
		//log.Fatalf("error dialing %s\n", contact)
		return errors.New(fmt.Sprintf("error dialing %s - %s\n", contact, err))
	}

	req := &chordpb.KV{Key: key, Value: val}

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cc.Put(ctx, req)
	return err
}

func Locate(contact string, key string) (*chordpb.Node, error) {
	cc, err := GetChordClient(contact)
	if err != nil {
		//log.Fatalf("error dialing %s\n", contact)
		return nil, errors.New(fmt.Sprintf("error dialing %s - %s\n", contact, err))
	}

	req := &chordpb.Key{Key: key}

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	node, err := cc.Locate(ctx, req)
	return node, err
}

func readConfig(filename string, defaults map[string]interface{}) (*viper.Viper, error) {
	v := viper.New()
	for key, value := range defaults {
		v.SetDefault(key, value)
	}
	v.SetConfigName(filename) // name of config file without extensions
	v.AddConfigPath(".")
	v.AddConfigPath("./client")
	v.AutomaticEnv()
	err := v.ReadInConfig()
	return v, err
}

func defaults() map[string]interface{} {
	return map[string]interface{}{
		"addr":	"0.0.0.0:8001",
	}
}

func main() {
	// read config file
	v, err := readConfig("config", defaults())
	if err != nil {
		log.Fatalf("error when reading config: %v\n", err)
	}
	contact := v.GetString("addr")

	var cmdPut = &cobra.Command{
		Use:   "put [key] [value]",
		Short: "Put a key-value pair into the dht",
		Long:  `put is for inserting a key-value pair into the distributed hash table`,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			key := args[0]
			val := []byte(args[1])
			err := Put(contact, key, val)
			if err != nil {
				log.Fatalf("error calling Put(k,v): %s\n", err)
			}
			log.Infof("put kv: (%s, %s) in datastore\n", key, string(val))
		},
	}

	var cmdGet = &cobra.Command{
		Use:   "get [key]",
		Short: "Get a key from the dht",
		Long:  `get is for retrieving the value for a key in the distributed hash table`,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			key := args[0]
			val, err := Get(contact, key)
			if err != nil {
				log.Fatalf("error calling Get(k): %s\n", err)
			}
			log.Infof("%s --> %s", key, string(val.Value))
		},
	}

	var cmdLocate = &cobra.Command{
		Use:   "locate [key]",
		Short: "Locate the node responsible for a key",
		Long:  `locate is for getting the node in the chord ring which is responsible for a key`,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			key := args[0]
			node, err := Locate(contact, key)
			if err != nil {
				log.Fatalf("error calling Locate(k): %s\n", err)
			}
			chord.PrintNode(node, false, "Node storing key")
		},
	}

	var rootCmd = &cobra.Command{Use: "chord"}
	rootCmd.AddCommand(cmdGet, cmdPut, cmdLocate)
	rootCmd.Execute()
}
