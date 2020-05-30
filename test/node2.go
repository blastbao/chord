package main

import (
	"github.com/cdesiniotis/chord"
	"os"
	"time"
)

func main() {
	addr := "0.0.0.0"
	port := 8002

	cfg := chord.DefaultConfig(addr, port)
	_, err := chord.JoinChord(cfg, "0.0.0.0", 8001)
	if err != nil {
		os.Exit(1)
	}

	for {
		time.Sleep(5 * time.Second)
	}

}
