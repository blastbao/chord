package main

import (
	"github.com/cdesiniotis/chord"
	"time"
)

func main() {

	addr := "0.0.0.0"
	port := 8001

	cfg := chord.DefaultConfig(addr, port)

	chord.CreateChord(cfg)

	for {
		time.Sleep(5 * time.Second)
	}

}
