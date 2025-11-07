package main

import (
	"fmt"
	"mapreduce/mr"
	"mapreduce/mrapps"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	// start worker (reference business implementation)
	mr.Worker(mrapps.Map, mrapps.Reduce)
}
