package main

import (
	"fmt"
	"mapreduce/mr"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	// create coordinator
	m := mr.MakeCoordinator(os.Args[1:], 10)

	// wait for all tasks to complete
	for !m.Done() {
		time.Sleep(1 * time.Second)
	}

	time.Sleep(1 * time.Second)
	fmt.Println("All tasks completed!")
}
