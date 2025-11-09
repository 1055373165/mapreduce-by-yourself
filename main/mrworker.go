package main

import (
	"fmt"
	"mapreduce/mr"
	"mapreduce/mrapps"
	"mapreduce/mrapps/urlcount"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker <app-name>\n")
		fmt.Fprintf(os.Stderr, "Available apps: wc, urlcount\n")
		os.Exit(1)
	}

	appName := os.Args[1]

	// Select application based on argument
	switch appName {
	case "wc", "wc.so":
		// WordCount application
		mr.Worker(mrapps.Map, mrapps.Reduce)
	case "urlcount", "urlcount.so":
		// URL access statistics application
		mr.Worker(urlcount.Map, urlcount.Reduce)
	default:
		fmt.Fprintf(os.Stderr, "Unknown application: %s\n", appName)
		fmt.Fprintf(os.Stderr, "Available apps: wc, urlcount\n")
		os.Exit(1)
	}
}
