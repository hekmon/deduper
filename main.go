package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	// Set flags
	sideA := flag.String("sideA", "", "Referential directory")
	sideB := flag.String("sideB", "", "Second directory to compare side A against")
	// noDryRun := flag.Bool("apply", false, "By default deduper run in dry run mode: set this flag to actually apply changes")
	flag.Parse()

	// Test paths first
	err := validatePaths(*sideA, *sideB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to validate paths: %s\n", err)
		os.Exit(1)
	}
}
