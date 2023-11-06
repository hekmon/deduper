package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"golang.org/x/sync/semaphore"
)

func main() {
	// Set flags
	sideA := flag.String("sideA", "", "Referential directory")
	sideB := flag.String("sideB", "", "Second directory to compare side A against")
	concurrency := flag.Int("concurrency", runtime.NumCPU(), "Control concurrency for scanning")
	// noDryRun := flag.Bool("apply", false, "By default deduper run in dry run mode: set this flag to actually apply changes")
	flag.Parse()

	// Test paths first
	cleanPathA, cleanPathB, err := validatePaths(*sideA, *sideB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to validate paths: %s\n", err)
		os.Exit(1)
	}

	// Prepare for concurrency
	semaphoreSize := int64(*concurrency)
	if semaphoreSize < 1 {
		semaphoreSize = 1
	}
	tokenPool := semaphore.NewWeighted(semaphoreSize)

	// Start indexing
	pathATree, pathBTree, nbAFiles, err := index(cleanPathA, cleanPathB, tokenPool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to index directories: %s\n", err)
		os.Exit(2)
	}

	// Start dedup
	dedup(pathATree, pathBTree, tokenPool, nbAFiles)
}
