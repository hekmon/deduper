package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"golang.org/x/sync/semaphore"
)

var (
	noDryRun bool
)

func main() {
	// Set flags
	dirA := flag.String("dirA", "", "Referential directory")
	dirB := flag.String("dirB", "", "Second directory to compare dirA against")
	workers := flag.Int("workers", runtime.NumCPU(), "Set the maximum numbers of workers")
	flag.BoolVar(&noDryRun, "apply", false, "By default deduper run in dry run mode: set this flag to actually apply changes")
	flag.Parse()

	// Test paths first
	cleanPathA, cleanPathB, err := validatePaths(*dirA, *dirB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to validate paths: %s\n", err)
		os.Exit(1)
	}

	// Prepare for concurrency
	semaphoreSize := int64(*workers)
	if semaphoreSize < 1 {
		semaphoreSize = 1
	}
	tokenPool := semaphore.NewWeighted(semaphoreSize)

	// Start Processing
	start := time.Now()
	if !noDryRun {
		fmt.Println("Running in dry run mode. Use -apply to actually dedup files.")
		fmt.Println()
	}
	pathATree, pathBTree, nbAFiles := index(cleanPathA, cleanPathB, tokenPool)
	fmt.Println()
	dedup(pathATree, pathBTree, nbAFiles, tokenPool)
	fmt.Println()
	fmt.Printf("Done in %v (max workers: %d)\n", (time.Since(start)/time.Second)*time.Second, semaphoreSize)
}
