package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/hekmon/cunits/v2"
	"golang.org/x/sync/semaphore"
)

const (
	reasonableMaxParallelIO = 6
)

var (
	apply             bool
	force             bool
	debug             bool
	minSize           cunits.Bits
	defaultMaxWorkers int
)

func init() {
	defaultMaxWorkers = runtime.NumCPU()
	if defaultMaxWorkers > reasonableMaxParallelIO {
		defaultMaxWorkers = reasonableMaxParallelIO
	}
}

func main() {
	var err error
	// Process launch flags
	dirA := flag.String("dirA", "", "Referential directory")
	dirB := flag.String("dirB", "", "Second directory to compare dirA against")
	workers := flag.Int("workers", defaultMaxWorkers, "Set the maximum numbers of workers that will perform IO tasks")
	minSizeStr := flag.String("minSize", "", "Set the minimum size a file must have to be kept for analysis (ex: 100MiB)")
	flag.BoolVar(&apply, "apply", false, "By default deduper run in dry run mode: set this flag to actually apply changes")
	flag.BoolVar(&force, "force", false, "Dedup files that have the same content even if their inode metadata (ownership and mode) are not the same")
	flag.BoolVar(&debug, "debug", false, "Show debug logs during the analysis phase")
	flag.Parse()
	if *minSizeStr != "" {
		if minSize, err = cunits.Parse(*minSizeStr); err != nil {
			fmt.Fprintf(os.Stderr, "failed to parse minSize: %s\n", err)
			os.Exit(1)
		}
	}

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
	if apply {
		fmt.Println("/!\\ WARNING: running in apply mode: files will be replaced by hardlinks if they match!")
	} else {
		fmt.Println("Running in dry run mode. Use -apply to actually dedup files.")
	}
	fmt.Println()
	pathATree, pathBTree, nbAFiles, errorCount := index(cleanPathA, cleanPathB, tokenPool)
	if errorCount > 0 {
		fmt.Printf("%d error(s) encountered during indexing, please check the logs", errorCount)
	}
	fmt.Println()
	if errorCount = dedup(pathATree, pathBTree, nbAFiles, tokenPool); errorCount > 0 {
		fmt.Printf("%d error(s) encountered during processing, please check the logs", errorCount)
	}
	fmt.Printf("Done in %v (max IO workers: %d)\n", (time.Since(start)/time.Millisecond)*time.Millisecond, semaphoreSize)
}
