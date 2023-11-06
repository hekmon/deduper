package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

func dedup(pathATree, pathBTree *TreeStat, tokenPool *semaphore.Weighted, totalFiles uint64) (errorsList []error) {
	// Prepare to launch goroutines
	var workers sync.WaitGroup
	errChan := make(chan error)
	processed := new(atomic.Uint64)
	hashInProgress := new(atomic.Int64)
	// error logger
	errorsDone := make(chan any)
	go func() {
		for err := range errChan {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			errorsList = append(errorsList, err)
		}
		close(errorsDone)
	}()
	// stats logger
	loggerCtx, loggerCtxCancel := context.WithCancel(context.Background())
	defer loggerCtxCancel() // in case we do not reach the end of the fx
	loggerDone := make(chan any)
	go func() {
		for loggerCtx.Err() == nil {
			timer := time.NewTimer(5 * time.Second)
			select {
			case <-loggerCtx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				break
			case <-timer.C:
				// proceed
			}
			localProcessed := processed.Add(0)
			fmt.Printf("[%0.2f%%] %d/%d files processed so far (%d hashing in progress)\n",
				float64(localProcessed)/float64(totalFiles)*100, localProcessed, totalFiles, hashInProgress.Add(0))
		}
		localProcessed := processed.Add(0)
		fmt.Printf("[%0.2f%%] %d files processed\n",
			float64(localProcessed)/float64(totalFiles)*100, localProcessed)
		close(loggerDone)
	}()
	// start dedup search
	for _, child := range pathATree.Children {
		// can we launch it concurrently ?
		if tokenPool.TryAcquire(1) {
			workers.Add(1)
			go func(localChild *TreeStat) {
				dedupFile(localChild, pathBTree, tokenPool, &workers, processed, hashInProgress, errChan)
				tokenPool.Release(1)
				workers.Done()
			}(child)
		} else {
			dedupFile(child, pathBTree, tokenPool, &workers, processed, hashInProgress, errChan)
		}
	}
	// wait for all workers to finish
	workers.Wait()
	// Stop utilities goroutines
	loggerCtxCancel()
	<-loggerDone // wait for final stats
	<-errorsDone // wait for all errors to be compiled before return
	return
}

func dedupFile(refFile, pathBTree *TreeStat, tokenPool *semaphore.Weighted, waitGroup *sync.WaitGroup, processed *atomic.Uint64, hashInProgress *atomic.Int64, errChan chan<- error) {
	if refFile.Infos.Mode().IsDir() {
		// continue to children
		for _, child := range refFile.Children {
			// can we launch it concurrently ?
			if tokenPool.TryAcquire(1) {
				waitGroup.Add(1)
				go func(localChild *TreeStat) {
					dedupFile(localChild, pathBTree, tokenPool, waitGroup, processed, hashInProgress, errChan)
					tokenPool.Release(1)
					waitGroup.Done()
				}(child)
			} else {
				// process within the same goroutine
				dedupFile(child, pathBTree, tokenPool, waitGroup, processed, hashInProgress, errChan)
			}
		}
		return
	}
	// else regular file, try to find a match !
	defer processed.Add(1)
	candidates := findFileWithSize(refFile.Infos.Size(), pathBTree)
	if len(candidates) == 0 {
		return
	}
	fmt.Printf("Found %d candidates for %s, computing checksums...\n", len(candidates), refFile.FullPath)
	// compute checksum of the ref file
	var (
		originalHash   []byte
		localWaitGroup sync.WaitGroup
		err            error
	)
	//// can we launch it concurrently ?
	if tokenPool.TryAcquire(1) {
		waitGroup.Add(1)
		localWaitGroup.Add(1)
		go func() {
			if originalHash, err = computeHash(refFile.FullPath, hashInProgress); err != nil {
				errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", refFile.FullPath, err)
				return
			}
			tokenPool.Release(1)
			localWaitGroup.Done()
			waitGroup.Done()
		}()
	} else {
		// process within the same goroutine
		if originalHash, err = computeHash(refFile.FullPath, hashInProgress); err != nil {
			errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", refFile.FullPath, err)
			return
		}
	}
	// compute checksums of the candidates and replace them if a match is found
	candidatesHashes := make([][]byte, len(candidates))
	for candidateIndex, candidate := range candidates {
		// Check if not already a hard link
		//// TODO
		// Compute checksum
		if tokenPool.TryAcquire(1) {
			waitGroup.Add(1)
			localWaitGroup.Add(1)
			go func(localCandidate *TreeStat, target *[]byte) {
				if *target, err = computeHash(localCandidate.FullPath, hashInProgress); err != nil {
					errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", localCandidate.FullPath, err)
					return
				}
				tokenPool.Release(1)
				localWaitGroup.Done()
				waitGroup.Done()
			}(candidate, &(candidatesHashes[candidateIndex]))
		} else {
			// process within the same goroutine
			if candidatesHashes[candidateIndex], err = computeHash(candidate.FullPath, hashInProgress); err != nil {
				errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", candidate.FullPath, err)
				return
			}
		}
	}
	localWaitGroup.Wait()
	// time to compare
	for candidateIndex, candidateHash := range candidatesHashes {
		if !bytes.Equal(originalHash, candidateHash) {
			continue
		}
		// Same file found !
		fmt.Printf("Match found for %s: %s has the same checksum: replacing by a hard link\n",
			refFile.FullPath, candidates[candidateIndex].FullPath)
		/// TODO
	}
}

func findFileWithSize(refSize int64, node *TreeStat) (candidates []*TreeStat) {
	if node.Infos.Mode().IsDir() {
		for _, child := range node.Children {
			candidates = append(candidates, findFileWithSize(refSize, child)...)
		}
		return
	}
	// else regular file
	if node.Infos.Size() == refSize {
		candidates = append(candidates, node)
	}
	return
}

func computeHash(path string, hashInProgress *atomic.Int64) (hash []byte, err error) {
	fmt.Printf("Computing hash of %s...\n", path)
	hasher := sha256.New()
	fd, err := os.Open(path)
	if err != nil {
		return
	}
	defer fd.Close()
	hashInProgress.Add(1)
	defer hashInProgress.Add(-1)
	if _, err = io.Copy(hasher, fd); err != nil {
		return
	}
	hash = hasher.Sum(nil)
	fmt.Printf("Computed hash of %s: %x\n", path, hash)
	return
}
