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

func dedup(pathATree, pathBTree *TreeStat, tokenPool *semaphore.Weighted, totalFiles uint64) {
	// Prepare to launch goroutines
	var workers sync.WaitGroup
	errChan := make(chan error)
	defer close(errChan)
	processed := new(atomic.Uint64)
	// error logger
	go func() {
		for err := range errChan {
			fmt.Fprintf(os.Stderr, "%s\n", err)
		}
	}()
	// stats logger
	loggerCtx, loggerCtxCancel := context.WithCancel(context.Background())
	loggerDone := make(chan any)
	go func() {
		for loggerCtx.Err() == nil {
			timer := time.NewTimer(time.Second)
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
			fmt.Printf("[%0.2f%%] %d/%d files processed so far\n",
				float64(localProcessed)/float64(totalFiles)*100, localProcessed, totalFiles)
		}
		localProcessed := processed.Add(0)
		fmt.Printf("[%0.2f%%] %d/%d files processed so far\n",
			float64(localProcessed)/float64(totalFiles)*100, localProcessed, totalFiles)
		close(loggerDone)
	}()
	defer loggerCtxCancel() // in case we do not reach the end of the fx
	// start dedup search
	for _, child := range pathATree.Children {
		// can we launch it concurrently ?
		if tokenPool.TryAcquire(1) {
			workers.Add(1)
			go func(localChild *TreeStat) {
				dedupFile(localChild, pathBTree, tokenPool, &workers, processed, errChan)
				tokenPool.Release(1)
				workers.Done()
			}(child)
		} else {
			dedupFile(child, pathBTree, tokenPool, &workers, processed, errChan)
		}
	}
	// wait for all workers to finish
	workers.Wait()
	// Stop utilities goroutines
	loggerCtxCancel()
	<-loggerDone // wait for final stats
}

func dedupFile(refFile, pathBTree *TreeStat, tokenPool *semaphore.Weighted, waitGroup *sync.WaitGroup, processed *atomic.Uint64, errChan chan<- error) {
	if refFile.Infos.Mode().IsDir() {
		// continue to children
		for _, child := range refFile.Children {
			// can we launch it concurrently ?
			if tokenPool.TryAcquire(1) {
				waitGroup.Add(1)
				go func(localChild *TreeStat) {
					dedupFile(localChild, pathBTree, tokenPool, waitGroup, processed, errChan)
					tokenPool.Release(1)
					waitGroup.Done()
				}(child)
			} else {
				// process within the same goroutine
				dedupFile(child, pathBTree, tokenPool, waitGroup, processed, errChan)
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
			if originalHash, err = computeHash(refFile.FullPath); err != nil {
				errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", refFile.FullPath, err)
				return
			}
			tokenPool.Release(1)
			localWaitGroup.Done()
			waitGroup.Done()
		}()
	} else {
		// process within the same goroutine
		if originalHash, err = computeHash(refFile.FullPath); err != nil {
			errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", refFile.FullPath, err)
			return
		}
	}
	// compute checksums of the candidates and replace them if a match is found
	candidatesHashes := make([][]byte, len(candidates))
	for candidateIndex, candidate := range candidates {
		if tokenPool.TryAcquire(1) {
			waitGroup.Add(1)
			localWaitGroup.Add(1)
			go func(localCandidate *TreeStat, target *[]byte) {
				if *target, err = computeHash(localCandidate.FullPath); err != nil {
					errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", localCandidate.FullPath, err)
					return
				}
				tokenPool.Release(1)
				localWaitGroup.Done()
				waitGroup.Done()
			}(candidate, &(candidatesHashes[candidateIndex]))
		} else {
			// process within the same goroutine
			if candidatesHashes[candidateIndex], err = computeHash(candidate.FullPath); err != nil {
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

func computeHash(path string) (hash []byte, err error) {
	fmt.Printf("Computing hash of %s\n...", path)
	hasher := sha256.New()
	fd, err := os.Open(path)
	if err != nil {
		return
	}
	defer fd.Close()
	if _, err = io.Copy(hasher, fd); err != nil {
		return
	}
	hash = hasher.Sum(nil)
	fmt.Printf("Computed hash of %s: %x\n", path, hash)
	return
}
