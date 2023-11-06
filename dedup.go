package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/gosuri/uiprogress"
	"github.com/hekmon/cunits/v2"
	"golang.org/x/sync/semaphore"
)

const (
	readSize = 64 * 1024 * 1024
)

func dedup(pathATree, pathBTree *TreeStat, tokenPool *semaphore.Weighted, totalFiles int64) (errorsList []error) {
	// Prepare to launch goroutines
	var workers sync.WaitGroup
	errChan := make(chan error)
	// error logger
	errorsDone := make(chan any)
	go func() {
		for err := range errChan {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			errorsList = append(errorsList, err)
		}
		close(errorsDone)
	}()
	// Progress
	progress := uiprogress.New()
	globalProgress := progress.AddBar(int(totalFiles)).AppendCompleted().PrependElapsed()
	globalProgress.AppendFunc(func(b *uiprogress.Bar) string {
		return fmt.Sprintf("Files scanned: %d/%d", b.Current(), totalFiles)
	})
	progress.Start()
	defer progress.Stop()
	// start dedup search
	for _, child := range pathATree.Children {
		// can we launch it concurrently ?
		if tokenPool.TryAcquire(1) {
			workers.Add(1)
			go func(localChild *TreeStat) {
				dedupFile(localChild, pathBTree, tokenPool, &workers, progress, globalProgress.Incr, errChan)
				tokenPool.Release(1)
				workers.Done()
			}(child)
		} else {
			dedupFile(child, pathBTree, tokenPool, &workers, progress, globalProgress.Incr, errChan)
		}
	}
	// wait for all workers to finish
	workers.Wait()
	// Stop utilities goroutines
	<-errorsDone // wait for all errors to be compiled before return
	return
}

func dedupFile(refFile, pathBTree *TreeStat, tokenPool *semaphore.Weighted, waitGroup *sync.WaitGroup, progress *uiprogress.Progress, processed func() bool, errChan chan<- error) {
	if refFile.Infos.Mode().IsDir() {
		// continue to children
		for _, child := range refFile.Children {
			// can we launch it concurrently ?
			if tokenPool.TryAcquire(1) {
				waitGroup.Add(1)
				go func(localChild *TreeStat) {
					dedupFile(localChild, pathBTree, tokenPool, waitGroup, progress, processed, errChan)
					tokenPool.Release(1)
					waitGroup.Done()
				}(child)
			} else {
				// process within the same goroutine
				dedupFile(child, pathBTree, tokenPool, waitGroup, progress, processed, errChan)
			}
		}
		return
	}
	// else regular file
	defer processed()
	// do not process empty files
	if refFile.Infos.Size() == 0 {
		return
	}
	// try to find candidates
	candidates := findFileWithSize(refFile.Infos.Size(), pathBTree)
	if len(candidates) == 0 {
		return
	}
	// Remove candidates that are already hardlink to reffile
	//// TODO
	if len(candidates) == 0 {
		return
	}
	// candidates ready, create the progress bar
	totalSize := cunits.ImportInByte(float64(refFile.Infos.Size() * int64(len(candidates)+1)))
	fileBar := progress.AddBar(int(totalSize)).AppendCompleted().PrependElapsed()
	fileBar.AppendFunc(func(b *uiprogress.Bar) string {
		return fmt.Sprintf("%s and %d candidate(s) (total to hash: %s)", refFile.Infos.Name(), len(candidates), totalSize)
	})
	var (
		totalWritten       int
		fileProgressAccess sync.Mutex
	)
	updateProgress := func(add int) {
		fileProgressAccess.Lock()
		totalWritten += add
		_ = fileBar.Set(totalWritten)
		fileProgressAccess.Unlock()
	}
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
			if originalHash, err = computeHash(refFile.FullPath, updateProgress); err != nil {
				errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", refFile.FullPath, err)
				return
			}
			tokenPool.Release(1)
			localWaitGroup.Done()
			waitGroup.Done()
		}()
	} else {
		// process within the same goroutine
		if originalHash, err = computeHash(refFile.FullPath, updateProgress); err != nil {
			errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", refFile.FullPath, err)
			return
		}
	}
	// compute checksums of the candidates and replace them if a match is found
	candidatesHashes := make([][]byte, len(candidates))
	for candidateIndex, candidate := range candidates {
		// Compute checksum
		if tokenPool.TryAcquire(1) {
			waitGroup.Add(1)
			localWaitGroup.Add(1)
			go func(localCandidate *TreeStat, target *[]byte) {
				if *target, err = computeHash(localCandidate.FullPath, updateProgress); err != nil {
					errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", localCandidate.FullPath, err)
					return
				}
				tokenPool.Release(1)
				localWaitGroup.Done()
				waitGroup.Done()
			}(candidate, &(candidatesHashes[candidateIndex]))
		} else {
			// process within the same goroutine
			if candidatesHashes[candidateIndex], err = computeHash(candidate.FullPath, updateProgress); err != nil {
				errChan <- fmt.Errorf("failed to compute hash of ref file %s: %w", candidate.FullPath, err)
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
		fmt.Fprintf(progress.Bypass(), "Match found for '%s': '%s' has the same checksum: replacing by a hard link\n",
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

func computeHash(path string, reportWritten func(add int)) (hash []byte, err error) {
	// Prepare
	hasher := sha256.New()
	buffer := make([]byte, readSize)
	fd, err := os.Open(path)
	if err != nil {
		return
	}
	defer fd.Close()
	// Feed the hasher and report progress
	var read, written int
	continueReading := true
	for continueReading {
		if read, err = fd.Read(buffer); err != nil {
			if !errors.Is(err, io.EOF) {
				err = fmt.Errorf("failed to read from '%s': %w", path, err)
				return
			}
			continueReading = false
			err = nil
		}
		if written, err = hasher.Write(buffer[:read]); err != nil {
			err = fmt.Errorf("failed to write to hasher from file '%s': %w", path, err)
			return
		}
		go reportWritten(written)
	}
	hash = hasher.Sum(nil)
	return
}
