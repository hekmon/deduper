package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gosuri/uiprogress"
	"github.com/hekmon/cunits/v2"
	"golang.org/x/sync/semaphore"
)

func dedup(pathATree, pathBTree *TreeStat, tokenPool *semaphore.Weighted, totalFiles int64) (errorsList []error) {
	// Prepare to launch goroutines
	var workers sync.WaitGroup
	// Progress
	progress := uiprogress.New()
	progress.RefreshInterval = time.Second
	progress.Width = 30
	globalProgress := progress.AddBar(int(totalFiles)).AppendCompleted()
	globalProgress.Empty = ' '
	globalProgress.AppendFunc(func(b *uiprogress.Bar) string {
		return fmt.Sprintf("Global progress: %d/%d files processed", b.Current(), totalFiles)
	})
	progress.Start()
	defer progress.Stop()
	// error logger
	errChan := make(chan error)
	errorsDone := make(chan any)
	go func() {
		for err := range errChan {
			fmt.Fprintf(progress.Bypass(), "ERROR: %s\n", err)
			errorsList = append(errorsList, err)
		}
		close(errorsDone)
	}()
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
	// wait for all dedup workers to finish
	workers.Wait()
	// Stop utilities goroutines
	close(errChan)
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
	// else process regular file
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
	// candidates ready
	fullPaths := make([]string, len(candidates))
	for index, candidate := range candidates {
		fullPaths[index] = candidate.FullPath
	}
	fmt.Fprintf(progress.Bypass(), "File '%s' has %d candidate(s) for dedup/hardlinking: '%s'\n", refFile.FullPath, len(candidates), strings.Join(fullPaths, "', '"))
	// create the progress bar for this file
	totalSize := cunits.ImportInByte(float64(refFile.Infos.Size() * int64(len(candidates)+1)))
	fileBar := progress.AddBar(int(totalSize.Byte())).AppendCompleted()
	fileBar.Empty = ' '
	fileBar.AppendFunc(func(b *uiprogress.Bar) string {
		return fmt.Sprintf("%s + %d candidate(s) (hashing: %s/%s)",
			refFile.Infos.Name(), len(candidates), cunits.ImportInByte(float64(b.Current())), totalSize)
	})
	var totalWritten atomic.Int64
	updateProgress := func(add int) {
		if err := fileBar.Set(int(totalWritten.Add(int64(add)))); err != nil {
			fmt.Fprintf(progress.Bypass(), "ERROR: failed to set progress bar for '%s': %s", refFile.Infos.Name(), err)
		}
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
			fmt.Fprintf(progress.Bypass(), "Hash computed for '%s': %X\n", refFile.FullPath, originalHash)
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
		fmt.Fprintf(progress.Bypass(), "Hash computed for '%s': %X\n", refFile.FullPath, originalHash)
	}
	// compute checksums of the candidates and replace them if a match is found
	candidatesHashes := make([][]byte, len(candidates))
	for candidateIndex, candidate := range candidates {
		// can we launch it concurrently ?
		if tokenPool.TryAcquire(1) {
			waitGroup.Add(1)
			localWaitGroup.Add(1)
			go func(localCandidate *TreeStat, target *[]byte) {
				if *target, err = computeHash(localCandidate.FullPath, updateProgress); err != nil {
					errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", localCandidate.FullPath, err)
					return
				}
				fmt.Fprintf(progress.Bypass(), "Hash computed for '%s': %X\n", localCandidate.FullPath, *target)
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
			fmt.Fprintf(progress.Bypass(), "Hash computed for '%s': %x\n", candidate.FullPath, candidatesHashes[candidateIndex])
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
	progressWriter := writterProgress{
		writter: hasher,
		report:  reportWritten,
	}
	fd, err := os.Open(path)
	if err != nil {
		return
	}
	defer fd.Close()
	// Feed the hasher
	if _, err = io.Copy(progressWriter, fd); err != nil {
		return
	}
	hash = hasher.Sum(nil)
	return
}

type writterProgress struct {
	writter io.Writer
	report  func(add int)
}

func (wp writterProgress) Write(p []byte) (n int, err error) {
	n, err = wp.writter.Write(p)
	if err == nil || errors.Is(err, io.EOF) {
		wp.report(n)
	}
	return
}
