package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gosuri/uiprogress"
	"github.com/hekmon/cunits/v2"
	"golang.org/x/sync/semaphore"
)

type dedupedReport struct {
	Reference *TreeStat
	Files     []*TreeStat
}

func dedup(pathATree, pathBTree *TreeStat, totalAFiles int64, tokenPool *semaphore.Weighted) (errorCount int) {
	// Setup global progress bar
	progress := uiprogress.New()
	progress.RefreshInterval = time.Second
	progress.Width = 30
	globalProgress := progress.AddBar(int(totalAFiles)).AppendCompleted()
	globalProgress.Empty = ' '
	globalProgress.AppendFunc(func(b *uiprogress.Bar) string {
		return fmt.Sprintf("Global progress: %d/%d files processed", b.Current(), totalAFiles)
	})
	progress.Start()
	// error logger
	errChan := make(chan error)
	errorsDone := make(chan any)
	go func() {
		for err := range errChan {
			fmt.Fprintf(progress.Bypass(), "ERROR: %s\n", err)
			errorCount++
		}
		close(errorsDone)
	}()
	// report logger
	dedupedChan := make(chan dedupedReport)
	dedupedDone := make(chan any)
	dedupedList := make(map[*TreeStat][]*TreeStat)
	go func() {
		for deduped := range dedupedChan {
			dedupedList[deduped.Reference] = deduped.Files
		}
		close(dedupedDone)
	}()
	// start nodes processing from the top of the tree
	var workers sync.WaitGroup
	processNode(pathATree, pathBTree, concurrentToolBox{tokenPool, &workers, progress, globalProgress.Incr, dedupedChan, errChan})
	workers.Wait()
	// Stop utilities goroutines
	close(dedupedChan)
	close(errChan)
	<-dedupedDone
	<-errorsDone
	// Print results log
	progress.Stop()
	var (
		totalSaved cunits.Bits
		totalFiles int
	)
	fmt.Println()
	fmt.Println("Dedup listing:")
	for refFile, matchsDeduped := range dedupedList {
		fileSize := cunits.ImportInByte(float64(refFile.Infos.Size()))
		saved := fileSize * cunits.Bits(len(matchsDeduped))
		if noDryRun {
			fmt.Printf("File '%s' (%s) has %d match(s) (saved %s):\n",
				refFile.FullPath, fileSize, len(matchsDeduped), saved)
		} else {
			fmt.Printf("File '%s' (%s) has %d match(s) (potential saving of %s):\n",
				refFile.FullPath, fileSize, len(matchsDeduped), saved)
		}
		for _, matchDeduped := range matchsDeduped {
			if noDryRun {
				fmt.Printf("\tMatch '%s' has been replaced by a hardlink\n", matchDeduped.FullPath)
			} else {
				fmt.Printf("\tMatch '%s' could have been replaced by a hardlink\n", matchDeduped.FullPath)
			}
		}
		totalSaved += saved
		totalFiles += len(matchsDeduped)
	}
	fmt.Println()
	if noDryRun {
		fmt.Printf("%d file(s) deduped with hard linking saving a total of %s\n", totalFiles, totalSaved)
	} else {
		fmt.Printf("%d file(s) could be deduped with hard links to save a total of %s\n", totalFiles, totalSaved)
	}
	return
}

type concurrentToolBox struct {
	tokenPool          *semaphore.Weighted
	waitGroup          *sync.WaitGroup
	progress           *uiprogress.Progress
	processedReporting func() bool
	dedupedChan        chan<- dedupedReport
	errChan            chan<- error
}

func processNode(refFile, pathBTree *TreeStat, concurrent concurrentToolBox) {
	if refFile.Infos.Mode().IsDir() {
		for _, child := range refFile.Children {
			processNode(child, pathBTree, concurrent)
		}
	} else {
		processFileFindCandidates(refFile, pathBTree, concurrent)
	}
}

func processFileFindCandidates(refFile, pathBTree *TreeStat, concurrent concurrentToolBox) {
	// If we do not make it to candidates evaluation, report the file as processed
	processed := true
	defer func() {
		if processed {
			concurrent.processedReporting()
		}
	}()
	// do not process empty files
	if refFile.Infos.Size() == 0 {
		return
	}
	// try to find candidates by size
	sizeCandidates := findFileWithSize(refFile.Infos.Size(), pathBTree)
	if len(sizeCandidates) == 0 {
		return
	}
	// Remove candidates that are already hardlink to reffile
	refFileSystem, ok := refFile.Infos.Sys().(*syscall.Stat_t)
	if !ok {
		concurrent.errChan <- fmt.Errorf("can not check for hardlinks for '%s': backing storage device can not be checked", refFile.FullPath)
		return
	}
	var finalCandidates []*TreeStat
	if refFileSystem.Nlink > 1 {
		finalCandidates = make([]*TreeStat, 0, len(sizeCandidates))
		// files has hard links, checking against candidates
		for _, candidate := range sizeCandidates {
			candidateSystem, ok := candidate.Infos.Sys().(*syscall.Stat_t)
			if !ok {
				concurrent.errChan <- fmt.Errorf("can not check for hardlinks for '%s': backing storage device can not be checked", candidate.FullPath)
				continue
			}
			// check if inode is the same
			if candidateSystem.Ino != refFileSystem.Ino {
				finalCandidates = append(finalCandidates, candidate)
			} else {
				fmt.Fprintf(concurrent.progress.Bypass(), "File '%s' is already a hardlink of '%s': skipping\n",
					candidate.FullPath, refFile.FullPath)
			}
		}
	} else {
		finalCandidates = sizeCandidates
	}
	if len(finalCandidates) == 0 {
		return
	}
	// final candidates ready, fire a log
	fullPaths := make([]string, len(finalCandidates))
	for index, candidate := range finalCandidates {
		fullPaths[index] = candidate.FullPath
	}
	fmt.Fprintf(concurrent.progress.Bypass(), "File '%s' has %d candidate(s) for dedup/hardlinking: '%s'\n",
		refFile.FullPath, len(finalCandidates), strings.Join(fullPaths, "', '"))
	// Start a goroutine as handler for this file (no token used as this goroutine will not produce IO itself but will launch others goroutines that will)
	concurrent.waitGroup.Add(1)
	go func() {
		processFileEvaluateCandidates(refFile, finalCandidates, concurrent)
		concurrent.waitGroup.Done()
	}()
	// do not report this file as processed (yet) when exiting this fx as candidates must be evaluated: processFileEvaluateCandidates() will mark this file as processed when done
	processed = false
}

func findFileWithSize(refSize int64, node *TreeStat) (candidates []*TreeStat) {
	if node.Infos.Mode().IsDir() {
		for _, child := range node.Children {
			candidates = append(candidates, findFileWithSize(refSize, child)...)
		}
	} else {
		if node.Infos.Size() == refSize {
			candidates = append(candidates, node)
		}
	}
	return
}

func processFileEvaluateCandidates(refFile *TreeStat, candidates []*TreeStat, concurrent concurrentToolBox) {
	// Mark the file as processed when done
	defer concurrent.processedReporting()
	// create the progress bar for this particular file
	endStatus := ""
	totalSize := cunits.ImportInByte((float64(refFile.Infos.Size() * int64(len(candidates)+1))) + 1) // add one last fake byte to trigger print of end message
	fileBar := concurrent.progress.AddBar(int(totalSize.Byte())).AppendCompleted()
	fileBar.Empty = ' '
	fileBar.AppendFunc(func(b *uiprogress.Bar) string {
		if endStatus != "" {
			return endStatus
		}
		return fmt.Sprintf("%s + %d candidate(s) (total hashing: %s/%s)",
			refFile.Infos.Name(), len(candidates), cunits.ImportInByte(float64(b.Current())), totalSize)
	})
	var totalWritten atomic.Uint64
	updateProgress := func(add int) {
		if err := fileBar.Set(int(totalWritten.Add(uint64(add)))); err != nil {
			fmt.Fprintf(concurrent.progress.Bypass(), "ERROR: failed to set progress bar for '%s': %s", refFile.Infos.Name(), err)
		}
	}
	// prepare to compute checksums for reFile and its candidates
	var (
		originalHash            []byte
		fileProcessingWaitGroup sync.WaitGroup
		err                     error
	)
	candidatesHashes := make([][]byte, len(candidates))
	// launch reffile hash in a weighted goroutine
	_ = concurrent.tokenPool.Acquire(context.Background(), 1) // no err check as error can only come from expired context
	fileProcessingWaitGroup.Add(1)
	go func() {
		defer concurrent.tokenPool.Release(1)
		defer fileProcessingWaitGroup.Done()
		if originalHash, err = computeHash(refFile.FullPath, updateProgress); err != nil {
			concurrent.errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", refFile.FullPath, err)
			return
		}
		fmt.Fprintf(concurrent.progress.Bypass(), "SHA256 computed for '%s': %x\n", refFile.FullPath, originalHash)
	}()
	// compute checksums of the candidates in weighted goroutines too
	for candidateIndex, candidate := range candidates {
		_ = concurrent.tokenPool.Acquire(context.Background(), 1) // no err check as error can only come from expired context
		fileProcessingWaitGroup.Add(1)
		go func(localCandidate *TreeStat, target *[]byte) {
			defer concurrent.tokenPool.Release(1)
			defer fileProcessingWaitGroup.Done()
			if *target, err = computeHash(localCandidate.FullPath, updateProgress); err != nil {
				concurrent.errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", localCandidate.FullPath, err)
				return
			}
			fmt.Fprintf(concurrent.progress.Bypass(), "SHA256 computed for '%s': %x\n", localCandidate.FullPath, *target)
		}(candidate, &(candidatesHashes[candidateIndex]))
	}
	// wait for hashing goroutines to finish
	fileProcessingWaitGroup.Wait()
	// time to compare all of them
	deduped := make([]*TreeStat, 0, len(candidates))
	for candidateIndex, candidateHash := range candidatesHashes {
		if !bytes.Equal(originalHash, candidateHash) {
			continue
		}
		// Same file found !
		if noDryRun {
			fmt.Fprintf(concurrent.progress.Bypass(), "Match found for '%s': '%s' has the same checksum: replacing by a hard link\n",
				refFile.FullPath, candidates[candidateIndex].FullPath)
			if err = os.Remove(candidates[candidateIndex].FullPath); err != nil {
				concurrent.errChan <- fmt.Errorf("can not make hardlink against '%s': failed to remove '%s': %s", refFile.FullPath, candidates[candidateIndex].FullPath, err)
				continue
			}
			if err = os.Link(refFile.FullPath, candidates[candidateIndex].FullPath); err != nil {
				concurrent.errChan <- fmt.Errorf("can not make hardlink between '%s' <> '%s' (file already removed!): %s", refFile.FullPath, candidates[candidateIndex].FullPath, err)
				continue
			}
			deduped = append(deduped, candidates[candidateIndex])
		} else {
			fmt.Fprintf(concurrent.progress.Bypass(), "Match found for '%s': '%s' has the same checksum: it could be replaced by a hard link\n",
				refFile.FullPath, candidates[candidateIndex].FullPath)
			deduped = append(deduped, candidates[candidateIndex])
		}
	}
	// Done, show end message
	if noDryRun {
		endStatus = fmt.Sprintf("%s: %d/%d candidates hardlinked (saved %s)",
			refFile.Infos.Name(), len(deduped), len(candidates), cunits.ImportInByte(float64(refFile.Infos.Size()*int64(len(deduped)))))
	} else {
		endStatus = fmt.Sprintf("%s: %d/%d candidates could be hardlinked (potential saving of %s)",
			refFile.Infos.Name(), len(deduped), len(candidates), cunits.ImportInByte(float64(refFile.Infos.Size()*int64(len(deduped)))))
	}
	fileBar.Incr() // add the last fake byte to trigger one last update of the append msg
	// send report
	concurrent.dedupedChan <- dedupedReport{
		Reference: refFile,
		Files:     deduped,
	}
}

func computeHash(path string, reportWritten func(add int)) (hash []byte, err error) {
	// Prepare the hasher
	hasher := sha256.New()
	hasherProgressReporter := writterProgress{
		writter: hasher,
		report:  reportWritten,
	}
	// Open file for reading
	fd, err := os.Open(path)
	if err != nil {
		return
	}
	defer fd.Close()
	// Feed it to the hasher
	if _, err = io.Copy(hasherProgressReporter, fd); err != nil {
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
