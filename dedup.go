package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/hekmon/cunits/v2"
	"github.com/hekmon/liveprogress"
	"golang.org/x/sync/semaphore"
)

type dedupedReport struct {
	Reference  *FileInfos
	HardLinked FileInfosList
}

func dedup(pathATree, pathBTree *FileInfos, totalAFiles int64, tokenPool *semaphore.Weighted) (errorCount int) {
	// Setup global progress bar
	// liveprogress.RefreshInterval = time.Second
	barConfig := liveprogress.DefaultConfig
	// barConfig.Width = 30
	barConfig.Empty = ' '
	globalProgress := liveprogress.AddBar(uint64(totalAFiles), liveprogress.DefaultConfig,
		liveprogress.AppendPercent(),
		liveprogress.AppendDecorator(func(b *liveprogress.Bar) string {
			return fmt.Sprintf("Global progress: %d/%d files processed", b.Current(), totalAFiles)
		}),
	)
	var (
		liveprogressErr error
		stdout, stderr  io.Writer
	)
	if liveprogressErr = liveprogress.Start(); liveprogressErr != nil {
		fmt.Fprintf(os.Stderr, "failed to start liveprogress: %s\n", liveprogressErr)
		stdout = os.Stdout
		stderr = os.Stderr
	} else {
		stdout = liveprogress.Bypass()
		stderr = stdout
	}
	// error logger
	errChan := make(chan error)
	errorsDone := make(chan any)
	go func() {
		for err := range errChan {
			fmt.Fprintf(stderr, "ERROR: %s\n", err)
			errorCount++
		}
		close(errorsDone)
	}()
	// report logger
	dedupedChan := make(chan dedupedReport)
	dedupedDone := make(chan any)
	dedupedList := make(map[*FileInfos]FileInfosList)
	go func() {
		for deduped := range dedupedChan {
			dedupedList[deduped.Reference] = deduped.HardLinked
		}
		close(dedupedDone)
	}()
	// start nodes processing from the top of the tree
	var workers sync.WaitGroup
	processNode(
		pathATree,
		pathBTree,
		concurrentToolBox{
			tokenPool:          tokenPool,
			waitGroup:          &workers,
			stdout:             stdout,
			barConfig:          barConfig,
			processedReporting: globalProgress.CurrentIncrement,
			dedupedChan:        dedupedChan,
			errChan:            errChan,
		},
	)
	workers.Wait()
	// Stop utilities goroutines
	close(dedupedChan)
	close(errChan)
	<-dedupedDone
	<-errorsDone
	// Print results log
	if liveprogressErr == nil {
		liveprogress.Stop(true)
	}
	var (
		totalSaved cunits.Bits
		totalFiles int
	)
	if len(dedupedList) > 0 {
		fmt.Println()
		fmt.Println("Dedup listing:")
		for refFile, matchsDeduped := range dedupedList {
			fileSize := cunits.ImportInByte(float64(refFile.Infos.Size()))
			saved := fileSize * cunits.Bits(len(matchsDeduped))
			if apply {
				fmt.Printf("File '%s' (%s) has %d match(s) (saved %s):\n",
					refFile.FullPath, fileSize, len(matchsDeduped), saved)
			} else {
				fmt.Printf("File '%s' (%s) has %d match(s) (potential saving of %s):\n",
					refFile.FullPath, fileSize, len(matchsDeduped), saved)
			}
			for _, matchDeduped := range matchsDeduped {
				if apply {
					fmt.Printf("\tMatch '%s' has been replaced by a hardlink\n", matchDeduped.FullPath)
				} else {
					fmt.Printf("\tMatch '%s' could have been replaced by a hardlink\n", matchDeduped.FullPath)
				}
			}
			totalSaved += saved
			totalFiles += len(matchsDeduped)
		}
	}
	fmt.Println()
	if apply {
		fmt.Printf("%d file(s) deduped with hard linking saving a total of %s\n", totalFiles, totalSaved)
	} else {
		fmt.Printf("%d file(s) could be deduped with hard linking to save a total of %s\n", totalFiles, totalSaved)
	}
	return
}

type concurrentToolBox struct {
	tokenPool          *semaphore.Weighted
	waitGroup          *sync.WaitGroup
	stdout             io.Writer
	barConfig          liveprogress.BarConfig
	processedReporting func()
	dedupedChan        chan<- dedupedReport
	errChan            chan<- error
}

func processNode(refFile, pathBTree *FileInfos, concurrent concurrentToolBox) {
	if refFile.Infos.Mode().IsDir() {
		for _, child := range refFile.Children {
			processNode(child, pathBTree, concurrent)
		}
	} else {
		processFileFindCandidates(refFile, pathBTree, concurrent)
	}
}

func processFileFindCandidates(refFile, pathBTree *FileInfos, concurrent concurrentToolBox) {
	// if all IO workers are busy, slow down a little bit
	_ = concurrent.tokenPool.Acquire(context.Background(), 1)
	concurrent.tokenPool.Release(1)
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
	// if min size set, enforce it
	if minSize != 0 {
		fileSize := cunits.ImportInByte(float64(refFile.Infos.Size()))
		if fileSize < minSize {
			if debug {
				fmt.Fprintf(concurrent.stdout, "Reference file '%s' size (%s) is lower than minSize (%s): skipping\n",
					refFile.FullPath, fileSize, minSize)
			}
			return
		}
	}
	// try to find candidates by size
	sizeCandidates := findFileWithSize(refFile.Infos.Size(), pathBTree)
	if len(sizeCandidates) == 0 {
		return
	}
	if debug {
		fmt.Fprintf(concurrent.stdout, "Reference file '%s' has %d size candidate(s): %s\n",
			refFile.FullPath, len(sizeCandidates), sizeCandidates)
	}
	// start several inodes checks to discard unfitted candidates
	refFileSystem, ok := refFile.Infos.Sys().(*syscall.Stat_t)
	if !ok {
		concurrent.errChan <- fmt.Errorf("can not check for hardlinks for '%s': backing storage device can not be checked", refFile.FullPath)
		return
	}
	finalCandidates := make(FileInfosList, 0, len(sizeCandidates))
	for _, candidate := range sizeCandidates {
		candidateSystem, ok := candidate.Infos.Sys().(*syscall.Stat_t)
		if !ok {
			concurrent.errChan <- fmt.Errorf("can not check for hardlinks for '%s': backing storage device can not be checked", candidate.FullPath)
			continue
		}
		// in case refFile already has hardlinks, check if candidate inode is the same as reffile inode
		if refFileSystem.Nlink > 1 && candidateSystem.Ino == refFileSystem.Ino {
			if debug {
				fmt.Fprintf(concurrent.stdout, "File '%s' is already a hardlink of '%s': skipping\n",
					candidate.FullPath, refFile.FullPath)
			}
			continue
		}
		// check if inode metadata is identical if necessary
		if candidateSystem.Uid != refFileSystem.Uid {
			if !force {
				if debug {
					fmt.Fprintf(concurrent.stdout, "File '%s' UID (%d) is not the same as ref file '%s' UID (%d): skipping (activate force mode to retain it)\n",
						candidate.FullPath, candidateSystem.Uid, refFile.FullPath, refFileSystem.Uid)
				}
				continue
			}
			if debug {
				fmt.Fprintf(concurrent.stdout, "File '%s' UID (%d) is not the same as ref file '%s' UID (%d): keeping it anyway (force mode is on)\n",
					candidate.FullPath, candidateSystem.Uid, refFile.FullPath, refFileSystem.Uid)
			}
		}
		if candidateSystem.Gid != refFileSystem.Gid {
			if !force {
				if debug {
					fmt.Fprintf(concurrent.stdout, "File '%s' GID (%d) is not the same as ref file '%s' GID (%d): skipping (activate force mode to retain it)\n",
						candidate.FullPath, candidateSystem.Uid, refFile.FullPath, refFileSystem.Uid)
				}
				continue
			}
			if debug {
				fmt.Fprintf(concurrent.stdout, "File '%s' GID (%d) is not the same as ref file '%s' GID (%d): keeping it anyway (force mode is on)\n",
					candidate.FullPath, candidateSystem.Gid, refFile.FullPath, refFileSystem.Gid)
			}
		}
		if candidateSystem.Mode != refFileSystem.Mode {
			if !force {
				if debug {
					fmt.Fprintf(concurrent.stdout, "File '%s' mode (%o) is not the same as ref file '%s' mode (%o): skipping (activate force mode to retain it)\n",
						candidate.FullPath, candidateSystem.Mode, refFile.FullPath, refFileSystem.Mode)
				}
				continue
			}
			if debug {
				fmt.Fprintf(concurrent.stdout, "File '%s' mode (%o) is not the same as ref file '%s' mode (%o): keeping it anyway (force mode is on)\n",
					candidate.FullPath, candidateSystem.Mode, refFile.FullPath, refFileSystem.Mode)
			}
		}
		// candidate survived all the tests, keeping it to the finals
		finalCandidates = append(finalCandidates, candidate)
	}
	if len(finalCandidates) == 0 {
		return
	}
	// do not report this file as processed (yet) when exiting this fx as candidates must be evaluated:
	// processFileEvaluateCandidates() will mark this file as processed when done
	processed = false
	// final candidates ready
	if debug {
		fmt.Fprintf(concurrent.stdout, "File '%s' has %d final candidate(s) for dedup/hardlinking: %s\n",
			refFile.FullPath, len(finalCandidates), finalCandidates)
	}
	// Start a goroutine as handler for this file (no token used as this goroutine will not produce IO itself but will launch others goroutines that will)
	concurrent.waitGroup.Add(1)
	go func() {
		processFileEvaluateCandidates(refFile, finalCandidates, concurrent)
		concurrent.waitGroup.Done()
	}()
	// return to let the caller goroutine continue to walk the mem tree
}

func findFileWithSize(refSize int64, node *FileInfos) (candidates FileInfosList) {
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

func processFileEvaluateCandidates(refFile *FileInfos, candidates FileInfosList, concurrent concurrentToolBox) {
	// Mark the file as processed when done
	defer concurrent.processedReporting()
	// Do not create the progress bar (yet) if there is not at least one worker available for the original hash
	_ = concurrent.tokenPool.Acquire(context.Background(), 1) // no err check as error can only come from expired context
	// create the progress bar for this particular file
	endStatus := ""
	totalSize := cunits.ImportInByte((float64(refFile.Infos.Size() * int64(len(candidates)+1))))
	fileBar := liveprogress.AddBar(uint64(totalSize.Byte()), concurrent.barConfig,
		liveprogress.AppendPercent(),
		liveprogress.AppendDecorator(func(b *liveprogress.Bar) string {
			if endStatus != "" {
				return endStatus
			}
			return fmt.Sprintf("%s + %d candidate(s) (total hashing: %s/%s)",
				refFile.Infos.Name(), len(candidates), cunits.ImportInByte(float64(b.Current())), totalSize)
		}),
	)
	// prepare to compute checksums for reFile and its candidates
	var (
		originalHash            []byte
		candidatesHashes        [][]byte
		fileProcessingWaitGroup sync.WaitGroup
		err                     error
	)
	// launch reffile hash in a weighted goroutine (token already took before creating the progress bar)
	fileProcessingWaitGroup.Add(1)
	go func() {
		defer concurrent.tokenPool.Release(1)
		defer fileProcessingWaitGroup.Done()
		if originalHash, err = computeHash(refFile.FullPath, fileBar.CurrentAdd); err != nil {
			concurrent.errChan <- fmt.Errorf("failed to compute hash of ref File %s: %w", refFile.FullPath, err)
			return
		}
		if debug {
			fmt.Fprintf(concurrent.stdout, "SHA256 computed for '%s': %x\n", refFile.FullPath, originalHash)
		}
	}()
	// compute checksums of the candidates in weighted goroutines too
	candidatesHashes = make([][]byte, len(candidates))
	for candidateIndex, candidate := range candidates {
		_ = concurrent.tokenPool.Acquire(context.Background(), 1) // no err check as error can only come from expired context
		fileProcessingWaitGroup.Add(1)
		go func(localCandidate *FileInfos, target *[]byte) {
			defer concurrent.tokenPool.Release(1)
			defer fileProcessingWaitGroup.Done()
			if *target, err = computeHash(localCandidate.FullPath, fileBar.CurrentAdd); err != nil {
				concurrent.errChan <- fmt.Errorf("failed to compute hash of candidate File %s: %w", localCandidate.FullPath, err)
				return
			}
			if debug {
				fmt.Fprintf(concurrent.stdout, "SHA256 computed for '%s': %x\n", localCandidate.FullPath, *target)
			}
		}(candidate, &(candidatesHashes[candidateIndex]))
	}
	// wait for hashing goroutines to finish
	fileProcessingWaitGroup.Wait()
	// start dedup using the computed hashes
	deduped := processFileDedupCandidates(refFile, originalHash, candidates, candidatesHashes, concurrent)
	// Done, show end message
	if apply {
		endStatus = fmt.Sprintf("%s: %d/%d candidates hardlinked (saved %s)",
			refFile.Infos.Name(), len(deduped), len(candidates), cunits.ImportInByte(float64(refFile.Infos.Size()*int64(len(deduped)))))
	} else {
		endStatus = fmt.Sprintf("%s: %d/%d candidates could be hardlinked (potential saving of %s)",
			refFile.Infos.Name(), len(deduped), len(candidates), cunits.ImportInByte(float64(refFile.Infos.Size()*int64(len(deduped)))))
	}
	// send dedup report if any files deduped
	if len(deduped) > 0 {
		concurrent.dedupedChan <- dedupedReport{
			Reference:  refFile,
			HardLinked: deduped,
		}
	}
}

func computeHash(path string, reportWritten func(add uint64)) (hash []byte, err error) {
	// Prepare the hasher
	hashReporter := hasherProgress{
		hasher: sha256.New(),
		report: reportWritten,
	}
	// Open file for reading
	fd, err := os.Open(path)
	if err != nil {
		return
	}
	defer fd.Close()
	// Feed it to the hasher
	if _, err = io.Copy(hashReporter, fd); err != nil {
		return
	}
	hash = hashReporter.hasher.Sum(nil)
	return
}

type hasherProgress struct {
	hasher hash.Hash
	report func(add uint64)
}

func (wp hasherProgress) Write(p []byte) (n int, err error) {
	n, err = wp.hasher.Write(p)
	if err == nil || errors.Is(err, io.EOF) {
		wp.report(uint64(n))
	}
	return
}

func processFileDedupCandidates(refFile *FileInfos, originalHash []byte, candidates FileInfosList, candidatesHashes [][]byte, concurrent concurrentToolBox) (deduped FileInfosList) {
	deduped = make(FileInfosList, 0, len(candidates))
	for candidateIndex, candidateHash := range candidatesHashes {
		if !bytes.Equal(originalHash, candidateHash) {
			if debug {
				fmt.Fprintf(concurrent.stdout, "File '%s' checksum does not match ref file '%s' checksum: skipping\n",
					candidates[candidateIndex].FullPath, refFile.FullPath)
			}
			continue
		}
		// Same file found !
		if apply {
			fmt.Fprintf(concurrent.stdout, "Match found for '%s': '%s' has the same checksum: replacing by a hard link\n",
				refFile.FullPath, candidates[candidateIndex].FullPath)
			if err := os.Remove(candidates[candidateIndex].FullPath); err != nil {
				concurrent.errChan <- fmt.Errorf("can not make hardlink against '%s': failed to remove '%s': %s", refFile.FullPath, candidates[candidateIndex].FullPath, err)
				continue
			}
			if err := os.Link(refFile.FullPath, candidates[candidateIndex].FullPath); err != nil {
				concurrent.errChan <- fmt.Errorf("can not make hardlink between '%s' <> '%s' (file already removed!): %s", refFile.FullPath, candidates[candidateIndex].FullPath, err)
				continue
			}
			deduped = append(deduped, candidates[candidateIndex])
		} else {
			fmt.Fprintf(concurrent.stdout, "Match found for '%s': '%s' has the same checksum: it could be replaced by a hard link\n",
				refFile.FullPath, candidates[candidateIndex].FullPath)
			deduped = append(deduped, candidates[candidateIndex])
		}
	}
	return
}
