package main

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hekmon/liveterm"
	"golang.org/x/sync/semaphore"
)

const (
	specialFileModes = os.ModeSymlink | os.ModeDevice | os.ModeNamedPipe | os.ModeSocket | os.ModeCharDevice | os.ModeIrregular
)

type FileInfos struct {
	FullPath       string
	Infos          fs.FileInfo
	Children       FileInfosList
	ChildrenAccess *sync.Mutex
}

type FileInfosList []*FileInfos

func (list FileInfosList) String() string {
	fullPaths := make([]string, len(list))
	for index, candidate := range list {
		fullPaths[index] = candidate.FullPath
	}
	return "'" + strings.Join(fullPaths, "', '") + "'"
}

func index(pathA, pathB string, tokenPool *semaphore.Weighted) (pathATree, pathBTree *FileInfos, nbAFiles int64, errorCount int) {
	// Prepare to launch goroutines
	var workers sync.WaitGroup
	errChan := make(chan error)
	scannedA := new(atomic.Int64)
	scannedB := new(atomic.Int64)
	// Prepare live term
	var stdout, stderr io.Writer
	err := liveterm.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start liveterm: %s\n", err)
		stdout = os.Stdout
		stderr = os.Stderr
	} else {
		defer liveterm.Stop(true)
		stdout = liveterm.Bypass()
		stderr = stdout
		liveterm.SetSingleLineUpdateFx(func() string {
			return fmt.Sprintf("Indexing: %d regular files found on '%s' & %d regular files found on '%s'\n",
				scannedA.Load(), pathA, scannedB.Load(), pathB)
		})
	}
	// error logger
	errorsDone := make(chan any)
	go func() {
		for err := range errChan {
			fmt.Fprintf(stderr, "ERROR: %s\n", err)
			errorCount++
		}
		close(errorsDone)
	}()
	// Launch the path A walker
	fakeAParent := FileInfos{
		Children:       make(FileInfosList, 0, 1),
		ChildrenAccess: new(sync.Mutex),
	}
	_ = tokenPool.Acquire(context.Background(), 1) // no err check as error can only come from expired context
	workers.Add(1)
	go func() {
		indexChild(pathA, &fakeAParent, tokenPool, &workers, scannedA, errChan)
		tokenPool.Release(1)
		workers.Done()
	}()
	// Launch the path B walker
	fakeBParent := FileInfos{
		Children:       make(FileInfosList, 0, 1),
		ChildrenAccess: new(sync.Mutex),
	}
	_ = tokenPool.Acquire(context.Background(), 1) // no err check as error can only come from expired context
	workers.Add(1)
	go func() {
		indexChild(pathB, &fakeBParent, tokenPool, &workers, scannedB, errChan)
		tokenPool.Release(1)
		workers.Done()
	}()
	// Wait for walkers to return
	workers.Wait()
	// Stop utilities goroutines
	close(errChan)
	// Return both trees root
	pathATree = fakeAParent.Children[0]
	pathBTree = fakeBParent.Children[0]
	// Print results
	<-errorsDone
	totalA := scannedA.Load()
	totalB := scannedB.Load()
	total := totalA + totalB
	fmt.Fprintf(stdout, "Indexing done: %d total files found\n\t%d on %s\n\t%d on %s\n", total, totalA, pathA, totalB, pathB)
	nbAFiles = totalA
	return
}

func indexChild(pathScan string, parent *FileInfos, tokenPool *semaphore.Weighted, waitGroup *sync.WaitGroup, scanned *atomic.Int64, errChan chan<- error) {
	var err error
	self := FileInfos{
		FullPath: pathScan,
	}
	// get infos
	if self.Infos, err = os.Lstat(pathScan); err != nil {
		errChan <- fmt.Errorf("failed to stat '%s': %w: this path won't be included in the scan", pathScan, err)
		return
	}
	// if special file: skip
	if self.Infos.Mode()&specialFileModes != 0 {
		return
	}
	// if dir, scan it too
	if self.Infos.IsDir() {
		// list dir
		var entries []fs.DirEntry
		if entries, err = os.ReadDir(pathScan); err != nil {
			errChan <- fmt.Errorf("failed to list directory '%s': %w: this path won't be included in the scan", pathScan, err)
			return
		}
		// process list
		self.Children = make(FileInfosList, 0, len(entries))
		self.ChildrenAccess = new(sync.Mutex)
		for _, entry := range entries {
			// can we launch it concurrently ?
			if tokenPool.TryAcquire(1) {
				waitGroup.Add(1)
				go func(localEntry fs.DirEntry) {
					indexChild(path.Join(pathScan, localEntry.Name()), &self, tokenPool, waitGroup, scanned, errChan)
					tokenPool.Release(1)
					waitGroup.Done()
				}(entry)
			} else {
				indexChild(path.Join(pathScan, entry.Name()), &self, tokenPool, waitGroup, scanned, errChan)
			}
		}
	} else {
		// regular file, update stats
		scanned.Add(1)
	}
	// register to parent when possible in a non weighted goroutine
	waitGroup.Add(1)
	go func() {
		parent.ChildrenAccess.Lock()
		parent.Children = append(parent.Children, &self)
		parent.ChildrenAccess.Unlock()
		waitGroup.Done()
	}()
}
