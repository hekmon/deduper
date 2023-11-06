package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

const (
	specialFileModes = os.ModeSymlink | os.ModeDevice | os.ModeNamedPipe | os.ModeSocket | os.ModeCharDevice | os.ModeIrregular
)

type TreeStat struct {
	FullPath       string
	Infos          fs.FileInfo
	Children       []*TreeStat
	ChildrenAccess *sync.Mutex
}

func index(pathA, pathB string, tokenPool *semaphore.Weighted) (pathATree, pathBTree *TreeStat, nbAFiles uint64, err error) {
	// Prepare to launch goroutines
	var workers sync.WaitGroup
	errChan := make(chan error)
	scannedA := new(atomic.Uint64)
	scannedB := new(atomic.Uint64)
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
			fmt.Printf("[indexing] %d regular files found on path A | %d regular files found on path B\n",
				scannedA.Add(0), scannedB.Add(0))
		}
		totalA := scannedA.Add(0)
		totalB := scannedB.Add(0)
		total := totalA + totalB
		fmt.Printf("Indexing done: %d files found ! (%d on path A and %d on path B)\n", total, totalA, totalB)
		nbAFiles = totalA
		close(loggerDone)
	}()
	defer loggerCtxCancel() // in case we do not reach the end of the fx
	// Launch the path A walker
	fakeAParent := TreeStat{
		Children:       make([]*TreeStat, 0, 1),
		ChildrenAccess: new(sync.Mutex),
	}
	if err = tokenPool.Acquire(context.Background(), 1); err != nil {
		err = fmt.Errorf("failed to acquire token for prescan (should not happen): %s", err)
		return
	}
	workers.Add(1)
	go func() {
		indexChild(pathA, &fakeAParent, tokenPool, &workers, scannedA, errChan)
		tokenPool.Release(1)
		workers.Done()
	}()
	// Launch the path B walker
	fakeBParent := TreeStat{
		Children:       make([]*TreeStat, 0, 1),
		ChildrenAccess: new(sync.Mutex),
	}
	if err = tokenPool.Acquire(context.Background(), 1); err != nil {
		err = fmt.Errorf("failed to acquire token for prescan (should not happen): %s", err)
		return
	}
	workers.Add(1)
	go func() {
		indexChild(pathB, &fakeBParent, tokenPool, &workers, scannedB, errChan)
		tokenPool.Release(1)
		workers.Done()
	}()
	// Wait for walkers to return
	workers.Wait()
	// Stop utilities goroutines
	loggerCtxCancel()
	close(errChan)
	<-loggerDone
	// Return both trees root
	if len(fakeAParent.Children) == 0 {
		err = errors.New("path A failed to be analyzed")
		return
	}
	pathATree = fakeAParent.Children[0]
	if len(fakeBParent.Children) == 0 {
		err = errors.New("path B failed to be analyzed")
		return
	}
	pathBTree = fakeBParent.Children[0]
	return
}

func indexChild(pathScan string, parent *TreeStat, tokenPool *semaphore.Weighted, waitGroup *sync.WaitGroup, scanned *atomic.Uint64, errChan chan<- error) {
	var err error
	self := TreeStat{
		FullPath: pathScan,
	}
	// get infos
	if self.Infos, err = os.Lstat(pathScan); err != nil {
		errChan <- fmt.Errorf("failed to stat '%s': %w: this path won't be included in the scan", pathScan, err)
		return
	}
	// if special file: skip
	// fmt.Println(pathScan, "mode", fmt.Sprintf("%b", stats.Infos.Mode()), "special modes:", fmt.Sprintf("%b", specialFileModes), "comparaison &:", fmt.Sprintf("%b", stats.Infos.Mode()&specialFileModes))
	if self.Infos.Mode()&specialFileModes != 0 {
		// fmt.Println("special file !", path.Join(pathScan, stats.Infos.Name()))
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
		self.Children = make([]*TreeStat, 0, len(entries))
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
	// register to parent (when possible)
	waitGroup.Add(1)
	go func() {
		parent.ChildrenAccess.Lock()
		parent.Children = append(parent.Children, &self)
		parent.ChildrenAccess.Unlock()
		waitGroup.Done()
	}()
}
