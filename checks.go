package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"
	"syscall"
)

const (
	testFileA = "deduper_fileA.test"
	testFileB = "deduper_fileB.test"
)

// TODO: reactivate tests
func validatePaths(pathA, pathB string) (cleanPathA, cleanPathB string, err error) {
	cleanPathA = path.Clean(pathA)
	cleanPathB = path.Clean(pathB)
	// Standalone tests
	_, err = validatePath(cleanPathA)
	if err != nil {
		err = fmt.Errorf("path A error: %w", err)
		return
	}
	_, err = validatePath(cleanPathB)
	if err != nil {
		err = fmt.Errorf("path B error: %w", err)
		return
	}
	// Relative tests
	//// They can not contains each others
	if strings.HasPrefix(cleanPathA, cleanPathB) {
		err = fmt.Errorf("path A is contained within path B")
		return
	}
	if strings.HasPrefix(cleanPathB, cleanPathA) {
		err = fmt.Errorf("path B is contained within path A")
		return
	}
	// //// They need to have the same backing filesystem for hardlinks
	// pathADevID, err := getDeviceID(pathAStat)
	// if err != nil {
	// 	err = fmt.Errorf("can not retreive filesystem ID of path A: %w", err)
	// 	return
	// }
	// pathBDevID, err := getDeviceID(pathBStat)
	// if err != nil {
	// 	err = fmt.Errorf("can not retreive filesystem ID of path A: %w", err)
	// 	return
	// }
	// if pathADevID != pathBDevID {
	// 	err = errors.New("path A does not seems to be on the same filesystem than path B")
	// 	return
	// }
	// // Final check, let's try to make a hardlink
	// err = hardlinkTest(cleanPathA, cleanPathB)
	return
}

func validatePath(pathCandidate string) (pathStat fs.FileInfo, err error) {
	if pathCandidate == "" {
		err = errors.New("path is empty")
		return
	}
	if !path.IsAbs(pathCandidate) {
		err = errors.New("not an absolute path")
		return
	}
	if pathStat, err = os.Stat(pathCandidate); err != nil {
		err = fmt.Errorf("cannot stat path: %w", err)
		return
	}
	if !pathStat.IsDir() {
		err = errors.New("path is not a directory")
		return
	}
	return
}

func getDeviceID(pathStat fs.FileInfo) (deviceID uint64, err error) {
	system, ok := pathStat.Sys().(*syscall.Stat_t)
	if !ok {
		err = fmt.Errorf("backing storage device can not be checked: excepting '*syscall.Stat_t' got '%v'", system)
		return
	}
	deviceID = system.Dev
	return
}

func hardlinkTest(pathA, pathB string) (err error) {
	// Create file on pathA
	testPathA := path.Join(pathA, testFileA)
	testFile, err := os.OpenFile(testPathA, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
	if err != nil {
		return fmt.Errorf("failed to create test file for hardlink: %w", err)
	}
	testFile.Close()
	defer os.Remove(testPathA)
	// Create a hardlink on pathB
	testPathB := path.Join(pathB, testFileB)
	if err = os.Link(testPathA, testPathB); err != nil {
		return fmt.Errorf("failed to create a test hardlink: %w", err)
	}
	os.Remove(testPathB)
	return
}
