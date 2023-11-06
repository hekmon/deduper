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

func validatePaths(pathA, pathB string) (err error) {
	pathA = path.Clean(pathA)
	pathB = path.Clean(pathB)
	// Standalone tests
	pathAStat, err := validatePath(pathA)
	if err != nil {
		return fmt.Errorf("path A error: %w", err)
	}
	pathBStat, err := validatePath(pathB)
	if err != nil {
		return fmt.Errorf("path B error: %w", err)
	}
	// Relative tests
	//// They can not contains each others
	if strings.HasPrefix(pathA, pathB) {
		return fmt.Errorf("path A is contained within path B")
	}
	if strings.HasPrefix(pathB, pathA) {
		return fmt.Errorf("path B is contained within path A")
	}
	//// They need to have the same backing filesystem for hardlinks
	pathADevID, err := getDeviceID(pathAStat)
	if err != nil {
		return fmt.Errorf("can not retreive filesystem ID of path A: %w", err)
	}
	pathBDevID, err := getDeviceID(pathBStat)
	if err != nil {
		return fmt.Errorf("can not retreive filesystem ID of path A: %w", err)
	}
	if pathADevID != pathBDevID {
		return errors.New("path A does not seems to be on the same filesystem than path B")
	}
	// Final check, let's try to make a hardlink
	return hardlinkTest(pathA, pathB)
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
	switch system := pathStat.Sys().(type) {
	case *syscall.Stat_t:
		deviceID = system.Dev
	default:
		err = fmt.Errorf("backing storage device can not be checked: excepting '*syscall.Stat_t' got '%v'", system)
	}
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
