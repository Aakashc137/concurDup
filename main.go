package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const workerPoolSize = 16

type fileHashPair struct {
	hash     string
	filePath string
}

func main() {
	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime)
		fmt.Printf("Time taken: %.2fs\n", elapsed.Seconds())
	}()

	if len(os.Args) < 2 {
		fmt.Println("Usage: program <directory_path>")
		os.Exit(1)
	}

	directoryPath := os.Args[1]
	filePathsByHash := make(map[string][]string)

	if err := walkDirectory(directoryPath, filePathsByHash); err != nil {
		fmt.Printf("Error walking directory: %v\n", err)
	}

	writeDuplicateFiles(filePathsByHash)
}

func walkDirectory(path string, pairs map[string][]string) error {
	var workerWg sync.WaitGroup
	fileHashCh := make(chan fileHashPair)
	done := make(chan struct{})
	filesProcessed := 0

	// Start the collector goroutine

	go func() {
		collectFileHashes(pairs, fileHashCh, &filesProcessed)
		close(done)
	}()

	filePathCh := make(chan string, workerPoolSize)

	// Start worker goroutines
	for i := 0; i < workerPoolSize; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			processFiles(filePathCh, fileHashCh)
		}()
	}

	// Walk through the directory and send file paths to filePathCh
	err := filepath.WalkDir(path, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			filePathCh <- path
		}
		return nil
	})
	close(filePathCh)

	// Wait for all workers to complete
	workerWg.Wait()
	close(fileHashCh)

	// Wait for collector to finish processing
	<-done

	return err
}

func processFiles(filePathCh <-chan string, fileHashCh chan<- fileHashPair) {
	for filePath := range filePathCh {
		processSingleFile(filePath, fileHashCh)
	}
}

func processSingleFile(filePath string, fileHashCh chan<- fileHashPair) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Failed to open file %s: %v\n", filePath, err)

	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("Failed to stat file %s: %v\n", filePath, err)
		return
	}

	// Skip empty files and files larger than 1MB
	if fileInfo.Size() == 0 || fileInfo.Size() > 1*1024*1024 {
		return
	}

	hasher := md5.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return
	}

	hash := fmt.Sprintf("%x", hasher.Sum(nil))[:8]
	fileHashCh <- fileHashPair{hash: hash, filePath: filePath}
}

func collectFileHashes(pairs map[string][]string, fileHashCh <-chan fileHashPair, filesProcessed *int) {
	for pair := range fileHashCh {
		*filesProcessed++
		pairs[pair.hash] = append(pairs[pair.hash], pair.filePath)
		if len(pairs)%10000 == 0 {
			fmt.Printf("Processed approximately %d unique hashes\n", len(pairs))
		}
	}
	fmt.Printf("Processed %d files\n", *filesProcessed)
}

type DuplicateEntry struct {
	Hash      string   `json:"hash"`
	FilePaths []string `json:"filePaths"`
}

func writeDuplicateFiles(filePathsByHash map[string][]string) error {
	file, err := os.Create("duplicateFiles.json")
	if err != nil {
		return fmt.Errorf("error while creating file: %v", err)
	}
	defer file.Close()

	// Start the JSON array
	_, err = file.Write([]byte("[\n"))
	if err != nil {
		return fmt.Errorf("error writing to file: %v", err)
	}

	first := true
	for hash, filePaths := range filePathsByHash {
		if len(filePaths) > 1 {
			if !first {
				_, err = file.Write([]byte(",\n"))
				if err != nil {
					return fmt.Errorf("error writing to file: %v", err)
				}
			} else {
				first = false
			}

			entry := DuplicateEntry{
				Hash:      hash,
				FilePaths: filePaths,
			}

			// Encode the entry without adding a newline
			entryBytes, err := json.Marshal(entry)
			if err != nil {
				return fmt.Errorf("error marshalling entry: %v", err)
			}

			_, err = file.Write(entryBytes)
			if err != nil {
				return fmt.Errorf("error writing entry to file: %v", err)
			}
		}
	}

	// End the JSON array
	_, err = file.Write([]byte("\n]\n"))
	if err != nil {
		return fmt.Errorf("error writing to file: %v", err)
	}

	return nil
}
