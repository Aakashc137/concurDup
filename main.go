package main

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type pair struct {
	hash     string
	filePath string
}

func processFile(filePath string, pairs map[string][]string, ch chan<- pair, wg *sync.WaitGroup) {
	defer wg.Done()
	file, err := os.Open(filePath)
	if err != nil {
		return
	}
	defer file.Close()

	fileContents, err := io.ReadAll(file)
	if err != nil {
		return
	}

	//skip empty files and files with size more than 1Mb
	if len(fileContents) == 0 || len(fileContents) > 1024*1024 {
		return
	}

	md5Hash := md5.New()
	md5Hash.Write(fileContents)
	md5HashString := fmt.Sprintf("%x", md5Hash.Sum(nil))
	shortmd5HashString := md5HashString[:8]

	pair := pair{
		hash:     shortmd5HashString,
		filePath: filePath,
	}

	ch <- pair
}

func walk(path string, pairs map[string][]string) error {
	ch := make(chan pair)
	defer close(ch)

	//instantiate a waitgroup to wait for all goroutines to finish using the keyword new
	wg := new(sync.WaitGroup)

	go collectPairs(pairs, ch, wg)

	err := filepath.WalkDir(path, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			fmt.Printf("%v", err)
		}

		if d.IsDir() {
			return nil
		}

		wg.Add(1)
		go processFile(path, pairs, ch, wg)

		return nil
	})

	wg.Wait()
	return err
}

func collectPairs(pairs map[string][]string, ch chan pair, wg *sync.WaitGroup) {
	// wait for a goroutine to send a pair
	for pair := range ch {
		pairs[pair.hash] = append(pairs[pair.hash], pair.filePath)

		if len(pairs) != 0 && len(pairs)%100 == 0 {
			fmt.Printf("Processed approximately %d files\n", len(pairs))
		}
	}

	fmt.Printf("Processed all channels")
}

func trackTime(start time.Time) {
	elapsed := time.Since(start)
	// print time taken in seconds rounded to 2 decimal places
	fmt.Printf("Time taken: %.2fs\n", elapsed.Seconds())
}

func main() {
	defer trackTime(time.Now())
	directoryPathArgs := os.Args[1:]

	if len(directoryPathArgs) == 0 {
		fmt.Printf("No filepath provided. Exiting gracefully\n")
		os.Exit(0)
	}

	directoryPath := directoryPathArgs[0]
	filePathsByHash := make(map[string][]string)

	err := walk(directoryPath, filePathsByHash)
	if err != nil {
		fmt.Printf("Error walking directory: %v", err)
	}

	for hash, filePaths := range filePathsByHash {
		if len(filePaths) > 1 {
			fmt.Printf("Duplicate files found for hash: %s\n", hash)
			for _, filePath := range filePaths {
				fmt.Printf("File: %s\n", filePath)
			}
		}
	}
}
