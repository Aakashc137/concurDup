package main

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func processFile(filePath string, pairs map[string][]string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	fileContents, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	//skip empty files and files with size more than 500Mb
	if len(fileContents) == 0 || len(fileContents) > 500*1024*1024 {
		return nil
	}

	md5Hash := md5.New()
	md5Hash.Write(fileContents)

	md5HashString := fmt.Sprintf("%x", md5Hash.Sum(nil))
	shortmd5HashString := md5HashString[:8]
	pairs[shortmd5HashString] = append(pairs[shortmd5HashString], filePath)

	if len(pairs) != 0 && len(pairs)%100 == 0 {
		fmt.Printf("Processed approximately %d|%d files\n", len(pairs), count)
	}

	return nil
}

func walk(path string, pairs map[string][]string) error {
	err := filepath.WalkDir(path, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			fmt.Printf("%v", err)
		}

		if d.IsDir() {
			return nil
		}

		err = processFile(path, pairs)
		if err != nil {
			// We are not checking for errors, If we encounter an error, we will just skip the file
			return nil
		}

		return nil
	})

	return err
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
