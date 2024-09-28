# Duplicate File Finder

Scans a directory to identify duplicate files based on their content (MD5 hash). Utilizes Go's concurrency primitives like `goroutines`, `channels`, and `sync.WaitGroup` to process files in parallel for efficient performance.
