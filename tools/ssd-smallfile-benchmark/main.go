package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type BenchmarkResult struct {
	Concurrency int
	FileSizeKB  int
	TotalFiles  int
	Mode        string
	Duration    time.Duration
	FilesPerSec float64
	Throughput  float64 // MB/s
	AvgLatMs    float64
	P50LatMs    float64
	P95LatMs    float64
	P99LatMs    float64
	MaxLatMs    float64
}

func runSmallFileTest(targetDir string, concurrency int, fileSizeKB int, totalFiles int, doSync bool) BenchmarkResult {
	fileSize := fileSizeKB * 1024
	sampleData := make([]byte, fileSize)
	_, _ = rand.Read(sampleData[:4096]) // fill header

	testDir := filepath.Join(targetDir, fmt.Sprintf("test_c%d_s%d_sync%v", concurrency, fileSizeKB, doSync))
	_ = os.RemoveAll(testDir)
	if err := os.MkdirAll(testDir, 0755); err != nil {
		panic(fmt.Sprintf("mkdir %s failed: %v", testDir, err))
	}
	defer os.RemoveAll(testDir)

	type fileJob struct {
		id int
	}
	jobChan := make(chan fileJob, totalFiles)
	for i := 0; i < totalFiles; i++ {
		jobChan <- fileJob{id: i}
	}
	close(jobChan)

	var latenciesMu sync.Mutex
	latencies := make([]float64, 0, totalFiles)
	var completedCount int64

	startTime := time.Now()
	var wg sync.WaitGroup

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localLats := make([]float64, 0, totalFiles/concurrency+10)
			for job := range jobChan {
				t0 := time.Now()
				filePath := filepath.Join(testDir, fmt.Sprintf("f_%d_%d.dat", workerID, job.id))
				f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
				if err != nil {
					fmt.Printf("open error: %v\n", err)
					continue
				}
				_, _ = f.Write(sampleData)
				if doSync {
					_ = f.Sync()
				}
				_ = f.Close()
				durMs := float64(time.Since(t0).Microseconds()) / 1000.0
				localLats = append(localLats, durMs)
				atomic.AddInt64(&completedCount, 1)
			}
			latenciesMu.Lock()
			latencies = append(latencies, localLats...)
			latenciesMu.Unlock()
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	sort.Float64s(latencies)
	var sumLat float64
	for _, l := range latencies {
		sumLat += l
	}
	avgLat := sumLat / float64(len(latencies))
	p50 := latencies[int(float64(len(latencies))*0.50)]
	p95 := latencies[int(float64(len(latencies))*0.95)]
	p99 := latencies[int(float64(len(latencies))*0.99)]
	maxLat := latencies[len(latencies)-1]

	modeStr := "Buffered"
	if doSync {
		modeStr = "Sync(fsync)"
	}

	totalMB := float64(totalFiles*fileSize) / (1024 * 1024)
	return BenchmarkResult{
		Concurrency: concurrency,
		FileSizeKB:  fileSizeKB,
		TotalFiles:  totalFiles,
		Mode:        modeStr,
		Duration:    elapsed,
		FilesPerSec: float64(totalFiles) / elapsed.Seconds(),
		Throughput:  totalMB / elapsed.Seconds(),
		AvgLatMs:    avgLat,
		P50LatMs:    p50,
		P95LatMs:    p95,
		P99LatMs:    p99,
		MaxLatMs:    maxLat,
	}
}

func main() {
	targetDir := flag.String("dir", "/app/temp/tdl", "Target SSD directory")
	flag.Parse()

	fmt.Printf("=== NAS NVMe SSD Concurrent Small File Write Benchmark ===\n")
	fmt.Printf("Target Directory: %s\n\n", *targetDir)

	concurrencies := []int{4, 8, 16, 32, 64, 128}

	// Suite 1: 100 KB Small Files (Images / Photos - 2,000 files per test)
	fmt.Println("==========================================================================================")
	fmt.Println("--- Test Suite 1: 100 KB Images / Small Files (2,000 files per run, Total 200 MB) ---")
	fmt.Println("==========================================================================================")
	fmt.Printf("%-12s %-12s %-14s %-14s %-10s %-10s %-10s %-10s %-10s\n",
		"Concurrency", "Mode", "Files/Sec", "Throughput", "Avg Lat", "P50 Lat", "P95 Lat", "P99 Lat", "Max Lat")
	for _, c := range concurrencies {
		res := runSmallFileTest(*targetDir, c, 100, 2000, false)
		fmt.Printf("%-12d %-12s %10.1f f/s %8.2f MB/s %8.2fms %8.2fms %8.2fms %8.2fms %8.2fms\n",
			res.Concurrency, res.Mode, res.FilesPerSec, res.Throughput, res.AvgLatMs, res.P50LatMs, res.P95LatMs, res.P99LatMs, res.MaxLatMs)
	}

	// Suite 2: 500 KB Small Files (High-res Photos / Audio - 1,000 files per test)
	fmt.Println("\n==========================================================================================")
	fmt.Println("--- Test Suite 2: 500 KB Medium Files (1,000 files per run, Total 500 MB) ---")
	fmt.Println("==========================================================================================")
	fmt.Printf("%-12s %-12s %-14s %-14s %-10s %-10s %-10s %-10s %-10s\n",
		"Concurrency", "Mode", "Files/Sec", "Throughput", "Avg Lat", "P50 Lat", "P95 Lat", "P99 Lat", "Max Lat")
	for _, c := range concurrencies {
		res := runSmallFileTest(*targetDir, c, 500, 1000, false)
		fmt.Printf("%-12d %-12s %10.1f f/s %8.2f MB/s %8.2fms %8.2fms %8.2fms %8.2fms %8.2fms\n",
			res.Concurrency, res.Mode, res.FilesPerSec, res.Throughput, res.AvgLatMs, res.P50LatMs, res.P95LatMs, res.P99LatMs, res.MaxLatMs)
	}

	// Suite 3: 1 MB Boundary Files (1,000 files per test, Total 1,000 MB)
	fmt.Println("\n==========================================================================================")
	fmt.Println("--- Test Suite 3: 1 MB Boundary Files (1,000 files per run, Total 1,000 MB) ---")
	fmt.Println("==========================================================================================")
	fmt.Printf("%-12s %-12s %-14s %-14s %-10s %-10s %-10s %-10s %-10s\n",
		"Concurrency", "Mode", "Files/Sec", "Throughput", "Avg Lat", "P50 Lat", "P95 Lat", "P99 Lat", "Max Lat")
	for _, c := range concurrencies {
		res := runSmallFileTest(*targetDir, c, 1024, 1000, false)
		fmt.Printf("%-12d %-12s %10.1f f/s %8.2f MB/s %8.2fms %8.2fms %8.2fms %8.2fms %8.2fms\n",
			res.Concurrency, res.Mode, res.FilesPerSec, res.Throughput, res.AvgLatMs, res.P50LatMs, res.P95LatMs, res.P99LatMs, res.MaxLatMs)
	}

	// Suite 4: 100 KB Small Files with Physical SSD fsync (1,000 files per test)
	fmt.Println("\n==========================================================================================")
	fmt.Println("--- Test Suite 4: 100 KB with Direct SSD fsync (1,000 files per run) ---")
	fmt.Println("==========================================================================================")
	fmt.Printf("%-12s %-12s %-14s %-14s %-10s %-10s %-10s %-10s %-10s\n",
		"Concurrency", "Mode", "Files/Sec", "Throughput", "Avg Lat", "P50 Lat", "P95 Lat", "P99 Lat", "Max Lat")
	for _, c := range concurrencies {
		res := runSmallFileTest(*targetDir, c, 100, 1000, true)
		fmt.Printf("%-12d %-12s %10.1f f/s %8.2f MB/s %8.2fms %8.2fms %8.2fms %8.2fms %8.2fms\n",
			res.Concurrency, res.Mode, res.FilesPerSec, res.Throughput, res.AvgLatMs, res.P50LatMs, res.P95LatMs, res.P99LatMs, res.MaxLatMs)
	}
}
