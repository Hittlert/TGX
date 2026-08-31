package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Result struct {
	Concurrency int
	Mode        string
	TotalMB     int
	Duration    time.Duration
	Throughput  float64 // MB/s
	AvgLatMs    float64
	P50LatMs    float64
	P95LatMs    float64
	P99LatMs    float64
	MaxLatMs    float64
}

func runBenchmark(dir string, concurrency int, numFiles int, totalChunks int, syncInterval int, directSync bool) Result {
	const chunkSize = 1024 * 1024 // 1 MiB
	chunksPerFile := int64(1000)  // 1000 MiB (1 GiB) file size

	// Prepare target files
	files := make([]*os.File, numFiles)
	for i := 0; i < numFiles; i++ {
		path := filepath.Join(dir, fmt.Sprintf("bench_file_%d.tmp", i))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
		if err != nil {
			panic(fmt.Sprintf("create file %s failed: %v", path, err))
		}
		// Preallocate 1GB
		_ = f.Truncate(chunksPerFile * chunkSize)
		files[i] = f
	}
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
		for i := 0; i < numFiles; i++ {
			_ = os.Remove(filepath.Join(dir, fmt.Sprintf("bench_file_%d.tmp", i)))
		}
	}()

	// Fill sample 1MB chunk data
	sampleData := make([]byte, chunkSize)
	_, _ = rand.Read(sampleData[:4096]) // non-zero content

	// Generate random chunk jobs across files
	type chunkJob struct {
		fileIdx int
		offset  int64
	}
	jobs := make([]chunkJob, totalChunks)
	for i := 0; i < totalChunks; i++ {
		jobs[i] = chunkJob{
			fileIdx: i % numFiles,
			offset:  int64((i*17)%int(chunksPerFile)) * chunkSize,
		}
	}

	jobChan := make(chan chunkJob, totalChunks)
	for _, j := range jobs {
		jobChan <- j
	}
	close(jobChan)

	var latenciesMu sync.Mutex
	latencies := make([]float64, 0, totalChunks)

	var writtenChunks int64
	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localLats := make([]float64, 0, totalChunks/concurrency+10)
			for job := range jobChan {
				t0 := time.Now()
				f := files[job.fileIdx]
				_, err := f.WriteAt(sampleData, job.offset)
				if err != nil {
					fmt.Printf("write error: %v\n", err)
					return
				}
				cnt := atomic.AddInt64(&writtenChunks, 1)
				if directSync {
					_ = f.Sync()
				} else if syncInterval > 0 && cnt%int64(syncInterval) == 0 {
					_ = f.Sync()
				}
				durMs := float64(time.Since(t0).Microseconds()) / 1000.0
				localLats = append(localLats, durMs)
			}
			latenciesMu.Lock()
			latencies = append(latencies, localLats...)
			latenciesMu.Unlock()
		}()
	}

	wg.Wait()
	// Final sync to flush everything to disk
	for _, f := range files {
		_ = f.Sync()
	}
	elapsed := time.Since(start)

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
	if directSync {
		modeStr = "Sync(Direct)"
	} else if syncInterval > 0 {
		modeStr = fmt.Sprintf("Sync(every %d)", syncInterval)
	}

	return Result{
		Concurrency: concurrency,
		Mode:        modeStr,
		TotalMB:     totalChunks,
		Duration:    elapsed,
		Throughput:  float64(totalChunks) / elapsed.Seconds(),
		AvgLatMs:    avgLat,
		P50LatMs:    p50,
		P95LatMs:    p95,
		P99LatMs:    p99,
		MaxLatMs:    maxLat,
	}
}

func main() {
	targetDir := flag.String("dir", ".", "Directory to test write performance")
	totalMB := flag.Int("mb", 1000, "Total MB to write per test run")
	flag.Parse()

	fmt.Printf("=== NAS HDD Concurrent Write Benchmark ===\n")
	fmt.Printf("Target Directory: %s\n", *targetDir)
	fmt.Printf("Total Data per Run: %d MB (1MB chunks across 5 active files)\n\n", *totalMB)

	concurrencies := []int{4, 8, 16, 32, 64}

	// 1. Buffered Mode (Standard Production TGX Model - OS Page Cache absorbing writes)
	fmt.Printf("--- Test 1: Buffered Async Write (TGX Production Mode) ---\n")
	fmt.Printf("%-12s %-12s %-12s %-10s %-10s %-10s %-10s %-10s\n",
		"Concurrency", "Mode", "Throughput", "Avg Lat", "P50 Lat", "P95 Lat", "P99 Lat", "Max Lat")
	for _, c := range concurrencies {
		res := runBenchmark(*targetDir, c, 5, *totalMB, 0, false)
		fmt.Printf("%-12d %-12s %8.2f MB/s %8.2fms %8.2fms %8.2fms %8.2fms %8.2fms\n",
			res.Concurrency, res.Mode, res.Throughput, res.AvgLatMs, res.P50LatMs, res.P95LatMs, res.P99LatMs, res.MaxLatMs)
	}

	// 2. Periodic Sync Mode (fsync every 16 chunks / 16MB)
	fmt.Printf("\n--- Test 2: Semi-Synchronous Write (fsync every 16 MB) ---\n")
	fmt.Printf("%-12s %-12s %-12s %-10s %-10s %-10s %-10s %-10s\n",
		"Concurrency", "Mode", "Throughput", "Avg Lat", "P50 Lat", "P95 Lat", "P99 Lat", "Max Lat")
	for _, c := range concurrencies {
		res := runBenchmark(*targetDir, c, 5, *totalMB, 16, false)
		fmt.Printf("%-12d %-12s %8.2f MB/s %8.2fms %8.2fms %8.2fms %8.2fms %8.2fms\n",
			res.Concurrency, res.Mode, res.Throughput, res.AvgLatMs, res.P50LatMs, res.P95LatMs, res.P99LatMs, res.MaxLatMs)
	}

	// 3. Physical Disk Saturation Mode (fsync on every chunk - pure magnetic platter seek stress)
	fmt.Printf("\n--- Test 3: Pure Magnetic Platter Direct Write (fsync per 1MB chunk) ---\n")
	fmt.Printf("%-12s %-12s %-12s %-10s %-10s %-10s %-10s %-10s\n",
		"Concurrency", "Mode", "Throughput", "Avg Lat", "P50 Lat", "P95 Lat", "P99 Lat", "Max Lat")
	for _, c := range concurrencies {
		// Use smaller totalMB for direct sync so test completes in reasonable time
		res := runBenchmark(*targetDir, c, 5, int(math.Min(float64(*totalMB), 200)), 0, true)
		fmt.Printf("%-12d %-12s %8.2f MB/s %8.2fms %8.2fms %8.2fms %8.2fms %8.2fms\n",
			res.Concurrency, res.Mode, res.Throughput, res.AvgLatMs, res.P50LatMs, res.P95LatMs, res.P99LatMs, res.MaxLatMs)
	}
}
