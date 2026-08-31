package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"

	"github.com/Hittlert/TGX/core/dcpool"
	"github.com/Hittlert/TGX/core/storage"
	"github.com/Hittlert/TGX/core/tclient"
	"github.com/Hittlert/TGX/core/tmedia"
	"github.com/Hittlert/TGX/core/util/tutil"
	"github.com/Hittlert/TGX/pkg/kv"
)

type SmallFileTask struct {
	ChatID    string
	MessageID int
	FileSize  int64
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func main() {
	dbPath := flag.String("db", "/app/state/download_records.sqlite3", "Path to sqlite DB")
	sessionDir := flag.String("session-dir", "/data", "Path to session bbolt directory")
	namespace := flag.String("ns", "production", "Session namespace")
	concurrency := flag.Int("c", 32, "Concurrent workers")
	limit := flag.Int("n", 300, "Number of small files to benchmark")
	flag.Parse()

	// 1. Query most recent small files from sqlite
	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		panic(fmt.Sprintf("open db error: %v", err))
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT chat_id, message_id, file_size FROM download_records WHERE file_size > 0 AND file_size <= 1048576 ORDER BY updated_at DESC LIMIT %d", *limit))
	if err != nil {
		panic(fmt.Sprintf("query error: %v", err))
	}
	defer rows.Close()

	var tasks []SmallFileTask
	var totalExpectedBytes int64
	for rows.Next() {
		var t SmallFileTask
		if err := rows.Scan(&t.ChatID, &t.MessageID, &t.FileSize); err == nil {
			tasks = append(tasks, t)
			totalExpectedBytes += t.FileSize
		}
	}
	fmt.Printf("Loaded %d recent small file tasks (Total payload: %.2f MB)\n", len(tasks), float64(totalExpectedBytes)/(1024*1024))
	if len(tasks) == 0 {
		fmt.Println("No tasks found in DB!")
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Make a temporary copy of the session file to avoid bbolt lock conflict with the running daemon
	tempSessionDir := "/app/temp/tdl/bench_session"
	_ = os.MkdirAll(tempSessionDir, 0755)
	srcFile := filepath.Join(*sessionDir, *namespace)
	dstFile := filepath.Join(tempSessionDir, *namespace)
	if err := copyFile(srcFile, dstFile); err != nil {
		panic(fmt.Sprintf("failed to copy session file: %v", err))
	}
	defer os.RemoveAll(tempSessionDir)

	// Open KV storage for session
	stg, err := kv.NewWithMap(map[string]string{
		kv.DriverTypeKey: kv.DriverBolt.String(),
		"path":           tempSessionDir,
	})
	if err != nil {
		panic(fmt.Sprintf("open kv error: %v", err))
	}
	defer stg.Close()

	kvd, err := stg.Open(*namespace)
	if err != nil {
		panic(fmt.Sprintf("open namespace error: %v", err))
	}

	client, err := tclient.New(ctx, tclient.Options{
		KV:               kvd,
		ReconnectTimeout: 10 * time.Second,
	})
	if err != nil {
		panic(fmt.Sprintf("create client error: %v", err))
	}

	err = client.Run(ctx, func(ctx context.Context) error {
		api := client.API()
		manager := peers.Options{Storage: storage.NewPeers(kvd)}.Build(api)

		fmt.Println("Telegram client connected. Initializing DC Pool...")

		// Pre-resolve messages and group by peer
		type resolvedItem struct {
			inputPeer tg.InputPeerClass
			location  tg.InputFileLocationClass
			dc        int
			size      int64
			name      string
		}

		resolvedTasks := make([]resolvedItem, 0, len(tasks))
		fmt.Printf("Resolving %d media locations...\n", len(tasks))

		// Group by chat
		chatTasks := make(map[string][]int)
		for _, t := range tasks {
			chatTasks[t.ChatID] = append(chatTasks[t.ChatID], t.MessageID)
		}

		for chatID, msgIDs := range chatTasks {
			peer, err := tutil.GetInputPeer(ctx, manager, chatID)
			if err != nil {
				continue
			}
			for i := 0; i < len(msgIDs); i += 50 {
				end := i + 50
				if end > len(msgIDs) {
					end = len(msgIDs)
				}
				chunkIDs := msgIDs[i:end]
				msgs, err := tutil.GetMessagesBatch(ctx, api, peer.InputPeer(), chunkIDs)
				if err != nil {
					continue
				}
				for _, m := range msgs {
					if m != nil {
						media, ok := tmedia.GetMedia(m)
						if ok && media.Size > 0 && media.Size <= 1048576 && media.InputFileLoc != nil {
							resolvedTasks = append(resolvedTasks, resolvedItem{
								inputPeer: peer.InputPeer(),
								location:  media.InputFileLoc,
								dc:        media.DC,
								size:      media.Size,
								name:      media.Name,
							})
						}
					}
				}
			}
		}

		fmt.Printf("Successfully resolved %d valid downloadable media items.\n", len(resolvedTasks))
		if len(resolvedTasks) == 0 {
			return fmt.Errorf("no resolved media available")
		}

		// Initialize DC Pool for data transfer
		pool := dcpool.NewPool(client, int64(*concurrency))
		defer pool.Close()

		// Warm up DC connections
		dcClients := make(map[int]*tg.Client)
		for _, item := range resolvedTasks {
			if _, exists := dcClients[item.dc]; !exists {
				dcClients[item.dc] = pool.Client(ctx, item.dc)
			}
		}

		fmt.Printf("\n--- Starting 32-Worker Pipeline Benchmark (In-Memory Download & Discard) ---\n")

		taskChan := make(chan resolvedItem, len(resolvedTasks))
		for _, item := range resolvedTasks {
			taskChan <- item
		}
		close(taskChan)

		var totalDownloaded int64
		var completedFiles int64
		var failedFiles int64
		var latenciesMu sync.Mutex
		latencies := make([]float64, 0, len(resolvedTasks))

		// Bandwidth monitor
		stopMonitor := make(chan struct{})
		var peakSpeedBps float64
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			var lastBytes int64
			var lastTime = time.Now()
			for {
				select {
				case <-stopMonitor:
					return
				case t := <-ticker.C:
					curr := atomic.LoadInt64(&totalDownloaded)
					dt := t.Sub(lastTime).Seconds()
					if dt > 0 {
						bps := float64(curr-lastBytes) / dt
						if bps > peakSpeedBps {
							peakSpeedBps = bps
						}
						lastBytes = curr
						lastTime = t
					}
				}
			}
		}()

		startTime := time.Now()
		var wg sync.WaitGroup

		for w := 0; w < *concurrency; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				// Persistent worker pipeline: worker takes task 1 -> downloads immediately -> takes task 2 on same connection
				for item := range taskChan {
					t0 := time.Now()
					client := dcClients[item.dc]
					if client == nil {
						client = pool.Client(ctx, item.dc)
					}

					req := &tg.UploadGetFileRequest{
						Location: item.location,
						Offset:   0,
						Limit:    1024 * 1024, // 1MB part
					}

					res, err := client.UploadGetFile(ctx, req)
					if err == nil {
						if uf, ok := res.(*tg.UploadFile); ok {
							bytesLen := int64(len(uf.Bytes))
							atomic.AddInt64(&totalDownloaded, bytesLen)
							atomic.AddInt64(&completedFiles, 1)
							// Zero Disk I/O: buffer is immediately discarded and GC-eligible!
							_ = uf.Bytes
						} else {
							atomic.AddInt64(&failedFiles, 1)
						}
					} else {
						atomic.AddInt64(&failedFiles, 1)
					}
					latMs := float64(time.Since(t0).Microseconds()) / 1000.0
					latenciesMu.Lock()
					latencies = append(latencies, latMs)
					latenciesMu.Unlock()
				}
			}(w)
		}

		wg.Wait()
		close(stopMonitor)
		totalDuration := time.Since(startTime)

		finalDownloaded := atomic.LoadInt64(&totalDownloaded)
		finalCompleted := atomic.LoadInt64(&completedFiles)
		finalFailed := atomic.LoadInt64(&failedFiles)
		avgThroughputMBps := float64(finalDownloaded) / (1024 * 1024) / totalDuration.Seconds()
		avgThroughputMbps := avgThroughputMBps * 8
		peakThroughputMBps := peakSpeedBps / (1024 * 1024)
		peakThroughputMbps := peakThroughputMBps * 8

		sort.Float64s(latencies)
		var sumLat float64
		for _, l := range latencies {
			sumLat += l
		}
		avgLat := sumLat / float64(len(latencies))
		p50 := latencies[int(float64(len(latencies))*0.50)]
		p95 := latencies[int(float64(len(latencies))*0.95)]
		p99 := latencies[int(float64(len(latencies))*0.99)]

		fmt.Printf("\n=== Benchmark Results ===\n")
		fmt.Printf("Files Completed:        %d / %d (Failed: %d)\n", finalCompleted, len(resolvedTasks), finalFailed)
		fmt.Printf("Total Data Downloaded:  %.2f MB\n", float64(finalDownloaded)/(1024*1024))
		fmt.Printf("Total Time Elapsed:     %.3f seconds\n", totalDuration.Seconds())
		fmt.Printf("Average Throughput:     %.2f MB/s (%.2f Mbps)\n", avgThroughputMBps, avgThroughputMbps)
		fmt.Printf("Peak Bandwidth:         %.2f MB/s (%.2f Mbps)\n", peakThroughputMBps, peakThroughputMbps)
		fmt.Printf("Average File Latency:   %.2f ms\n", avgLat)
		fmt.Printf("P50 File Latency:       %.2f ms\n", p50)
		fmt.Printf("P95 File Latency:       %.2f ms\n", p95)
		fmt.Printf("P99 File Latency:       %.2f ms\n", p99)
		fmt.Printf("File Download Rate:     %.1f files/sec\n", float64(finalCompleted)/totalDuration.Seconds())

		return nil
	})

	if err != nil {
		fmt.Printf("Benchmark error: %v\n", err)
	}
}
