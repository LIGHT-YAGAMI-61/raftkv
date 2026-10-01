package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LIGHT-YAGAMI-61/raftkv/client"
)

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func main() {
	nodes := flag.String("nodes", "localhost:5001,localhost:5002,localhost:5003", "comma-separated node addresses")
	duration := flag.Duration("duration", 10*time.Second, "how long to run")
	concurrency := flag.Int("concurrency", 10, "number of concurrent clients")
	readRatio := flag.Float64("read-ratio", 0.5, "fraction of operations that are reads (0-1)")
	warmup := flag.Duration("warmup", 3*time.Second, "unmeasured warm-up before the timed run")
	flag.Parse()

	addrs := strings.Split(*nodes, ",")

	var ops int64
	var errs int64
	var latencies []time.Duration
	var latMu sync.Mutex
	var wg sync.WaitGroup

	measureStart := time.Now().Add(*warmup)
	stop := measureStart.Add(*duration)

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			c := client.New(addrs) // each worker gets its own client/leader cache
			i := 0
			for time.Now().Before(stop) {
				key := fmt.Sprintf("bench-%d-%d", worker, i%50) // small keyspace so reads hit real data
				start := time.Now()
				var err error
				if rand.Float64() < *readRatio {
					_, _, err = c.Get(key)
				} else {
					err = c.Put(key, fmt.Sprintf("v%d", i))
				}
				elapsed := time.Since(start)
				if start.Before(measureStart) {
					i++
					continue // warm-up op, not counted
				}

				atomic.AddInt64(&ops, 1)
				if err != nil {
					atomic.AddInt64(&errs, 1)
				} else {
					latMu.Lock()
					latencies = append(latencies, elapsed)
					latMu.Unlock()
				}
				i++
			}
		}(w)
	}

	wg.Wait()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	total := atomic.LoadInt64(&ops)
	failed := atomic.LoadInt64(&errs)

	fmt.Println("=== Benchmark results ===")
	fmt.Printf("Duration:        %s\n", *duration)
	fmt.Printf("Concurrency:     %d\n", *concurrency)
	fmt.Printf("Total ops:       %d\n", total)
	fmt.Printf("Failed ops:      %d\n", failed)
	fmt.Printf("Throughput:      %.1f ops/sec\n", float64(total)/duration.Seconds())
	if len(latencies) > 0 {
		fmt.Printf("Latency p50:     %s\n", percentile(latencies, 0.50))
		fmt.Printf("Latency p95:     %s\n", percentile(latencies, 0.95))
		fmt.Printf("Latency p99:     %s\n", percentile(latencies, 0.99))
		fmt.Printf("Latency max:     %s\n", latencies[len(latencies)-1])
	}
}
