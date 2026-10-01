package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"raftkv/client"
)

type results struct {
	mu          sync.Mutex
	putOK       int
	putFail     int
	getFail     int
	violations  []string
	lastSuccess time.Time
	longestGap  time.Duration
	start       time.Time
}

func (r *results) ackedPut() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if !r.lastSuccess.IsZero() && now.Sub(r.lastSuccess) > r.longestGap {
		r.longestGap = now.Sub(r.lastSuccess)
	}
	r.lastSuccess = now
	r.putOK++
}

func (r *results) count(counter *int) {
	r.mu.Lock()
	*counter++
	r.mu.Unlock()
}

func (r *results) violation(msg string) {
	r.mu.Lock()
	r.violations = append(r.violations, fmt.Sprintf("[+%.1fs] %s", time.Since(r.start).Seconds(), msg))
	r.mu.Unlock()
}

// One goroutine per key, so each key has a single writer and a known history.
func runKey(id int, addrs []string, stop <-chan struct{}, res *results, wg *sync.WaitGroup) {
	defer wg.Done()
	c := client.New(addrs)
	key := fmt.Sprintf("chk-%d", id)
	acked, lastSeen := 0, 0

	// check returns false if the read broke an invariant.
	check := func(val string, found bool) bool {
		if !found {
			if acked > 0 {
				res.violation(fmt.Sprintf("%s: not found, but %d was acknowledged", key, acked))
				return false
			}
			return true
		}
		n, err := strconv.Atoi(val)
		if err != nil {
			res.violation(fmt.Sprintf("%s: unparseable value %q", key, val))
			return false
		}
		switch {
		case n < acked:
			res.violation(fmt.Sprintf("%s: read %d but %d was acknowledged (lost write)", key, n, acked))
			return false
		case n > acked+1:
			res.violation(fmt.Sprintf("%s: read %d, ahead of anything written (last acked %d)", key, n, acked))
			return false
		case n < lastSeen:
			res.violation(fmt.Sprintf("%s: read %d after already seeing %d (went backwards)", key, n, lastSeen))
			return false
		}
		lastSeen = n
		return true
	}

	for {
		select {
		case <-stop:
			// Final verify: wait for the cluster to heal, then read once.
			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				val, found, err := c.Get(key)
				if err == nil {
					check(val, found)
					return
				}
				time.Sleep(500 * time.Millisecond)
			}
			res.violation(fmt.Sprintf("%s: no successful read within 30s of the end", key))
			return
		default:
		}

		// A failed put is retried with the same value; it may already have committed.
		if err := c.Put(key, strconv.Itoa(acked+1)); err != nil {
			res.count(&res.putFail)
			time.Sleep(100 * time.Millisecond)
		} else {
			acked++
			res.ackedPut()
		}

		val, found, err := c.Get(key)
		if err != nil {
			res.count(&res.getFail)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		check(val, found)
	}
}

func main() {
	nodes := flag.String("nodes", "node1:5001,node2:5001,node3:5001", "comma-separated node addresses")
	numKeys := flag.Int("keys", 10, "number of keys, one writer goroutine each")
	duration := flag.Duration("duration", 60*time.Second, "how long to keep writing")
	flag.Parse()

	addrs := strings.Split(*nodes, ",")
	res := &results{start: time.Now()}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < *numKeys; i++ {
		wg.Add(1)
		go runKey(i, addrs, stop, res, &wg)
	}
	time.Sleep(*duration)
	close(stop)
	wg.Wait()

	fmt.Printf("puts acked: %d, puts failed: %d, gets failed: %d\n", res.putOK, res.putFail, res.getFail)
	fmt.Printf("longest gap between successful writes: %v\n", res.longestGap.Round(time.Millisecond))
	fmt.Printf("started: %s, violations: %d\n", res.start.Format("15:04:05"), len(res.violations))
	if len(res.violations) == 0 && res.putOK > 0 {
		fmt.Println("RESULT: PASS")
		return
	}
	for i, v := range res.violations {
		if i == 10 {
			fmt.Printf("... and %d more\n", len(res.violations)-10)
			break
		}
		fmt.Println("VIOLATION:", v)
	}
	fmt.Println("RESULT: FAIL")
	os.Exit(1)
}
