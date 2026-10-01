package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"raftkv/client"
	pb "raftkv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type nodeConfig struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

func loadNodes(path string) []nodeConfig {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read config: %v", err)
	}
	var raw struct {
		Nodes []nodeConfig `json:"nodes"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		log.Fatalf("parse config: %v", err)
	}
	return raw.Nodes
}

type harness struct {
	mu      sync.Mutex
	nodes   []nodeConfig
	procs   map[string]*exec.Cmd
	alive   map[string]bool
	chaos   map[string]pb.ChaosControlClient
	binPath string
}

func newHarness(nodes []nodeConfig, binPath string) *harness {
	h := &harness{
		nodes:   nodes,
		procs:   make(map[string]*exec.Cmd),
		alive:   make(map[string]bool),
		chaos:   make(map[string]pb.ChaosControlClient),
		binPath: binPath,
	}
	for _, n := range nodes {
		conn, err := grpc.NewClient(n.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("dial %s: %v", n.ID, err)
		}
		h.chaos[n.ID] = pb.NewChaosControlClient(conn)
	}
	return h
}

func (h *harness) start(id string) {
	logFile, err := os.Create(filepath.Join("data", "logs", id+"-chaos.log"))
	if err != nil {
		log.Fatalf("create log for %s: %v", id, err)
	}
	cmd := exec.Command(h.binPath, "-id", id, "-config", "config.json")
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		log.Fatalf("start %s: %v", id, err)
	}
	h.mu.Lock()
	h.procs[id], h.alive[id] = cmd, true
	h.mu.Unlock()
}

func (h *harness) kill(id string) {
	h.mu.Lock()
	cmd, ok := h.procs[id]
	h.alive[id] = false
	h.mu.Unlock()
	if ok && cmd.Process != nil {
		cmd.Process.Kill()
		cmd.Wait()
	}
}

func (h *harness) isAlive(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.alive[id]
}

func (h *harness) aliveIDs() (ids []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, a := range h.alive {
		if a {
			ids = append(ids, id)
		}
	}
	return
}

func (h *harness) otherIDs(id string) (ids []string) {
	for _, n := range h.nodes {
		if n.ID != id {
			ids = append(ids, n.ID)
		}
	}
	return
}

func (h *harness) setPartition(id string, blocked []string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h.chaos[id].SetPartition(ctx, &pb.PartitionArgs{BlockedPeerIds: blocked})
}

func (h *harness) setDelay(id string, ms int64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h.chaos[id].SetDelay(ctx, &pb.DelayArgs{DelayMs: ms})
}

func (h *harness) healAll() {
	for _, n := range h.nodes {
		h.setPartition(n.ID, nil)
		h.setDelay(n.ID, 0)
	}
}

// chaosLoop injects one fault at a time so effects stay easy to diagnose.
func (h *harness) chaosLoop(stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-stop:
			return
		default:
		}
		switch rand.Intn(3) {
		case 0:
			id := h.nodes[rand.Intn(len(h.nodes))].ID
			others := h.otherIDs(id)
			log.Printf("[chaos] partitioning %s from %v", id, others)
			h.setPartition(id, others)
			time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)
			h.setPartition(id, nil)
			log.Printf("[chaos] healed partition on %s", id)
		case 1:
			alive := h.aliveIDs()
			if len(alive) == 0 {
				continue
			}
			id := alive[rand.Intn(len(alive))]
			log.Printf("[chaos] killing %s", id)
			h.kill(id)
			time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)
			log.Printf("[chaos] restarting %s", id)
			h.start(id)
		case 2:
			id := h.nodes[rand.Intn(len(h.nodes))].ID
			ms := int64(50 + rand.Intn(250))
			log.Printf("[chaos] delaying %s by %dms", id, ms)
			h.setDelay(id, ms)
			time.Sleep(time.Duration(1000+rand.Intn(1000)) * time.Millisecond)
			h.setDelay(id, 0)
		}
		time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)
	}
}

// invariantChecker fails fast if two nodes ever both claim leadership for
// the same term (§5.2 safety property).
func (h *harness) invariantChecker(stop <-chan struct{}, wg *sync.WaitGroup, violated *bool, mu *sync.Mutex) {
	defer wg.Done()
	termLeader := make(map[uint64]string)
	for {
		select {
		case <-stop:
			return
		default:
		}
		for _, n := range h.nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			reply, err := h.chaos[n.ID].GetStatus(ctx, &pb.StatusRequest{})
			cancel()
			if err != nil || reply.State != "Leader" {
				continue
			}
			if existing, ok := termLeader[reply.Term]; ok && existing != reply.Id {
				log.Printf("[VIOLATION] term %d has two leaders: %s and %s", reply.Term, existing, reply.Id)
				mu.Lock()
				*violated = true
				mu.Unlock()
			} else {
				termLeader[reply.Term] = reply.Id
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func main() {
	duration := 25 * time.Second
	nodes := loadNodes("config.json")
	binPath := filepath.Join("bin", "node.exe")
	os.MkdirAll(filepath.Join("data", "logs"), 0755)

	h := newHarness(nodes, binPath)
	for _, n := range nodes {
		h.start(n.ID)
	}
	log.Println("cluster started, waiting for initial leader election...")
	time.Sleep(3 * time.Second)

	var addrs []string
	for _, n := range nodes {
		addrs = append(addrs, n.Address)
	}
	kv := client.New(addrs)

	ledger := make(map[string]string)
	var ledgerMu sync.Mutex
	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go h.chaosLoop(stop, &wg)

	var violated bool
	var violatedMu sync.Mutex
	wg.Add(1)
	go h.invariantChecker(stop, &wg, &violated, &violatedMu)

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			key := fmt.Sprintf("chaos-key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if err := kv.Put(key, value); err == nil {
				ledgerMu.Lock()
				ledger[key] = value
				ledgerMu.Unlock()
			} else {
				log.Printf("[writer] put %s failed (expected during faults): %v", key, err)
			}
			i++
			time.Sleep(300 * time.Millisecond)
		}
	}()

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	log.Println("healing cluster and restarting any dead nodes...")
	for _, n := range nodes {
		if !h.isAlive(n.ID) {
			h.start(n.ID)
		}
	}
	h.healAll()
	time.Sleep(5 * time.Second)

	log.Printf("verifying %d committed writes survived...", len(ledger))
	lost := 0
	for key, want := range ledger {
		got, ok, err := kv.Get(key)
		if err != nil || !ok || got != want {
			log.Printf("[LOST] key %q: want %q, got %q (ok=%v err=%v)", key, want, got, ok, err)
			lost++
		}
	}

	fmt.Println("\n=== Chaos test summary ===")
	fmt.Printf("Writes acknowledged as committed: %d\n", len(ledger))
	fmt.Printf("Committed entries lost: %d\n", lost)
	violatedMu.Lock()
	fmt.Printf("Two-leaders-in-one-term violations: %v\n", violated)
	violatedMu.Unlock()
	if lost == 0 && !violated {
		fmt.Println("RESULT: PASS")
	} else {
		fmt.Println("RESULT: FAIL")
		os.Exit(1)
	}
}
