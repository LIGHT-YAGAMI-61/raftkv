package raft

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Set RAFTKV_SNAPSHOT_THRESHOLD to override (benchmarks want ~1000).
var logCompactionThreshold = func() uint64 {
	if v, err := strconv.ParseUint(os.Getenv("RAFTKV_SNAPSHOT_THRESHOLD"), 10, 64); err == nil && v > 0 {
		return v
	}
	return 5
}()

type snapshotFile struct {
	LastIncludedIndex uint64            `json:"last_included_index"`
	LastIncludedTerm  uint64            `json:"last_included_term"`
	KVStore           map[string]string `json:"kv_store"`
}

func (n *Node) snapshotPath() string {
	return filepath.Join("data", n.id+"-snapshot.json")
}

// maybeSnapshot compacts the log once it's grown past the threshold.
// Caller must hold n.mu.
func (n *Node) maybeSnapshot() {
	if n.snapshotInProgress {
		return // a compaction is already in flight; it will cover this too
	}
	if n.lastApplied <= n.lastIncludedIndex {
		return // nothing new committed since the last snapshot — avoid redundant work
	}
	if uint64(len(n.log)) < logCompactionThreshold {
		return
	}

	newIndex := n.lastApplied
	newTerm := n.termAt(n.lastApplied)
	kvCopy := make(map[string]string, len(n.kvStore))
	for k, v := range n.kvStore {
		kvCopy[k] = v
	}

	// The marshal+write+rename below is the slow part. n.mu is needed by
	// every RPC handler on this node (including the one that resets this
	// node's own election timer if it's a follower), so it's released for
	// the I/O and re-acquired before touching any node state again.
	n.snapshotInProgress = true
	n.mu.Unlock()
	ok := n.writeSnapshotFile(snapshotFile{LastIncludedIndex: newIndex, LastIncludedTerm: newTerm, KVStore: kvCopy})
	n.mu.Lock()
	n.snapshotInProgress = false

	if !ok {
		return // disk write failed — retry next time more entries accumulate
	}
	if newIndex <= n.lastIncludedIndex {
		return // superseded while unlocked (e.g. a concurrent InstallSnapshot) — safe to skip
	}

	n.log = n.log[n.sliceIndex(newIndex)+1:]
	n.lastIncludedIndex = newIndex
	n.lastIncludedTerm = newTerm
	n.persist()

	log.Printf("%s: compacted log, snapshot through index %d", n.id, n.lastIncludedIndex)
}

func (n *Node) persistSnapshot(lastIncludedIndex, lastIncludedTerm uint64) bool {
	return n.writeSnapshotFile(snapshotFile{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		KVStore:           n.kvStore,
	})
}

// writeSnapshotFile does the actual disk I/O for a fully-built snapshot.
// Callers may hold n.mu or not — this touches only its snap argument,
// never n's fields directly, so it's safe to call with the lock released.
func (n *Node) writeSnapshotFile(snap snapshotFile) bool {
	data, err := json.Marshal(snap)
	if err != nil {
		log.Printf("%s: marshal snapshot failed: %v", n.id, err)
		return false
	}
	if err := os.MkdirAll(filepath.Dir(n.snapshotPath()), 0755); err != nil {
		log.Printf("%s: create data dir failed: %v", n.id, err)
		return false
	}
	tmp := n.snapshotPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		log.Printf("%s: write temp snapshot failed: %v", n.id, err)
		return false
	}
	for attempt := 0; attempt < 5; attempt++ {
		if err = os.Rename(tmp, n.snapshotPath()); err == nil {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	log.Printf("%s: rename snapshot failed after retries: %v", n.id, err)
	return false
}

// loadSnapshot restores snapshot state at startup, before loadPersisted
// runs. No lock needed — called only from NewNode.
func (n *Node) loadSnapshot() {
	data, err := os.ReadFile(n.snapshotPath())
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		log.Fatalf("%s: read snapshot: %v", n.id, err)
	}

	var snap snapshotFile
	if err := json.Unmarshal(data, &snap); err != nil {
		log.Fatalf("%s: unmarshal snapshot: %v", n.id, err)
	}
	n.lastIncludedIndex = snap.LastIncludedIndex
	n.lastIncludedTerm = snap.LastIncludedTerm
	n.kvStore = snap.KVStore
	if n.kvStore == nil {
		n.kvStore = make(map[string]string)
	}
	n.commitIndex = snap.LastIncludedIndex
	n.lastApplied = snap.LastIncludedIndex
}

// buildSnapshotData serializes the current applied state for sending to a
// lagging follower via InstallSnapshot. Caller must hold n.mu.
func (n *Node) buildSnapshotData() []byte {
	data, err := json.Marshal(n.kvStore)
	if err != nil {
		log.Fatalf("%s: marshal snapshot data: %v", n.id, err)
	}
	return data
}
