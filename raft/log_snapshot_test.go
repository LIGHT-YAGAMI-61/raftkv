package raft

import (
	"os"
	"testing"

	pb "github.com/LIGHT-YAGAMI-61/raftkv/proto"
)

func TestLogHelpersWithNoSnapshot(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	if n.lastLogIndex() != 0 || n.lastLogTerm() != 0 {
		t.Fatalf("empty log: lastLogIndex=%d lastLogTerm=%d, want 0 and 0", n.lastLogIndex(), n.lastLogTerm())
	}

	n.log = []*pb.LogEntry{putEnt(t, 1, 1, "a", "1"), putEnt(t, 2, 1, "b", "2"), putEnt(t, 3, 2, "c", "3")}
	if n.lastLogIndex() != 3 || n.lastLogTerm() != 2 {
		t.Fatalf("lastLogIndex=%d lastLogTerm=%d, want 3 and 2", n.lastLogIndex(), n.lastLogTerm())
	}
	for index, want := range map[uint64]uint64{1: 1, 2: 1, 3: 2, 4: 0, 0: 0} {
		if got := n.termAt(index); got != want {
			t.Errorf("termAt(%d) = %d, want %d", index, got, want)
		}
	}
}

func TestLogHelpersAfterCompaction(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.lastIncludedIndex = 10
	n.lastIncludedTerm = 3

	// Empty log: everything is described by the snapshot.
	if n.lastLogIndex() != 10 || n.lastLogTerm() != 3 {
		t.Fatalf("empty log after snapshot: lastLogIndex=%d lastLogTerm=%d, want 10 and 3", n.lastLogIndex(), n.lastLogTerm())
	}

	n.log = []*pb.LogEntry{putEnt(t, 11, 3, "a", "1"), putEnt(t, 12, 4, "b", "2")}
	if n.lastLogIndex() != 12 || n.lastLogTerm() != 4 {
		t.Fatalf("lastLogIndex=%d lastLogTerm=%d, want 12 and 4", n.lastLogIndex(), n.lastLogTerm())
	}
	if got := n.sliceIndex(11); got != 0 {
		t.Fatalf("sliceIndex(11) = %d, want 0", got)
	}
	if got := n.sliceIndex(12); got != 1 {
		t.Fatalf("sliceIndex(12) = %d, want 1", got)
	}

	for index, want := range map[uint64]uint64{
		10: 3, // the snapshot boundary uses lastIncludedTerm
		11: 3,
		12: 4,
		9:  0, // compacted away: unknown
		13: 0, // beyond the log: unknown
		0:  0,
	} {
		if got := n.termAt(index); got != want {
			t.Errorf("termAt(%d) = %d, want %d", index, got, want)
		}
	}
}

func TestRestartSkipsEntriesCoveredBySnapshot(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.currentTerm = 2
	n.log = []*pb.LogEntry{
		putEnt(t, 1, 1, "a", "1"),
		putEnt(t, 2, 1, "b", "2"),
		putEnt(t, 3, 2, "c", "3"),
		putEnt(t, 4, 2, "d", "4"),
	}
	n.persist()
	if !n.writeSnapshotFile(snapshotFile{
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		KVStore:           map[string]string{"a": "1", "b": "2"},
	}) {
		t.Fatal("could not write the snapshot file")
	}

	r := reboot(n)
	if r.lastIncludedIndex != 2 || r.lastIncludedTerm != 1 {
		t.Fatalf("snapshot position: index=%d term=%d, want 2 and 1", r.lastIncludedIndex, r.lastIncludedTerm)
	}
	if r.commitIndex != 2 || r.lastApplied != 2 {
		t.Fatalf("commitIndex=%d lastApplied=%d, want both 2", r.commitIndex, r.lastApplied)
	}
	if r.kvStore["a"] != "1" || r.kvStore["b"] != "2" {
		t.Fatalf("kvStore not restored from the snapshot: %v", r.kvStore)
	}
	assertLog(t, r.log, n.log[2:]) // entries 1 and 2 are already inside the snapshot
}

func TestMaybeSnapshotCompactsTheLogAndSurvivesRestart(t *testing.T) {
	tmpDir(t)
	old := logCompactionThreshold
	logCompactionThreshold = 3
	t.Cleanup(func() { logCompactionThreshold = old })

	n := mkNode(t, "n1", "n2", "n3")
	n.currentTerm = 1
	n.log = fiveEntries(t)
	n.persist()
	n.kvStore = map[string]string{"k1": "v1", "k2": "v2", "k3": "v3", "k4": "v4", "k5": "v5"}
	n.commitIndex, n.lastApplied = 5, 5

	n.mu.Lock() // maybeSnapshot expects the caller to hold n.mu
	n.maybeSnapshot()
	n.mu.Unlock()

	if n.lastIncludedIndex != 5 || n.lastIncludedTerm != 1 {
		t.Fatalf("after compaction: lastIncludedIndex=%d lastIncludedTerm=%d, want 5 and 1", n.lastIncludedIndex, n.lastIncludedTerm)
	}
	if len(n.log) != 0 {
		t.Fatalf("log should be empty after compacting everything, has %d entries", len(n.log))
	}
	if _, err := os.Stat(n.snapshotPath()); err != nil {
		t.Fatalf("snapshot file missing: %v", err)
	}

	r := reboot(n)
	if r.lastIncludedIndex != 5 || r.kvStore["k5"] != "v5" || len(r.log) != 0 {
		t.Fatalf("after restart: lastIncludedIndex=%d kv[k5]=%q logLen=%d", r.lastIncludedIndex, r.kvStore["k5"], len(r.log))
	}
}

func TestMaybeSnapshotWaitsForTheThreshold(t *testing.T) {
	tmpDir(t)
	old := logCompactionThreshold
	logCompactionThreshold = 100
	t.Cleanup(func() { logCompactionThreshold = old })

	n := mkNode(t, "n1", "n2", "n3")
	n.log = fiveEntries(t)
	n.kvStore = map[string]string{"k1": "v1"}
	n.commitIndex, n.lastApplied = 5, 5

	n.mu.Lock()
	n.maybeSnapshot()
	n.mu.Unlock()

	if n.lastIncludedIndex != 0 || len(n.log) != 5 {
		t.Fatalf("compacted below the threshold: lastIncludedIndex=%d logLen=%d", n.lastIncludedIndex, len(n.log))
	}
}
