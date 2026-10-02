package raft

import (
	"testing"

	pb "github.com/LIGHT-YAGAMI-61/raftkv/proto"
)

// mkLeader returns a node that is leader of term 3 in a cluster of
// 1+len(peers) nodes, with matchIndex tracking each peer at 0.
func mkLeader(t *testing.T, peers ...string) *Node {
	t.Helper()
	disableCompaction(t)
	tmpDir(t)
	n := mkNode(t, "n1", peers...)
	n.state = Leader
	n.currentTerm = 3
	n.nextIndex = make(map[string]uint64)
	n.matchIndex = make(map[string]uint64)
	for _, p := range peers {
		n.nextIndex[p] = 1
		n.matchIndex[p] = 0
	}
	return n
}

func advance(n *Node) {
	n.mu.Lock() // advanceCommitIndex expects the caller to hold n.mu
	defer n.mu.Unlock()
	n.advanceCommitIndex()
}

// Figure 8 / §5.4.2: an entry from an earlier term that is on a majority is
// still not committed by counting replicas.
func TestEntriesFromEarlierTermsAreNotCommittedByCounting(t *testing.T) {
	n := mkLeader(t, "n2", "n3")
	n.log = []*pb.LogEntry{
		putEnt(t, 1, 1, "a", "1"),
		putEnt(t, 2, 2, "b", "2"),
	}
	n.matchIndex["n2"] = 2 // leader + n2 = majority of 3, but both entries are from earlier terms

	advance(n)

	if n.commitIndex != 0 {
		t.Fatalf("commitIndex = %d, want 0 (no entry from the current term is replicated yet)", n.commitIndex)
	}
	if len(n.kvStore) != 0 {
		t.Fatalf("nothing should be applied yet, kvStore = %v", n.kvStore)
	}
}

func TestCurrentTermEntryCommitsEverythingBeforeIt(t *testing.T) {
	n := mkLeader(t, "n2", "n3")
	n.log = []*pb.LogEntry{
		putEnt(t, 1, 1, "a", "1"),
		putEnt(t, 2, 2, "b", "2"),
		noopEnt(t, 3, 3), // the no-op a new leader appends in its own term (§8)
	}
	n.matchIndex["n2"] = 3

	advance(n)

	if n.commitIndex != 3 || n.lastApplied != 3 {
		t.Fatalf("commitIndex=%d lastApplied=%d, want both 3", n.commitIndex, n.lastApplied)
	}
	if n.kvStore["a"] != "1" || n.kvStore["b"] != "2" {
		t.Fatalf("earlier-term entries were not applied: %v", n.kvStore)
	}
}

func TestNoCommitWithoutAMajority3Nodes(t *testing.T) {
	n := mkLeader(t, "n2", "n3")
	n.log = []*pb.LogEntry{putEnt(t, 1, 3, "a", "1")}

	advance(n) // only the leader has it: 1 of 3
	if n.commitIndex != 0 {
		t.Fatalf("committed with 1 of 3 nodes: commitIndex=%d", n.commitIndex)
	}

	n.matchIndex["n3"] = 1 // leader + n3 = 2 of 3
	advance(n)
	if n.commitIndex != 1 || n.kvStore["a"] != "1" {
		t.Fatalf("expected commit at 2 of 3: commitIndex=%d kvStore=%v", n.commitIndex, n.kvStore)
	}
}

func TestMajorityThresholdWithFiveNodes(t *testing.T) {
	n := mkLeader(t, "n2", "n3", "n4", "n5")
	n.log = []*pb.LogEntry{putEnt(t, 1, 3, "a", "1")}

	n.matchIndex["n2"] = 1 // 2 of 5
	advance(n)
	if n.commitIndex != 0 {
		t.Fatalf("committed with 2 of 5 nodes: commitIndex=%d", n.commitIndex)
	}

	n.matchIndex["n3"] = 1 // 3 of 5
	advance(n)
	if n.commitIndex != 1 {
		t.Fatalf("expected commit at 3 of 5: commitIndex=%d", n.commitIndex)
	}
}

func TestApplyHandlesPutDeleteAndNoop(t *testing.T) {
	n := mkLeader(t, "n2", "n3")
	n.log = []*pb.LogEntry{
		putEnt(t, 1, 3, "k", "v"),
		noopEnt(t, 2, 3),
		delEnt(t, 3, 3, "k"),
		putEnt(t, 4, 3, "z", "1"),
	}
	n.commitIndex = 4

	n.mu.Lock()
	n.applyCommitted()
	n.mu.Unlock()

	if n.lastApplied != 4 {
		t.Fatalf("lastApplied = %d, want 4", n.lastApplied)
	}
	if _, found := n.kvStore["k"]; found {
		t.Fatal("key k should have been deleted")
	}
	if n.kvStore["z"] != "1" {
		t.Fatalf("kvStore[z] = %q, want 1", n.kvStore["z"])
	}
}
