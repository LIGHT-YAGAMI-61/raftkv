package raft

import "log"

// After compaction, n.log only holds entries AFTER lastIncludedIndex — the
// slice is no longer 1:1 with absolute Raft log indices, so every lookup
// has to go through these helpers rather than indexing n.log directly.

func (n *Node) lastLogIndex() uint64 {
	if len(n.log) == 0 {
		return n.lastIncludedIndex
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTerm() uint64 {
	if len(n.log) == 0 {
		return n.lastIncludedTerm
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) termAt(index uint64) uint64 {
	if index == n.lastIncludedIndex {
		return n.lastIncludedTerm
	}
	if index < n.lastIncludedIndex || index > n.lastLogIndex() || index == 0 {
		return 0
	}
	pos := n.sliceIndex(index)
	if pos < 0 || pos >= len(n.log) {
		log.Printf("%s: termAt: log/snapshot index mismatch (index=%d, computed pos=%d, log len=%d, lastIncludedIndex=%d) — treating as unknown term", n.id, index, pos, len(n.log), n.lastIncludedIndex)
		return 0
	}
	return n.log[pos].Term
}

// sliceIndex converts an absolute log index into a position in n.log.
// Caller must ensure index > lastIncludedIndex.
func (n *Node) sliceIndex(index uint64) int {
	return int(index - n.lastIncludedIndex - 1)
}
