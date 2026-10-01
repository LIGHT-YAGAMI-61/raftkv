package raft

import "log"

// advanceCommitIndex is the leader's commit rule (§5.3, §5.4.2).
// Caller must hold n.mu. Only counts replicas for entries from the
// leader's OWN current term — see the note above for why.
func (n *Node) advanceCommitIndex() {
	for N := n.lastLogIndex(); N > n.commitIndex; N-- {
		if n.termAt(N) != n.currentTerm {
			continue
		}
		count := 1 // leader counts itself
		for _, m := range n.matchIndex {
			if m >= N {
				count++
			}
		}
		if count*2 > len(n.cluster.Peers) {
			n.commitIndex = N
			break
		}
	}
	n.applyCommitted()
}

// applyCommitted stands in for the real state-machine apply step that
// Phase 6 adds. Caller must hold n.mu.
func (n *Node) applyCommitted() {
	start := n.lastApplied
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.sliceIndex(n.lastApplied)]

		cmd, err := DecodeCommand(entry.Command)
		if err != nil {
			log.Printf("%s: entry %d not a valid command, skipping: %v", n.id, entry.Index, err)
			continue
		}
		switch cmd.Op {
		case OpPut:
			n.kvStore[cmd.Key] = cmd.Value
		case OpDelete:
			delete(n.kvStore, cmd.Key)
		}
	}
	// One summary line per batch instead of one console write per entry —
	// console I/O is synchronous and this runs while n.mu is held, so
	// per-entry logging was serializing every RPC handler behind it.
	if n.lastApplied > start {
		log.Printf("%s: applied entries %d..%d (commitIndex %d)", n.id, start+1, n.lastApplied, n.commitIndex)
	}
	if n.lastApplied > start {
		n.applyCond.Broadcast() // wake anyone in waitForApply before the (possibly slow) snapshot step
	}
	n.maybeSnapshot()
}
