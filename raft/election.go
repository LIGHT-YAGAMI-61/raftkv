package raft

import (
	"context"
	"log"
	"math/rand"
	"time"

	pb "github.com/LIGHT-YAGAMI-61/raftkv/proto"
)

const maxEntriesPerRPC = 256

const heartbeatInterval = 50 * time.Millisecond

// electionTimeout returns a fresh random duration each call — randomization
// (§5.2) is what stops split votes from repeating: if every node used the
// same fixed timeout, ties in a failed election would recur indefinitely.
// Also the PreVote lease window: a follower refuses pre-votes for this long after last hearing from a leader.
const minElectionTimeout = 300 * time.Millisecond

func electionTimeout() time.Duration {
	return minElectionTimeout + time.Duration(rand.Int63n(int64(300*time.Millisecond)))
}

// Run starts the node's background election timer. Call once after
// ConnectPeers, before serving.
func (n *Node) Run() {
	go n.runElectionTimer()
	go n.runBatcher()
	for peerID, client := range n.peers {
		go n.runReplicator(peerID, client)
	}
}

// runElectionTimer polls every 10ms instead of using a resettable timer —
// simpler to get right, at the cost of up to 10ms slack, negligible next
// to a 150-300ms window.
func (n *Node) runElectionTimer() {
	timeout := electionTimeout()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		state := n.state
		elapsed := time.Since(n.lastHeartbeat)
		n.mu.Unlock()

		if state == Leader {
			continue
		}
		if elapsed >= timeout {
			if n.preVote() {
				n.startElection()
			}
			timeout = electionTimeout() // re-arm with a fresh random window
		}
	}
}

// preVote polls peers for a would-you-vote-for-me answer at currentTerm+1
// without changing any term (§9.6). Blocks up to ~100ms, which is fine
// because only the timer goroutine calls it.
func (n *Node) preVote() bool {
	n.mu.Lock()
	nextTerm := n.currentTerm + 1
	lastLogIndex := n.lastLogIndex()
	lastLogTerm := n.lastLogTerm()
	n.mu.Unlock()

	need := len(n.cluster.Peers)/2 + 1
	granted := 1 // self
	ch := make(chan bool, len(n.peers))
	for peerID, client := range n.peers {
		go func(peerID string, client pb.RaftClient) {
			if n.isBlocked(peerID) {
				ch <- false
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			reply, err := client.PreVote(ctx, &pb.PreVoteArgs{
				Term:         nextTerm,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				ch <- false
				return
			}
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				n.persistHardState()
			}
			n.mu.Unlock()
			ch <- reply.VoteGranted
		}(peerID, client)
	}
	for range n.peers {
		if <-ch {
			granted++
		}
		if granted >= need {
			break
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	// Re-check: a leader may have reached us while we were waiting on replies.
	if granted >= need && time.Since(n.lastLeaderContact) >= minElectionTimeout {
		return true
	}
	n.lastHeartbeat = time.Now() // back off a full timeout window before the next attempt
	log.Printf("%s: pre-vote failed for term %d, staying follower", n.id, nextTerm)
	return false
}

// startElection is the candidate side of §5.2: bump term, vote for self,
// fan out RequestVote to every peer concurrently.
func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	savedTerm := n.currentTerm
	n.votedFor = n.id
	n.lastHeartbeat = time.Now()
	n.persistHardState()
	lastLogIndex := n.lastLogIndex()
	lastLogTerm := n.lastLogTerm()
	log.Printf("%s: starting election for term %d", n.id, savedTerm)
	n.mu.Unlock()

	votes := 1 // vote for self; all further access happens under n.mu

	for peerID, client := range n.peers {
		go func(peerID string, client pb.RaftClient) {
			if n.isBlocked(peerID) {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			reply, err := client.RequestVote(ctx, &pb.RequestVoteArgs{
				Term:         savedTerm,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				n.persistHardState()
				return
			}
			if n.state != Candidate || n.currentTerm != savedTerm || !reply.VoteGranted {
				return
			}

			votes++
			if votes*2 > len(n.cluster.Peers) {
				n.becomeLeader()
			}
		}(peerID, client)
	}
}

// leaderHeartbeats sends empty AppendEntries on a fixed interval (§5.2) —
// this is what suppresses followers' election timers.
func (n *Node) leaderHeartbeats() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		for peerID := range n.peers {
			n.kickPeer(peerID)
		}

		<-ticker.C
	}
}

// replicateToPeer sends whatever entries the peer is missing, per
// nextIndex. An empty entries slice doubles as the heartbeat (§5.2).
// Backtracking on rejection is the simple one-at-a-time version (§5.3);
// the paper's faster conflict-index optimization is skipped for now.
func (n *Node) replicateToPeer(peerID string, client pb.RaftClient) {
	n.mu.Lock()
	if n.state != Leader || n.blockedPeers[peerID] {
		n.mu.Unlock()
		return
	}

	if n.nextIndex[peerID] <= n.lastIncludedIndex {
		term := n.currentTerm
		lastIncludedIndex := n.lastIncludedIndex
		lastIncludedTerm := n.lastIncludedTerm
		data := n.buildSnapshotData()
		n.mu.Unlock()
		n.sendSnapshot(peerID, client, term, lastIncludedIndex, lastIncludedTerm, data)
		return
	}

	term := n.currentTerm
	next := n.nextIndex[peerID]
	prevLogIndex := next - 1
	prevLogTerm := n.termAt(prevLogIndex)

	start := n.sliceIndex(prevLogIndex + 1)
	if start < 0 {
		start = 0
	}
	if start > len(n.log) {
		start = len(n.log)
	}
	end := len(n.log)
	if end-start > maxEntriesPerRPC {
		end = start + maxEntriesPerRPC // cap round size for a far-behind follower; it'll catch up over several rounds
	}
	entries := append([]*pb.LogEntry{}, n.log[start:end]...)
	leaderCommit := n.commitIndex
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	reply, err := client.AppendEntries(ctx, &pb.AppendEntriesArgs{
		Term:         term,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	})
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		n.persistHardState()
		return
	}
	if n.state != Leader || term != n.currentTerm {
		return
	}

	if reply.Success {
		// Overlapping calls can return out of order; never move matchIndex backwards.
		if newMatch := prevLogIndex + uint64(len(entries)); newMatch > n.matchIndex[peerID] {
			n.matchIndex[peerID] = newMatch
			n.nextIndex[peerID] = newMatch + 1
		}
		n.advanceCommitIndex()
	} else if n.nextIndex[peerID] > 1 {
		n.nextIndex[peerID]--
	}
}

func (n *Node) sendSnapshot(peerID string, client pb.RaftClient, term, lastIncludedIndex, lastIncludedTerm uint64, data []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	reply, err := client.InstallSnapshot(ctx, &pb.InstallSnapshotArgs{
		Term:              term,
		LeaderId:          n.id,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	})
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		n.persistHardState()
		return
	}
	if n.state != Leader || term != n.currentTerm {
		return
	}
	n.matchIndex[peerID] = lastIncludedIndex
	n.nextIndex[peerID] = lastIncludedIndex + 1
}
