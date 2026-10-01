package raft

import (
	"context"
	"errors"
	"time"

	pb "raftkv/proto"
)

// SubmitCommand appends a Put/Delete to the log (only the leader may do
// this — §5.1) and blocks until it's actually applied, so the caller gets
// a real "this write is durable and visible" guarantee, not just "queued".
func (n *Node) SubmitCommand(cmd Command) error {
	data, err := cmd.Encode()
	if err != nil {
		return err
	}
	req := &submitReq{data: data, resp: make(chan submitResp, 1)}
	n.submitCh <- req // blocks when the queue is full, which gives natural backpressure
	r := <-req.resp
	if r.err != nil {
		return r.err
	}
	return n.waitForApply(r.index, r.term)
}

// waitForApply polls (same simple pattern as the election timer) until the
// entry is applied, or bails out if this node stops being leader for that
// term — the write's fate elsewhere is unknown, so the client should retry.
func (n *Node) waitForApply(index, term uint64) error {
	deadline := time.Now().Add(2 * time.Second)
	// One-shot wake so a stuck waiter can't block forever on Cond.Wait.
	timer := time.AfterFunc(2*time.Second, func() {
		n.mu.Lock()
		n.applyCond.Broadcast()
		n.mu.Unlock()
	})
	defer timer.Stop()

	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		if n.currentTerm != term || n.state != Leader {
			return errors.New("leadership changed before entry committed")
		}
		if n.lastApplied >= index {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for entry to commit")
		}
		n.applyCond.Wait() // releases n.mu while blocked, reacquires before returning
	}
}

// ReadIndexGet implements linearizable reads (§8). A plain local read
// isn't safe on its own — a newer leader could already exist elsewhere.
// Confirming leadership with a live majority first rules that out.
func (n *Node) ReadIndexGet(key string) (string, bool, error) {
	n.mu.Lock()
	if n.state != Leader {
		hint := n.leaderAddress()
		n.mu.Unlock()
		return "", false, &NotLeaderError{LeaderHint: hint}
	}
	term := n.currentTerm
	readIndex := n.commitIndex
	if n.noopIndex > readIndex {
		readIndex = n.noopIndex // no entry from this term committed yet; don't serve reads (§8)
	}
	peers := make([]pb.RaftClient, 0, len(n.peers))
	for _, c := range n.peers {
		peers = append(peers, c)
	}
	n.mu.Unlock()

	if !n.confirmLeadership(term, peers) {
		return "", false, errors.New("could not confirm leadership with a majority")
	}
	if err := n.waitForApply(readIndex, term); err != nil {
		return "", false, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader || n.currentTerm != term {
		return "", false, errors.New("leadership changed during read")
	}
	val, ok := n.kvStore[key]
	return val, ok, nil
}

// confirmLeadership sends a bare heartbeat to every peer and waits for a
// majority to acknowledge it under this exact term.
func (n *Node) confirmLeadership(term uint64, peers []pb.RaftClient) bool {
	need := len(n.cluster.Peers)/2 + 1
	acks := 1 // self
	if acks >= need {
		return true // single-node cluster
	}
	ch := make(chan bool, len(peers))
	for _, client := range peers {
		go func(client pb.RaftClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			reply, err := client.AppendEntries(ctx, &pb.AppendEntriesArgs{Term: term, LeaderId: n.id})
			ch <- err == nil && reply.Success
		}(client)
	}
	for i := 0; i < len(peers); i++ {
		if <-ch {
			acks++
			if acks >= need {
				return true // don't wait for a slow/dead peer once majority is confirmed
			}
		}
	}
	return false
}
