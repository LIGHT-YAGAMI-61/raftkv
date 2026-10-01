package raft

import (
	"log"
	pb "raftkv/proto"
)

const (
	maxBatchSize  = 128
	submitBufSize = 1024
)

type submitReq struct {
	data []byte
	resp chan submitResp // buffered(1) so the batcher never blocks on a slow caller
}

type submitResp struct {
	index, term uint64
	err         error
}

// runBatcher is the only goroutine that appends client commands to the log.
// It blocks for one request, then drains whatever is already queued without
// waiting, so batches stay size 1 under light load and grow under heavy load.
func (n *Node) runBatcher() {
	for first := range n.submitCh {
		batch := []*submitReq{first}
	drain:
		for len(batch) < maxBatchSize {
			select {
			case r := <-n.submitCh:
				batch = append(batch, r)
			default:
				break drain
			}
		}
		n.appendBatch(batch)
	}
}

func (n *Node) appendBatch(batch []*submitReq) {
	log.Printf("batch size %d", len(batch))
	n.mu.Lock()
	// Leadership is checked here, at dequeue time. It may have changed while
	// these requests sat in the queue.
	if n.state != Leader {
		hint := n.leaderAddress()
		n.mu.Unlock()
		for _, r := range batch {
			r.resp <- submitResp{err: &NotLeaderError{LeaderHint: hint}}
		}
		return
	}

	term := n.currentTerm
	resps := make([]submitResp, len(batch))
	for i, r := range batch {
		entry := &pb.LogEntry{Term: term, Index: n.lastLogIndex() + 1, Command: r.data}
		n.log = append(n.log, entry)
		resps[i] = submitResp{index: entry.Index, term: term}
	}
	n.appendLog() // append only this batch's entries
	n.mu.Unlock()

	for peerID := range n.peers {
		n.kickPeer(peerID)
	}
	for i, r := range batch {
		r.resp <- resps[i]
	}
}
