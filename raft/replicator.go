package raft

import pb "raftkv/proto"

// runReplicator is the only goroutine that sends AppendEntries to one peer.
func (n *Node) runReplicator(peerID string, client pb.RaftClient) {
	for range n.replicateKick[peerID] {
		n.replicateToPeer(peerID, client)
	}
}

func (n *Node) kickPeer(peerID string) {
	select {
	case n.replicateKick[peerID] <- struct{}{}:
	default: // a wakeup is already pending and will cover these entries too
	}
}