package raft

import (
	"context"
	"log"
	"time"

	pb "github.com/LIGHT-YAGAMI-61/raftkv/proto"
)

func (n *Node) SetPartition(ctx context.Context, args *pb.PartitionArgs) (*pb.ChaosAck, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blockedPeers = make(map[string]bool)
	for _, id := range args.BlockedPeerIds {
		n.blockedPeers[id] = true
	}
	log.Printf("%s: [chaos] blocking %v", n.id, args.BlockedPeerIds)
	return &pb.ChaosAck{}, nil
}

func (n *Node) SetDelay(ctx context.Context, args *pb.DelayArgs) (*pb.ChaosAck, error) {
	n.mu.Lock()
	n.artificialDelay = time.Duration(args.DelayMs) * time.Millisecond
	n.mu.Unlock()
	return &pb.ChaosAck{}, nil
}

func (n *Node) GetStatus(ctx context.Context, _ *pb.StatusRequest) (*pb.StatusReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return &pb.StatusReply{
		Id:            n.id,
		State:         n.state.String(),
		Term:          n.currentTerm,
		CommitIndex:   n.commitIndex,
		KnownLeaderId: n.leaderID,
		LastLogIndex:  n.lastLogIndex(),
	}, nil
}

