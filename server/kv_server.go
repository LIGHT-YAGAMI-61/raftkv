package server

import (
	"context"

	pb "github.com/LIGHT-YAGAMI-61/raftkv/proto"
	"github.com/LIGHT-YAGAMI-61/raftkv/raft"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVServer wraps a *raft.Node with the client-facing KV RPCs. It's
// registered on the same gRPC server as the internal Raft RPCs — both
// backed by the same Node instance, in the same process.
type KVServer struct {
	pb.UnimplementedKVServer
	node *raft.Node
}

func NewKVServer(n *raft.Node) *KVServer {
	return &KVServer{node: n}
}

func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	err := s.node.SubmitCommand(raft.Command{Op: raft.OpPut, Key: req.Key, Value: req.Value})
	if nle, ok := err.(*raft.NotLeaderError); ok && nle.LeaderHint != "" {
		return &pb.PutResponse{Ok: false, LeaderHint: nle.LeaderHint}, nil
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &pb.PutResponse{Ok: true}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	err := s.node.SubmitCommand(raft.Command{Op: raft.OpDelete, Key: req.Key})
	if nle, ok := err.(*raft.NotLeaderError); ok && nle.LeaderHint != "" {
		return &pb.DeleteResponse{Ok: false, LeaderHint: nle.LeaderHint}, nil
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

func (s *KVServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	val, ok, err := s.node.ReadIndexGet(req.Key)
	if nle, isNle := err.(*raft.NotLeaderError); isNle && nle.LeaderHint != "" {
		return &pb.GetResponse{Ok: false, LeaderHint: nle.LeaderHint}, nil
	}
	if err != nil {
		// Not "key missing": the client must see a failure and retry elsewhere.
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &pb.GetResponse{Ok: ok, Value: val}, nil
}
