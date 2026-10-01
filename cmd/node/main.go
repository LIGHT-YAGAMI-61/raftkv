package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"os"

	pb "raftkv/proto"
	"raftkv/raft"
	"raftkv/server"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func loadConfig(path string) raft.ClusterConfig {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("open config: %v", err)
	}
	defer f.Close()

	var raw struct {
		Nodes []raft.PeerConfig `json:"nodes"`
	}
	if err := json.NewDecoder(f).Decode(&raw); err != nil {
		log.Fatalf("decode config: %v", err)
	}
	return raft.ClusterConfig{Peers: raw.Nodes}
}

func main() {
	id := flag.String("id", "", "node id, e.g. node1")
	configPath := flag.String("config", "config.json", "path to cluster config")
	flag.Parse()

	cfg := loadConfig(*configPath)
	var self raft.PeerConfig
	for _, p := range cfg.Peers {
		if p.ID == *id {
			self = p
		}
	}
	if self.ID == "" {
		log.Fatalf("node id %q not found in config", *id)
	}
	cfg.Self = self

	_, port, err := net.SplitHostPort(self.Address)
	if err != nil {
		log.Fatalf("parse self address %q: %v", self.Address, err)
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	node := raft.NewNode(*id, cfg)
	pb.RegisterRaftServer(grpcServer, node)
	pb.RegisterKVServer(grpcServer, server.NewKVServer(node))
	pb.RegisterChaosControlServer(grpcServer, node)
	reflection.Register(grpcServer)

	node.ConnectPeers()
	node.Run()

	log.Printf("%s listening on %s", *id, self.Address)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
