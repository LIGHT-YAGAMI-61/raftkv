package raft

type PeerConfig struct {
	ID      string
	Address string // host:port
}

type ClusterConfig struct {
	Self  PeerConfig
	Peers []PeerConfig
}
