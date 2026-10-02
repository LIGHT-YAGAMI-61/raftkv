package raft

import (
	"bytes"
	"context"
	"os"
	"testing"

	pb "github.com/LIGHT-YAGAMI-61/raftkv/proto"
)

// tmpDir moves the test into a fresh temp directory. The node reads and
// writes the relative path "data/", so every test needs its own working dir.
// Tests that call it must not use t.Parallel (the working directory is global).
func tmpDir(t *testing.T) {
	t.Helper()
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(old) })
}

// mkNode builds a node whose cluster is itself plus the given peers.
func mkNode(t *testing.T, id string, peers ...string) *Node {
	t.Helper()
	cfg := ClusterConfig{Self: PeerConfig{ID: id}}
	cfg.Peers = append(cfg.Peers, PeerConfig{ID: id})
	for _, p := range peers {
		cfg.Peers = append(cfg.Peers, PeerConfig{ID: p})
	}
	return NewNode(id, cfg)
}

// reboot simulates a process restart: a new Node reading the same data dir.
func reboot(n *Node) *Node {
	return NewNode(n.id, n.cluster)
}

// disableCompaction keeps maybeSnapshot from firing in tests that don't want it.
func disableCompaction(t *testing.T) {
	t.Helper()
	old := logCompactionThreshold
	logCompactionThreshold = 1 << 40
	t.Cleanup(func() { logCompactionThreshold = old })
}

func putEnt(t *testing.T, index, term uint64, key, value string) *pb.LogEntry {
	t.Helper()
	data, err := Command{Op: OpPut, Key: key, Value: value}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return &pb.LogEntry{Index: index, Term: term, Command: data}
}

func delEnt(t *testing.T, index, term uint64, key string) *pb.LogEntry {
	t.Helper()
	data, err := Command{Op: OpDelete, Key: key}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return &pb.LogEntry{Index: index, Term: term, Command: data}
}

func noopEnt(t *testing.T, index, term uint64) *pb.LogEntry {
	t.Helper()
	data, err := Command{Op: OpNoop}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return &pb.LogEntry{Index: index, Term: term, Command: data}
}

func assertLog(t *testing.T, got, want []*pb.LogEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("log length: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Index != want[i].Index || got[i].Term != want[i].Term || !bytes.Equal(got[i].Command, want[i].Command) {
			t.Fatalf("entry %d differs: got (index %d, term %d), want (index %d, term %d)",
				i, got[i].Index, got[i].Term, want[i].Index, want[i].Term)
		}
	}
}

func callVote(t *testing.T, n *Node, term uint64, candidate string, lastIndex, lastTerm uint64) *pb.RequestVoteReply {
	t.Helper()
	reply, err := n.RequestVote(context.Background(), &pb.RequestVoteArgs{
		Term:         term,
		CandidateId:  candidate,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	return reply
}

func callAppend(t *testing.T, n *Node, args *pb.AppendEntriesArgs) *pb.AppendEntriesReply {
	t.Helper()
	reply, err := n.AppendEntries(context.Background(), args)
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	return reply
}
