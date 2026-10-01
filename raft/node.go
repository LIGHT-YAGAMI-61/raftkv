package raft

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	pb "github.com/LIGHT-YAGAMI-61/raftkv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Node struct {
	pb.UnimplementedRaftServer
	pb.UnimplementedChaosControlServer
	id      string
	cluster ClusterConfig

	mu            sync.Mutex
	state         State
	currentTerm   uint64
	votedFor      string
	lastHeartbeat time.Time
	// Only AppendEntries/InstallSnapshot from a valid leader update this; PreVote uses it as the "leader is alive" lease check.
	lastLeaderContact time.Time
	noopIndex         uint64 // index of this leader's term-start no-op; reads wait for it to apply

	log         []*pb.LogEntry
	commitIndex uint64
	lastApplied uint64
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64

	// Snapshot state (§7).
	lastIncludedIndex uint64
	lastIncludedTerm  uint64

	// kvStore is the real state machine (§5.3) — applied Put/Delete
	// commands mutate this map, and Get reads from it.
	kvStore map[string]string

	// leaderID lets a non-leader tell clients where to retry (leader_hint).
	leaderID string

	// Chaos-testing hooks (Phase 7) — application-level fault injection,
	// not a real OS/network partition (see note above).
	blockedPeers    map[string]bool
	artificialDelay time.Duration

	snapshotInProgress bool // prevents two concurrent compactions from racing on the same file

	peers map[string]pb.RaftClient

	submitCh chan *submitReq // client commands waiting for the batcher

	replicateKick map[string]chan struct{} // per-peer wakeup for its replicator

	applyCond *sync.Cond // wakes waitForApply callers when lastApplied advances, instead of polling

	walCount int // number of log entries currently in the log file
}

func NewNode(id string, cfg ClusterConfig) *Node {
	n := &Node{
		id:            id,
		cluster:       cfg,
		state:         Follower,
		lastHeartbeat: time.Now(),
		peers:         make(map[string]pb.RaftClient),
		kvStore:       make(map[string]string),
		blockedPeers:  make(map[string]bool),
		submitCh:      make(chan *submitReq, submitBufSize),
		replicateKick: make(map[string]chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.loadSnapshot()
	n.loadPersisted()
	for len(n.log) > 0 && n.log[0].Index <= n.lastIncludedIndex {
		n.log = n.log[1:] // defensive: drop anything already covered by the snapshot
	}
	return n
}

// ConnectPeers dials every other node once at startup; the connection is
// reused for every RPC rather than dialing per-call.
func (n *Node) ConnectPeers() {
	for _, p := range n.cluster.Peers {
		if p.ID == n.id {
			continue
		}
		conn, err := grpc.NewClient(p.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("dial %s: %v", p.ID, err)
		}
		n.peers[p.ID] = pb.NewRaftClient(conn)
		n.replicateKick[p.ID] = make(chan struct{}, 1)
	}
}
func (n *Node) RequestVote(ctx context.Context, args *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	if n.chaosGate(args.CandidateId) {
		return nil, status.Error(codes.Unavailable, "simulated partition")
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	reply := &pb.RequestVoteReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply, nil // stale candidate
	}

	upToDate := args.LastLogTerm > n.lastLogTerm() ||
		(args.LastLogTerm == n.lastLogTerm() && args.LastLogIndex >= n.lastLogIndex())

	if (n.votedFor == "" || n.votedFor == args.CandidateId) && upToDate {
		n.votedFor = args.CandidateId
		n.lastHeartbeat = time.Now()
		reply.VoteGranted = true
	}
	n.persistHardState() // only term/votedFor can have changed here
	return reply, nil
}

// PreVote (Ongaro thesis §9.6) answers "would you vote for me?" without
// changing any state: no term bump, no votedFor, no timer reset, no persist.
func (n *Node) PreVote(ctx context.Context, args *pb.PreVoteArgs) (*pb.PreVoteReply, error) {
	if n.chaosGate(args.CandidateId) {
		return nil, status.Error(codes.Unavailable, "simulated partition")
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &pb.PreVoteReply{Term: n.currentTerm}
	if args.Term <= n.currentTerm {
		return reply, nil
	}
	// A leader never refreshes its own lastLeaderContact, hence the explicit state check.
	if n.state == Leader || time.Since(n.lastLeaderContact) < minElectionTimeout {
		return reply, nil
	}

	reply.VoteGranted = args.LastLogTerm > n.lastLogTerm() ||
		(args.LastLogTerm == n.lastLogTerm() && args.LastLogIndex >= n.lastLogIndex())
	return reply, nil
}

func (n *Node) AppendEntries(ctx context.Context, args *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	if n.chaosGate(args.LeaderId) {
		return nil, status.Error(codes.Unavailable, "simulated partition")
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
		n.persistHardState() // log untouched here, so skip the log file
	}

	reply := &pb.AppendEntriesReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply, nil
	}

	// Valid leader for this term. Real log entries land in Phase 3 —
	// this only proves liveness (heartbeat).
	n.state = Follower
	n.lastHeartbeat = time.Now()
	n.lastLeaderContact = n.lastHeartbeat
	n.leaderID = args.LeaderId

	// Log consistency check (§5.3). A PrevLogIndex older than our snapshot
	// is automatically fine — our snapshot already covers everything up to
	// lastIncludedIndex, which is stronger than any individual entry check.
	if args.PrevLogIndex > n.lastIncludedIndex {
		if args.PrevLogIndex > n.lastLogIndex() || n.termAt(args.PrevLogIndex) != args.PrevLogTerm {
			return reply, nil // success stays false — leader backs up nextIndex and retries
		}
	}

	logChanged := false
	// Append new entries, truncating on the first conflict (§5.3).
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx <= n.lastIncludedIndex {
			continue // already compacted into our snapshot
		}
		if idx <= n.lastLogIndex() {
			if n.termAt(idx) == entry.Term {
				continue // already have this exact entry
			}
			n.log = n.log[:n.sliceIndex(idx)]
			n.walCount = -1 // file still holds the entries just discarded; appendLog will rewrite it // conflict — discard this entry and everything after
		}
		n.log = append(n.log, entry)
		logChanged = true // any truncation is always followed by an append
	}

	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, n.lastLogIndex())
	}
	n.applyCommitted()
	if logChanged {
		n.appendLog() // append-only unless truncated above; still before the reply (§5.1)
	}

	reply.Success = true
	return reply, nil
}

// becomeFollower and becomeLeader assume the caller already holds n.mu.
func (n *Node) becomeFollower(term uint64) {
	log.Printf("%s: becoming follower for term %d", n.id, term)
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.lastHeartbeat = time.Now()
}

func (n *Node) becomeLeader() {
	log.Printf("%s: becoming LEADER for term %d", n.id, n.currentTerm)
	n.state = Leader
	n.leaderID = n.id
	n.nextIndex = make(map[string]uint64)
	n.matchIndex = make(map[string]uint64)
	for peerID := range n.peers {
		n.nextIndex[peerID] = n.lastLogIndex() + 1
		n.matchIndex[peerID] = 0
	}

	// §8: commit a blank entry from this term so entries left over from
	// earlier terms can commit (§5.4.2) and reads have something to wait on.
	// nextIndex was set above, so followers' first send includes the no-op.
	if data, err := (Command{Op: OpNoop}).Encode(); err == nil {
		entry := &pb.LogEntry{Term: n.currentTerm, Index: n.lastLogIndex() + 1, Command: data}
		n.log = append(n.log, entry)
		n.noopIndex = entry.Index
		n.appendLog() // log grew by the no-op; term/vote unchanged
		go func() {
			for peerID := range n.peers {
				n.kickPeer(peerID)
			}
		}()
	}
	go n.leaderHeartbeats()
}

// Submit is a temporary test entry point for injecting log entries ahead
// of the real KV API (Phase 6). Only the leader accepts writes (§5.1).
func (n *Node) Submit(ctx context.Context, args *pb.SubmitArgs) (*pb.SubmitReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return &pb.SubmitReply{Success: false}, nil // leader redirect lands in Phase 6
	}

	entry := &pb.LogEntry{
		Term:    n.currentTerm,
		Index:   n.lastLogIndex() + 1,
		Command: args.Command,
	}
	n.log = append(n.log, entry)
	n.persist()
	return &pb.SubmitReply{Success: true}, nil
}

// InstallSnapshot (§7): a leader sends this when a follower has fallen so
// far behind that the entries it needs no longer exist individually — only
// inside a snapshot. Simplified: whole snapshot in one call, no chunking.
func (n *Node) InstallSnapshot(ctx context.Context, args *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	if n.chaosGate(args.LeaderId) {
		return nil, status.Error(codes.Unavailable, "simulated partition")
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	reply := &pb.InstallSnapshotReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply, nil
	}
	n.lastHeartbeat = time.Now()
	n.lastLeaderContact = n.lastHeartbeat
	n.leaderID = args.LeaderId

	if args.LastIncludedIndex <= n.lastIncludedIndex {
		return reply, nil // stale snapshot, already ahead of this
	}

	var kv map[string]string
	if err := json.Unmarshal(args.Data, &kv); err != nil {
		log.Fatalf("%s: unmarshal snapshot: %v", n.id, err)
	}

	// Write first; touch in-memory state only once the file is safely on disk.
	if !n.writeSnapshotFile(snapshotFile{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		KVStore:           kv,
	}) {
		log.Printf("%s: failed to persist installed snapshot, ignoring it", n.id)
		return reply, nil
	}
	n.kvStore = kv
	n.log = nil // simplified: discard the whole log rather than retain matching tail
	n.commitIndex = args.LastIncludedIndex
	n.lastApplied = args.LastIncludedIndex
	n.lastIncludedIndex = args.LastIncludedIndex
	n.lastIncludedTerm = args.LastIncludedTerm
	n.persist()

	log.Printf("%s: installed snapshot through index %d", n.id, n.lastIncludedIndex)
	return reply, nil
}

func (n *Node) leaderAddress() string {
	for _, p := range n.cluster.Peers {
		if p.ID == n.leaderID {
			return p.Address
		}
	}
	return ""
}

// chaosGate applies the simulated delay under a brief lock (not held during
// the sleep itself, so one slow RPC doesn't stall the whole node), then
// reports whether senderID is currently blocked.
func (n *Node) chaosGate(senderID string) bool {
	n.mu.Lock()
	delay := n.artificialDelay
	blocked := n.blockedPeers[senderID]
	n.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	return blocked
}

func (n *Node) isBlocked(peerID string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.blockedPeers[peerID]
}
