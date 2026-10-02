package raft

import (
	"context"
	"testing"
	"time"

	pb "github.com/LIGHT-YAGAMI-61/raftkv/proto"
)

// ---- RequestVote (§5.2, §5.4.1) ----

func TestAtMostOneVotePerTerm(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")

	if !callVote(t, n, 1, "n2", 0, 0).VoteGranted {
		t.Fatal("first candidate in term 1 should get the vote")
	}
	if callVote(t, n, 1, "n3", 0, 0).VoteGranted {
		t.Fatal("a second candidate in the same term must be refused")
	}
	if !callVote(t, n, 1, "n2", 0, 0).VoteGranted {
		t.Fatal("asking again from the same candidate should still be granted")
	}
	if !callVote(t, n, 2, "n3", 0, 0).VoteGranted {
		t.Fatal("a new term resets the vote")
	}
}

func TestRequestVoteRefusesStaleTerm(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.currentTerm = 5

	reply := callVote(t, n, 4, "n2", 0, 0)
	if reply.VoteGranted {
		t.Fatal("granted a vote to a candidate from an older term")
	}
	if reply.Term != 5 {
		t.Fatalf("reply term = %d, want 5", reply.Term)
	}
}

// §5.4.1: only a candidate whose log is at least as up to date may win.
func TestRequestVoteChecksCandidateLogIsUpToDate(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.log = []*pb.LogEntry{
		putEnt(t, 1, 1, "a", "1"),
		putEnt(t, 2, 2, "b", "2"),
		putEnt(t, 3, 2, "c", "3"),
	}

	cases := []struct {
		name      string
		term      uint64
		lastIndex uint64
		lastTerm  uint64
		want      bool
	}{
		{"older last term", 1, 3, 1, false},
		{"same last term but shorter log", 2, 2, 2, false},
		{"newer last term wins even with a shorter log", 3, 1, 3, true},
		{"same last term and same length", 4, 3, 2, true},
	}
	for _, c := range cases {
		// Each case uses a higher term so an earlier vote doesn't interfere.
		if got := callVote(t, n, c.term, "n2", c.lastIndex, c.lastTerm).VoteGranted; got != c.want {
			t.Errorf("%s: VoteGranted = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestVoteSurvivesARestart(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	if !callVote(t, n, 4, "n2", 0, 0).VoteGranted {
		t.Fatal("vote should have been granted")
	}

	r := reboot(n)
	if r.currentTerm != 4 || r.votedFor != "n2" {
		t.Fatalf("after restart: term=%d votedFor=%q, want 4 and n2", r.currentTerm, r.votedFor)
	}
	if callVote(t, r, 4, "n3", 0, 0).VoteGranted {
		t.Fatal("a restarted node voted twice in the same term")
	}
	if !callVote(t, r, 4, "n2", 0, 0).VoteGranted {
		t.Fatal("the original candidate should still get the vote")
	}
}

// ---- PreVote (thesis §9.6) ----

func callPreVote(t *testing.T, n *Node, term uint64) *pb.PreVoteReply {
	t.Helper()
	reply, err := n.PreVote(context.Background(), &pb.PreVoteArgs{Term: term, CandidateId: "n2"})
	if err != nil {
		t.Fatalf("PreVote: %v", err)
	}
	return reply
}

func TestPreVoteChangesNoState(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")

	if !callPreVote(t, n, 1).VoteGranted {
		t.Fatal("a node with no recent leader should grant a pre-vote")
	}
	if n.currentTerm != 0 || n.votedFor != "" {
		t.Fatalf("PreVote changed state: term=%d votedFor=%q", n.currentTerm, n.votedFor)
	}
}

func TestPreVoteRefusedWhileALeaderIsAlive(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.lastLeaderContact = time.Now() // heard from a leader just now

	if callPreVote(t, n, 1).VoteGranted {
		t.Fatal("pre-vote granted although a leader was heard from recently")
	}
}

func TestPreVoteRefusedByTheLeaderItself(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.state = Leader

	if callPreVote(t, n, 1).VoteGranted {
		t.Fatal("a leader must not grant a pre-vote")
	}
}

func TestPreVoteRefusedForNonHigherTerm(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.currentTerm = 5

	if callPreVote(t, n, 5).VoteGranted {
		t.Fatal("pre-vote granted for a term that is not higher than ours")
	}
}

// ---- AppendEntries (§5.3) ----

func TestAppendEntriesRejectsMissingPreviousEntry(t *testing.T) {
	tmpDir(t)
	disableCompaction(t)
	n := mkNode(t, "n1", "n2", "n3")

	reply := callAppend(t, n, &pb.AppendEntriesArgs{Term: 1, LeaderId: "n2", PrevLogIndex: 5, PrevLogTerm: 1})
	if reply.Success {
		t.Fatal("accepted an append although the previous entry is missing")
	}
	if reply.Term != 1 {
		t.Fatalf("reply term = %d, want 1", reply.Term)
	}
}

func TestAppendEntriesRejectsMismatchedPreviousTerm(t *testing.T) {
	tmpDir(t)
	disableCompaction(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.currentTerm = 1
	n.log = []*pb.LogEntry{putEnt(t, 1, 1, "a", "1")}

	reply := callAppend(t, n, &pb.AppendEntriesArgs{Term: 2, LeaderId: "n2", PrevLogIndex: 1, PrevLogTerm: 2})
	if reply.Success {
		t.Fatal("accepted an append whose previous entry has a different term")
	}
	if len(n.log) != 1 {
		t.Fatalf("log changed on a rejected append: len=%d", len(n.log))
	}
}

func TestAppendEntriesTruncatesAConflictingSuffix(t *testing.T) {
	tmpDir(t)
	disableCompaction(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.currentTerm = 1
	n.log = []*pb.LogEntry{
		putEnt(t, 1, 1, "a", "1"),
		putEnt(t, 2, 1, "b", "old"),
		putEnt(t, 3, 1, "c", "old"),
	}
	n.persist()

	// The new leader (term 2) has a different entry at index 2.
	reply := callAppend(t, n, &pb.AppendEntriesArgs{
		Term:         2,
		LeaderId:     "n2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []*pb.LogEntry{putEnt(t, 2, 2, "b", "new")},
	})
	if !reply.Success {
		t.Fatal("append with a matching previous entry was rejected")
	}

	want := []*pb.LogEntry{putEnt(t, 1, 1, "a", "1"), putEnt(t, 2, 2, "b", "new")}
	assertLog(t, n.log, want) // entry 3 is gone with the conflicting suffix
	assertLog(t, reboot(n).log, want) // and the file agrees with memory
}

func TestAppendEntriesIgnoresDuplicates(t *testing.T) {
	tmpDir(t)
	disableCompaction(t)
	n := mkNode(t, "n1", "n2", "n3")
	args := &pb.AppendEntriesArgs{
		Term:     1,
		LeaderId: "n2",
		Entries:  []*pb.LogEntry{putEnt(t, 1, 1, "a", "1"), putEnt(t, 2, 1, "b", "2")},
	}

	for i := 0; i < 2; i++ {
		if !callAppend(t, n, args).Success {
			t.Fatalf("append #%d was rejected", i+1)
		}
	}
	if len(n.log) != 2 {
		t.Fatalf("duplicate delivery changed the log: len=%d, want 2", len(n.log))
	}
}

func TestAppendEntriesRefusesAStaleLeader(t *testing.T) {
	tmpDir(t)
	disableCompaction(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.currentTerm = 5
	n.log = []*pb.LogEntry{putEnt(t, 1, 4, "a", "1")}

	reply := callAppend(t, n, &pb.AppendEntriesArgs{
		Term:     4,
		LeaderId: "n2",
		Entries:  []*pb.LogEntry{putEnt(t, 2, 4, "b", "2")},
	})
	if reply.Success {
		t.Fatal("accepted an append from a leader of an older term")
	}
	if reply.Term != 5 {
		t.Fatalf("reply term = %d, want 5", reply.Term)
	}
	if len(n.log) != 1 {
		t.Fatalf("log changed on a stale append: len=%d", len(n.log))
	}
}

func TestAppendEntriesAdvancesCommitAndApplies(t *testing.T) {
	tmpDir(t)
	disableCompaction(t)
	n := mkNode(t, "n1", "n2", "n3")

	reply := callAppend(t, n, &pb.AppendEntriesArgs{
		Term:         1,
		LeaderId:     "n2",
		Entries:      []*pb.LogEntry{putEnt(t, 1, 1, "a", "1"), putEnt(t, 2, 1, "b", "2")},
		LeaderCommit: 10, // larger than the log: must be capped at the last index
	})
	if !reply.Success {
		t.Fatal("append was rejected")
	}
	if n.commitIndex != 2 || n.lastApplied != 2 {
		t.Fatalf("commitIndex=%d lastApplied=%d, want both 2", n.commitIndex, n.lastApplied)
	}
	if n.kvStore["a"] != "1" || n.kvStore["b"] != "2" {
		t.Fatalf("entries were not applied: %v", n.kvStore)
	}
}
