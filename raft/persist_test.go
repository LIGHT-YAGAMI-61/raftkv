package raft

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/LIGHT-YAGAMI-61/raftkv/proto"
)

func fiveEntries(t *testing.T) []*pb.LogEntry {
	t.Helper()
	return []*pb.LogEntry{
		putEnt(t, 1, 1, "k1", "v1"),
		putEnt(t, 2, 1, "k2", "v2"),
		putEnt(t, 3, 1, "k3", "v3"),
		putEnt(t, 4, 1, "k4", "v4"),
		putEnt(t, 5, 1, "k5", "v5"),
	}
}

func TestPersistRoundTrip(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.currentTerm = 7
	n.votedFor = "n2"
	n.log = []*pb.LogEntry{putEnt(t, 1, 1, "a", "1"), putEnt(t, 2, 2, "b", "2")}
	n.persist()

	r := reboot(n)
	if r.currentTerm != 7 || r.votedFor != "n2" {
		t.Fatalf("hard state after restart: term=%d votedFor=%q, want 7 and n2", r.currentTerm, r.votedFor)
	}
	assertLog(t, r.log, n.log)
}

func TestAppendLogWritesNewEntriesOnly(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.log = fiveEntries(t)[:2]
	n.persist()
	if n.walCount != 2 {
		t.Fatalf("walCount after persist = %d, want 2", n.walCount)
	}

	n.log = append(n.log, putEnt(t, 3, 1, "k3", "v3"), putEnt(t, 4, 1, "k4", "v4"))
	n.appendLog()
	if n.walCount != 4 {
		t.Fatalf("walCount after appendLog = %d, want 4", n.walCount)
	}

	data, err := os.ReadFile(n.logFile())
	if err != nil {
		t.Fatal(err)
	}
	if lines := bytes.Count(data, []byte("\n")); lines != 4 {
		t.Fatalf("log file has %d lines, want 4", lines)
	}
	assertLog(t, reboot(n).log, n.log)
}

func TestAppendLogWithNothingNewLeavesFileAlone(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.log = fiveEntries(t)[:3]
	n.persist()

	before, err := os.ReadFile(n.logFile())
	if err != nil {
		t.Fatal(err)
	}
	n.appendLog()
	after, err := os.ReadFile(n.logFile())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("appendLog changed the file although no entries were added")
	}
}

func TestTornLastLineIsDroppedAndLaterAppendsAreClean(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.log = fiveEntries(t)[:3]
	n.persist()

	// A crash in the middle of an append leaves a partial last line with no newline.
	f, err := os.OpenFile(n.logFile(), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"term":1,"ind`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r := reboot(n)
	assertLog(t, r.log, n.log) // the torn entry was never complete, so it is gone

	// Later appends must not land after the garbage.
	r.log = append(r.log, putEnt(t, 4, 1, "k4", "v4"))
	r.appendLog()
	assertLog(t, reboot(n).log, r.log)
}

func TestTruncateThenAppendRewritesTheFile(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.log = fiveEntries(t)
	n.persist()

	// What AppendEntries does on a conflict at index 3 (§5.3).
	n.log = n.log[:2]
	n.walCount = -1
	n.log = append(n.log, putEnt(t, 3, 2, "x", "y"))
	n.appendLog()

	r := reboot(n)
	if len(r.log) != 3 || r.log[2].Term != 2 {
		t.Fatalf("after conflict rewrite: len=%d, want 3 with a term-2 entry at the end", len(r.log))
	}
	assertLog(t, r.log, n.log)
}

func TestAppendLogRewritesWhenTheLogShrank(t *testing.T) {
	tmpDir(t)
	n := mkNode(t, "n1", "n2", "n3")
	n.log = fiveEntries(t)
	n.persist()

	n.log = n.log[:2] // walCount is still 5, which is larger than the log
	n.appendLog()

	assertLog(t, reboot(n).log, n.log)
}

func TestLegacyLogInsideStateFileStillLoads(t *testing.T) {
	tmpDir(t)
	entries := []*pb.LogEntry{putEnt(t, 1, 1, "a", "1"), putEnt(t, 2, 2, "b", "2")}
	data, err := json.Marshal(persistentState{CurrentTerm: 3, VotedFor: "n3", LegacyLog: entries})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("data", 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("data", "n1.json"), data, 0644); err != nil {
		t.Fatal(err)
	}

	n := mkNode(t, "n1", "n2", "n3")
	if n.currentTerm != 3 || n.votedFor != "n3" {
		t.Fatalf("hard state: term=%d votedFor=%q, want 3 and n3", n.currentTerm, n.votedFor)
	}
	assertLog(t, n.log, entries)
}
