package raft

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"

	pb "raftkv/proto"
)

type persistentState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
	// Only present in files written before the log got its own file.
	LegacyLog []*pb.LogEntry `json:"log,omitempty"`
}

func (n *Node) dataFile() string {
	return filepath.Join("data", n.id+".json")
}

// persist writes state to disk atomically (temp file + rename) so a crash
// mid-write leaves the old, complete file intact rather than a corrupt one.
// Caller must hold n.mu.
// Caller must hold n.mu.
func (n *Node) persist() {
	n.persistHardState()
	n.persistLog()
}

func (n *Node) persistHardState() {
	data, err := json.Marshal(persistentState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor})
	if err != nil {
		log.Fatalf("%s: marshal state: %v", n.id, err)
	}
	n.writeFileAtomic(n.dataFile(), data)
}

// func (n *Node) persistLog() {
// 	var buf bytes.Buffer
// 	for _, e := range n.log {
// 		b, err := json.Marshal(e)
// 		if err != nil {
// 			log.Fatalf("%s: marshal log entry: %v", n.id, err)
// 		}
// 		buf.Write(b)
// 		buf.WriteByte('\n')
// 	}
// 	n.writeFileAtomic(n.logFile(), buf.Bytes())
// }

func encodeLog(entries []*pb.LogEntry) []byte {
	var buf bytes.Buffer
	for _, e := range entries {
		b, err := json.Marshal(e)
		if err != nil {
			log.Fatalf("marshal log entry: %v", err)
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// Full rewrite of the log file. Caller must hold n.mu.
func (n *Node) persistLog() {
	if n.writeFileAtomic(n.logFile(), encodeLog(n.log)) {
		n.walCount = len(n.log)
	} else {
		n.walCount = -1 // file is stale; force a full rewrite next time
	}
}

// appendLog writes only the entries the file doesn't have yet. Caller must hold n.mu.
func (n *Node) appendLog() {
	if n.walCount < 0 || n.walCount > len(n.log) {
		n.persistLog() // log shrank (truncation/compaction) or file is stale
		return
	}
	if n.walCount == len(n.log) {
		return
	}
	data := encodeLog(n.log[n.walCount:])
	f, err := os.OpenFile(n.logFile(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		_, err = f.Write(data)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		log.Printf("%s: append to log file failed, rewriting it: %v", n.id, err)
		n.persistLog() // a failed append may have left a partial line
		return
	}
	n.walCount = len(n.log)
}

// Temp file + rename, so a crash leaves the old complete file intact.
func (n *Node) writeFileAtomic(path string, data []byte) bool {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		log.Fatalf("%s: create data dir: %v", n.id, err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		log.Printf("%s: write temp state failed, will retry next call: %v", n.id, err)
		return false
	}
	// Windows Defender/indexing can transiently lock a just-written file;
	// a short retry loop rides out that window instead of crashing the node.
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if err = os.Rename(tmp, path); err == nil {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	log.Printf("%s: rename %s failed after retries, will retry next call: %v", n.id, path, err)
	return false
}

// loadPersisted reads state at startup, if a file exists. Called only from
// NewNode before the node is shared across goroutines, so no lock needed.
func (n *Node) loadPersisted() {
	data, err := os.ReadFile(n.dataFile())
	if os.IsNotExist(err) {
		return // first-ever startup
	}
	if err != nil {
		log.Fatalf("%s: read state file: %v", n.id, err)
	}

	var state persistentState
	if err := json.Unmarshal(data, &state); err != nil {
		log.Fatalf("%s: unmarshal state file: %v", n.id, err)
	}
	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor
	n.log = n.loadLogFile(state.LegacyLog)
}

func (n *Node) loadLogFile(legacy []*pb.LogEntry) []*pb.LogEntry {
	f, err := os.Open(n.logFile())
	if os.IsNotExist(err) {
		return legacy // data directory from before the split
	}
	if err != nil {
		log.Fatalf("%s: open log file: %v", n.id, err)
	}
	defer f.Close()

	var entries []*pb.LogEntry
	torn := false
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20) // default 64KB line limit is too small for large commands
	for sc.Scan() {
		e := &pb.LogEntry{}
		if err := json.Unmarshal(sc.Bytes(), e); err != nil {
			if sc.Scan() {
				log.Fatalf("%s: corrupt log entry before end of file: %v", n.id, err)
			}
			// A crash mid-append can leave a partial last line. Drop it; it was never
			// completely written, so it was not acknowledged from this node's disk.
			log.Printf("%s: dropping torn last log entry: %v", n.id, err)
			torn = true
			break
		}
		entries = append(entries, e)
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("%s: read log file: %v", n.id, err)
	}
	n.walCount = len(entries)
	if torn {
		// Rewrite cleanly so later appends don't land after the garbage line.
		if !n.writeFileAtomic(n.logFile(), encodeLog(entries)) {
			n.walCount = -1
		}
	}
	return entries
}

func (n *Node) logFile() string {
	return filepath.Join("data", n.id+"-wal.jsonl")
}
