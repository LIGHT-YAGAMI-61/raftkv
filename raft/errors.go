package raft

import "fmt"

// NotLeaderError carries the current known leader's address (if any) so
// callers can redirect instead of just failing.
type NotLeaderError struct {
	LeaderHint string
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("not leader, try %s", e.LeaderHint)
}
