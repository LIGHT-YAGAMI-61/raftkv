package raft

import "encoding/json"

type Op string

const (
	OpPut    Op = "put"
	OpDelete Op = "delete"
	OpNoop   Op = "noop" // appended by a new leader so earlier-term entries can commit (§8)
)

type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func (c Command) Encode() ([]byte, error) { return json.Marshal(c) }

func DecodeCommand(data []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(data, &c)
	return c, err
}
