package client

import (
	"context"
	"errors"
	"math/rand"
	"time"

	pb "raftkv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client follows leader redirects automatically: every call tries the
// last-known leader first, and on a leader_hint, retries there instead.
type Client struct {
	addrs      []string
	leaderAddr string
	conns      map[string]*grpc.ClientConn // one long-lived connection per node, reused across calls
}

func New(addrs []string) *Client {
	return &Client{addrs: addrs, leaderAddr: addrs[0], conns: make(map[string]*grpc.ClientConn)}
}

// connFor returns the cached connection for addr, dialing once if this is
// the first call to that address.
func (c *Client) connFor(addr string) (*grpc.ClientConn, error) {
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	conn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func (c *Client) Put(key, value string) error {
	return c.withRetry(func(addr string) (ok bool, hint string, err error) {
		conn, err := c.connFor(addr)
		if err != nil {
			return false, "", err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		reply, err := pb.NewKVClient(conn).Put(ctx, &pb.PutRequest{Key: key, Value: value})
		if err != nil {
			return false, "", err
		}
		return reply.Ok, reply.LeaderHint, nil
	})
}

func (c *Client) Delete(key string) error {
	return c.withRetry(func(addr string) (ok bool, hint string, err error) {
		conn, err := c.connFor(addr)
		if err != nil {
			return false, "", err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		reply, err := pb.NewKVClient(conn).Delete(ctx, &pb.DeleteRequest{Key: key})
		if err != nil {
			return false, "", err
		}
		return reply.Ok, reply.LeaderHint, nil
	})
}

func (c *Client) Get(key string) (string, bool, error) {
	var value string
	var found bool
	err := c.withRetry(func(addr string) (ok bool, hint string, err error) {
		conn, err := c.connFor(addr)
		if err != nil {
			return false, "", err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		reply, err := pb.NewKVClient(conn).Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			return false, "", err
		}
		if !reply.Ok && reply.LeaderHint != "" {
			return false, reply.LeaderHint, nil
		}
		value, found = reply.Value, reply.Ok
		return true, "", nil
	})
	return value, found, err
}

// withRetry tries the known leader, follows leader_hint redirects, and
// falls back to round-robin over every known address as a last resort.
func (c *Client) withRetry(call func(addr string) (ok bool, hint string, err error)) error {
	addr := c.leaderAddr
	tried := make(map[string]bool)

	for attempt := 0; attempt < len(c.addrs)+3; attempt++ {
		if attempt > 0 {
			backoff(attempt)
		}
		ok, hint, err := call(addr)
		if err == nil && ok {
			c.leaderAddr = addr
			return nil
		}
		tried[addr] = true
		if hint != "" && !tried[hint] {
			addr = hint
			continue
		}
		next := ""
		for _, a := range c.addrs {
			if !tried[a] {
				next = a
				break
			}
		}
		if next == "" {
			return errors.New("could not reach a leader on any known node")
		}
		addr = next
	}
	return errors.New("exceeded retry attempts")
}

// backoff waits a short, randomized delay before a retry. Capped and
// jittered so many workers retrying after the same failure (e.g. a leader
// election) don't all hit the cluster again in the same instant.
func backoff(attempt int) {
	base := 20 * time.Millisecond * time.Duration(attempt)
	if base > 200*time.Millisecond {
		base = 200 * time.Millisecond
	}
	jitter := time.Duration(rand.Int63n(int64(base) + 1))
	time.Sleep(base/2 + jitter/2)
}
