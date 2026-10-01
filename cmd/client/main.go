package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"raftkv/client"
)

func main() {
	nodes := flag.String("nodes", "localhost:5001,localhost:5002,localhost:5003", "comma-separated node addresses")
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		log.Fatalf("usage: client -nodes addr1,addr2,addr3 <get|put|delete> key [value]")
	}

	c := client.New(strings.Split(*nodes, ","))
	cmd, key := args[0], args[1]

	switch cmd {
	case "put":
		if len(args) < 3 {
			log.Fatalf("put requires a value")
		}
		if err := c.Put(key, args[2]); err != nil {
			log.Fatalf("put failed: %v", err)
		}
		fmt.Println("OK")
	case "get":
		val, ok, err := c.Get(key)
		if err != nil {
			log.Fatalf("get failed: %v", err)
		}
		if !ok {
			fmt.Println("(not found)")
			return
		}
		fmt.Println(val)
	case "delete":
		if err := c.Delete(key); err != nil {
			log.Fatalf("delete failed: %v", err)
		}
		fmt.Println("OK")
	default:
		log.Fatalf("unknown command %q", cmd)
	}
}
