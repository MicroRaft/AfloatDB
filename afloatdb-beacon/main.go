package main

import (
	"context"
	"github.com/metanet/MicroRaft/io_afloatdb_kv"
	"google.golang.org/grpc"
	"log"
	"time"
)

const (
	address = "localhost:6702"
)

func main() {
	log.Print("Hello")
	conn, grpcErr := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if grpcErr != nil {
		log.Fatalf("did not connect: %v", grpcErr)
	}
	defer conn.Close()
	c := io_afloatdb_kv.NewKVServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, putErr := c.Put(ctx, &io_afloatdb_kv.PutRequest{
		Key:   []byte("key"),
		Value: []byte("basri"),
	})

	if putErr != nil {
		log.Fatalf("Error occurred while put to server %v", putErr)
	}

	get, getErr := c.Get(ctx, &io_afloatdb_kv.GetRequest{Key: []byte("key")})

	if getErr != nil {
		log.Fatalf("Error occurred while put to server %v", getErr)
	}

	log.Printf("Get Value: %s", get.GetValue())
}
