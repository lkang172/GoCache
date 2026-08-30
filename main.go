package main

import (
	pb "GoCache/raftpb"
	"flag"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
)

func (r *Raft) StartServer(port string) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	
	pb.RegisterRaftServer(grpcServer, r)

	log.Printf("Node %d listening on %s", r.id, port)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func main() {
    nodeID := flag.Int("id", 0, "Node ID")
    port := flag.String("port", ":50051", "gRPC port")
    httpAddr := flag.String("http", ":8080", "HTTP port")
    flag.Parse()

    cache := NewLRUCache(1024)

    r := &Raft{
        id:        *nodeID,
        role:      2,
        votedFor:  -1,
        peers:     make(map[int]pb.RaftClient),
        cache:     cache,
        log:       []*pb.LogEntry{{Term: 0}}, // dummy entry at index 0
        applyCh:   make(chan struct{}, 1),
        commitCh:  make(chan chan error, 1024),
        lastHeard: time.Now(),
    }

    peerAddrs := map[int]string{
        0: "localhost:50051",
        1: "localhost:50052",
        2: "localhost:50053",
        3: "localhost:50054",
        4: "localhost:50055",
    }

    go r.ticker()
    go r.applyCommitted()
    go cache.cleanup()

    r.connectToPeers(peerAddrs)

    go r.StartHTTP(*httpAddr)

    r.StartServer(*port)
}