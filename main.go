package main

import (
	"fmt"
	"log"
	"net"

	mygrpc "Twopc-cli/grpc"
	"Twopc-cli/mykafka"
	pb "Twopc-cli/twopcserver"

	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	go mykafka.Push_query()
	go mykafka.Modify_map()
	pb.RegisterTwoPhaseCommitServiceServer(s, &mygrpc.Server{})
	fmt.Printf("Server is running on port %v\n", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		fmt.Println(err)
	}
}
