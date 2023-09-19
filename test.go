package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"Twopc-cli/mykafka"
	pb "Twopc-cli/twopcserver"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedTwoPhaseCommitServiceServer
}

func (s *server) createAccount(ctx context.Context, request *pb.CreateAccountRequest) (*pb.Response, error) {
	account_id := request.GetAccountId()
	fmt.Println("createAccount")
	mykafka.SendPayment(int(account_id), 100)
	return &pb.Response{Msg: "createdAccount"}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	go mykafka.Exec_ksql()
	pb.RegisterTwoPhaseCommitServiceServer(s, &server{})
	fmt.Printf("Server is running on port %v\n", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		fmt.Println(err)
	}
}
