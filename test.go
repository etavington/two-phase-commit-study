package main

import (
	"context"
	"errors"
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

func (s *server) CreateAccount(ctx context.Context, request *pb.CreateAccountRequest) (*pb.Response, error) {
	account_id := int(request.GetAccountId())
	_, ok := mykafka.QueryAccount(account_id)
	if ok {
		fmt.Println("Account already exists")
		return nil, errors.New("account already exists")
	} else {
		mykafka.SendPayment(account_id, 100)
		return &pb.Response{Msg: "create account successfully"}, nil
	}
}
func (s *server) ReadAccount(ctx context.Context, request *pb.ReadAccountRequest) (*pb.Response, error) {
	account_id := int(request.GetAccountId())
	balance, ok := mykafka.QueryAccount(account_id)
	if ok {
		fstr := fmt.Sprintf("%d's balance is %d\n", account_id, balance)
		fmt.Println(fstr)
		return &pb.Response{Msg: fstr}, nil
	} else {
		fmt.Println("Account doesn't exist")
		return nil, errors.New("create account successfully")
	}
}

func (s *server) UpdateAccountRequest(ctx context.Context, request *pb.UpdateAccountRequest) (*pb.Response, error) {
	account_id := int(request.GetAccountId())
	amount := int(request.GetAmount())
	if amount < 0 {
		fmt.Println("Amount should be positive")
		return nil, errors.New("amount should be positive")
	}
	balance, ok := mykafka.QueryAccount(account_id)
	if ok {
		delta := -int(balance) + amount
		mykafka.SendPayment(account_id, delta)
		return &pb.Response{Msg: "update account successfully"}, nil
	} else {
		fmt.Println("Account doesn't exist")
		return nil, errors.New("account doesn't exist")
	}
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
