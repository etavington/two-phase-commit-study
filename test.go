package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"

	"Twopc-cli/container"
	"Twopc-cli/mykafka"
	pb "Twopc-cli/twopcserver"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
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

func (s *server) UpdateAccount(ctx context.Context, request *pb.UpdateAccountRequest) (*pb.Response, error) {
	account_id := int(request.GetAccountId())
	amount := int(request.GetAmount())
	if amount < 0 {
		fmt.Println("Amount should be positive")
		return nil, errors.New("amount should be positive")
	}
	balance, ok := mykafka.QueryAccount(account_id)
	if ok {
		delta := -int(balance) + amount
		fmt.Println(delta)
		mykafka.SendPayment(account_id, delta)
		return &pb.Response{Msg: "update account successfully"}, nil
	} else {
		fmt.Println("Account doesn't exist")
		return nil, errors.New("account doesn't exist")
	}
}

func (s *server) DeleteAccount(ctx context.Context, request *pb.DeleteAccountRequest) (*pb.Response, error) {
	account_id := int(request.GetAccountId())
	balance, ok := mykafka.QueryAccount(account_id)
	if ok {
		err := mykafka.DeleteAccount(account_id, int(balance))
		if err != nil {
			fmt.Println(err)
			return nil, err
		} else {
			return &pb.Response{Msg: "delete account successfully"}, nil
		}
	}
	fmt.Println("Account doesn't exist")
	return nil, errors.New("account doesn't exist")
}

var txnTableGet, txnTableSet, txnTableDel = container.New2PCTxnTableAccessors()

func (s *server) BeginTransaction(ctx context.Context, request *pb.BeginTransactionRequest) (*pb.Response, error) {
	transaction_id := uint(request.GetTransactionId())
	amount := int(request.GetAmount())
	account_id := int(request.GetAccountId())

	v, ok := mykafka.QueryAccount(account_id)
	if !ok {
		fmt.Println("Account doesn't exist")
		return nil, errors.New("account doesn't exist")
	} else if v < -int64(amount) {
		fmt.Println("Not enough money")
		return nil, errors.New("not enough money")
	}

	delta := container.UserAccountChange{AccountId: account_id, Amount: amount}
	err := txnTableSet(transaction_id, delta)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	fmt.Println("begin transaction successfully")
	return &pb.Response{Msg: "begin transaction successfully"}, nil
}

func (s *server) Commit(ctx context.Context, request *pb.CommitRequest) (*pb.Response, error) {
	transaction_id := uint(request.GetTransactionId())
	delta, err := txnTableGet(transaction_id)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	if len(delta) == 1 {
		fmt.Print("commit successfully")
		mykafka.SendPayment(delta[0].AccountId, delta[0].Amount)
	} else {
		mykafka.SendPayment(delta[0].AccountId, delta[0].Amount)
		mykafka.SendPayment(delta[1].AccountId, delta[1].Amount)
	}
	txnTableDel(transaction_id)
	return &pb.Response{Msg: "commit successfully"}, nil
}
func (s *server) Abort(ctx context.Context, request *pb.AbortRequest) (*pb.Response, error) {
	transaction_id := uint(request.GetTransactionId())
	txnTableDel(transaction_id)
	return &pb.Response{Msg: "abort successfully"}, nil
}

func (s *server) Reset(ctx context.Context, e *emptypb.Empty) (*pb.Response, error) {
	ids := make([]uint64, 0, len(mykafka.Records.Map))
	for k := range mykafka.Records.Map {
		ids = append(ids, k)
	}
	for _, id := range ids {
		mykafka.DeleteAccount(int(id), int(mykafka.Records.Map[id]))
	}
	return &pb.Response{Msg: "reset successfully"}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	go mykafka.Push_query()
	go mykafka.Modify_map()
	pb.RegisterTwoPhaseCommitServiceServer(s, &server{})
	fmt.Printf("Server is running on port %v\n", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		fmt.Println(err)
	}
}
