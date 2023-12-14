package grpc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"Twopc-cli/mykafka"
	pb "Twopc-cli/twopcserver"

	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	pb.UnimplementedTwoPhaseCommitServiceServer
}

var lock = sync.Mutex{}

// var locks = container.Locks{}

func (s *Server) CreateAccount(ctx context.Context, request *pb.CreateAccountRequest) (*pb.Response, error) {
	account_id := int(request.GetAccountId())
	mykafka.SendPayment(account_id, 100)
	return &pb.Response{Msg: "create account successfully"}, nil
}
func (s *Server) ReadAccount(ctx context.Context, request *pb.ReadAccountRequest) (*pb.Response, error) {
	account_id := int(request.GetAccountId())
	balance, ok := mykafka.QueryAccount(account_id)
	if ok {
		fstr := fmt.Sprintf("%d's balance is %d", account_id, balance)
		return &pb.Response{Msg: fstr}, nil
	} else {
		return nil, errors.New("account doesn't exist")
	}
}

func (s *Server) UpdateAccount(ctx context.Context, request *pb.UpdateAccountRequest) (*pb.Response, error) {
	account_id := int(request.GetAccountId())
	amount := int(request.GetAmount())
	if amount < 0 {
		return nil, errors.New("amount should be positive")
	}
	balance, ok := mykafka.QueryAccount(account_id)
	if ok {
		delta := -int(balance) + amount
		mykafka.SendPayment(account_id, delta)
		return &pb.Response{Msg: "update account successfully"}, nil
	} else {
		return nil, errors.New("account doesn't exist")
	}
}

func (s *Server) DeleteAccount(ctx context.Context, request *pb.DeleteAccountRequest) (*pb.Response, error) {
	account_id := int(request.GetAccountId())
	balance, ok := mykafka.QueryAccount(account_id)
	if ok {
		err := mykafka.DeleteAccount(account_id, int(balance))
		if err != nil {
			return nil, err
		} else {
			return &pb.Response{Msg: "delete account successfully"}, nil
		}
	}
	return nil, errors.New("account doesn't exist")
}

func (s *Server) BeginTransaction(ctx context.Context, request *pb.BeginTransactionRequest) (*pb.Response, error) {
	// mykafka.KafkaLock.GetLock(request.GetUuid())
	lock.Lock()
	// defer lock.Unlock()
	amount := int(request.GetAmount())
	account_id := int(request.GetAccountId())
	// locks.Mus[account_id-1].Lock()
	// defer locks.Mus[account_id-1].Unlock()
	v, ok := mykafka.QueryAccount(account_id)
	if !ok {
		return nil, errors.New("account doesn't exist")
	} else if v < -int32(amount) {
		return nil, errors.New("not enough money")
	}

	return &pb.Response{Msg: "begin transaction successfully"}, nil
}

func (s *Server) Commit(ctx context.Context, request *pb.CommitRequest) (*pb.Response, error) {
	// defer mykafka.KafkaLock.ReleaseLock(request.GetUuid())
	// lock.Lock()
	defer lock.Unlock()
	amount := int(request.GetAmount())
	account_id := int(request.GetAccountId())
	// locks.Mus[account_id-1].Lock()
	// defer locks.Mus[account_id-1].Unlock()
	err := mykafka.SendPayment(account_id, amount)
	if err != nil {
		return nil, err
	}

	//log.Logger.Print("Commit(): commit successfully")

	return &pb.Response{Msg: "commit successfully"}, nil
}
func (s *Server) Abort(ctx context.Context, request *pb.AbortRequest) (*pb.Response, error) {
	// lock.Lock()
	defer lock.Unlock()
	// account_id := int(request.GetAccountId())
	// defer locks.Mus[account_id-1].Unlock()
	return &pb.Response{Msg: "abort successfully"}, nil
}

func (s *Server) Reset(ctx context.Context, e *emptypb.Empty) (*pb.Response, error) {
	return &pb.Response{Msg: "reset successfully"}, nil
}
