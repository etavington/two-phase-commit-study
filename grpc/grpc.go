package grpc

import (
	"context"
	"errors"

	"Twopc-cli/GOCouchDBAPIs"
	"Twopc-cli/mykafka"
	pb "Twopc-cli/twopcserver"

	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	pb.UnimplementedTwoPhaseCommitServiceServer
}

func (s *Server) BeginTransaction(ctx context.Context, request *pb.BeginTransactionRequest) (*pb.Response, error) {
	GOCouchDBAPIs.CouchLock.GetLock(request.GetUuid())

	amount := int(request.GetAmount())
	account_id := int(request.GetAccountId())
	accout, err := GOCouchDBAPIs.ReadAccount(int32(account_id), GOCouchDBAPIs.DBconnection, "account")

	if err != nil {
		return nil, errors.New(err.Error())
	} else if accout.AccountId < -int32(amount) {
		return nil, errors.New("not enough money")
	}

	return &pb.Response{Msg: "begin transaction successfully"}, nil
}

func (s *Server) Commit(ctx context.Context, request *pb.CommitRequest) (*pb.Response, error) {
	defer GOCouchDBAPIs.CouchLock.ReleaseLock(request.GetUuid())
	balance, ok := GOCouchDBAPIs.Cache.Get(request.GetAccountId())
	if !ok {
		return nil, errors.New("account doesn't exist")
	}
	_, err_update := GOCouchDBAPIs.UpdateAccount(request.GetAccountId(),
		GOCouchDBAPIs.DBconnection, "bank1", balance+request.GetAmount())
	if err_update != nil {
		return nil, err_update
	}
	return &pb.Response{Msg: "commit successfully"}, nil
}
func (s *Server) Abort(ctx context.Context, request *pb.AbortRequest) (*pb.Response, error) {
	//log.Logger.Println("Abort(): start abort")
	valid := mykafka.KafkaLock.ReleaseLock(request.GetUuid())
	if !valid {
		//log.Logger.Println("Abort(): abort failed")
		return nil, errors.New("abort failed")
	}
	return &pb.Response{Msg: "abort successfully"}, nil
}

func (s *Server) Reset(ctx context.Context, e *emptypb.Empty) (*pb.Response, error) {
	return &pb.Response{Msg: "reset successfully"}, nil
}

func (s *Server) CreateAccount(ctx context.Context, request *pb.CreateAccountRequest) (*pb.Response, error) {
	return nil, errors.New("account doesn't exist")
}
func (s *Server) ReadAccount(ctx context.Context, request *pb.ReadAccountRequest) (*pb.Response, error) {
	return nil, errors.New("account doesn't exist")
}

func (s *Server) UpdateAccount(ctx context.Context, request *pb.UpdateAccountRequest) (*pb.Response, error) {
	return nil, errors.New("account doesn't exist")
}

func (s *Server) DeleteAccount(ctx context.Context, request *pb.DeleteAccountRequest) (*pb.Response, error) {
	return nil, errors.New("account doesn't exist")
}
