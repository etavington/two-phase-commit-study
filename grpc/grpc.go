package grpc

import (
	"context"
	"errors"
	"fmt"

	log "Twopc-cli/logger"
	"Twopc-cli/mykafka"
	pb "Twopc-cli/twopcserver"

	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	pb.UnimplementedTwoPhaseCommitServiceServer
}

func (s *Server) CreateAccount(ctx context.Context, request *pb.CreateAccountRequest) (*pb.Response, error) {
	account_id := int(request.GetAccountId())
	log.Logger.Println("CreateAccount() ", "account_id", account_id, ": start create account")
	// _, ok := mykafka.QueryAccount(account_id)
	// if ok {
	// 	log.Logger.Println("CreateAccount() ", "account_id", account_id, ": Account already exists")
	// 	return nil, errors.New("account already exists")
	// } else {
	// 	mykafka.SendPayment(account_id, 0)
	// 	var v int32
	// 	var ok bool
	// 	for {
	// 		time.Sleep(1 * time.Second)
	// 		v, ok = mykafka.QueryAccount(account_id)
	// 		if ok {
	// 			break
	// 		}
	// 	}
	mykafka.SendPayment(account_id, 100)
	log.Logger.Println("CreateAccount() ", "account_id", account_id, ": create account successfully")
	return &pb.Response{Msg: "create account successfully"}, nil
}
func (s *Server) ReadAccount(ctx context.Context, request *pb.ReadAccountRequest) (*pb.Response, error) {
	fmt.Println(request)
	fmt.Println(request.GetAccountId())
	account_id := int(request.GetAccountId())
	log.Logger.Println("ReadAccount()", "account_id", account_id, ": start read account")
	balance, ok := mykafka.QueryAccount(account_id)
	if ok {
		fstr := fmt.Sprintf("%d's balance is %d", account_id, balance)
		log.Logger.Println("ReadAccount(): ", "account_id", account_id, ":", fstr)
		return &pb.Response{Msg: fstr}, nil
	} else {
		log.Logger.Println("ReadAccount(): ", "account_id", account_id, ": Account doesn't exist")
		return nil, errors.New("account doesn't exist")
	}
}

func (s *Server) UpdateAccount(ctx context.Context, request *pb.UpdateAccountRequest) (*pb.Response, error) {
	account_id := int(request.GetAccountId())
	log.Logger.Println("UpdateAccount()", "account_id", account_id, ": start update account")
	amount := int(request.GetAmount())
	if amount < 0 {
		log.Logger.Println("UpdateAccount()", "account_id", account_id, ": Amount should be positive")
		return nil, errors.New("amount should be positive")
	}
	balance, ok := mykafka.QueryAccount(account_id)
	if ok {
		delta := -int(balance) + amount
		log.Logger.Println(delta)
		mykafka.SendPayment(account_id, delta)
		log.Logger.Println("UpdateAccount()", "account_id", account_id, ": update account successfully")
		return &pb.Response{Msg: "update account successfully"}, nil
	} else {
		log.Logger.Println("UpdateAccount()", "account_id", account_id, ": Account doesn't exist")
		return nil, errors.New("account doesn't exist")
	}
}

func (s *Server) DeleteAccount(ctx context.Context, request *pb.DeleteAccountRequest) (*pb.Response, error) {
	account_id := int(request.GetAccountId())
	log.Logger.Println("DeleteAccount()", "account_id", account_id, ": start delete account")
	balance, ok := mykafka.QueryAccount(account_id)
	if ok {
		err := mykafka.DeleteAccount(account_id, int(balance))
		if err != nil {
			log.Logger.Println("DeleteAccount()", "account_id", account_id, ":", err)
			return nil, err
		} else {
			log.Logger.Println("DeleteAccount()", "account_id", account_id, ": delete account successfully")
			return &pb.Response{Msg: "delete account successfully"}, nil
		}
	}
	log.Logger.Println("DeleteAccount()", "account_id", account_id, ": Account doesn't exist")
	return nil, errors.New("account doesn't exist")
}

func (s *Server) BeginTransaction(ctx context.Context, request *pb.BeginTransactionRequest) (*pb.Response, error) {
	log.Logger.Println("BeginTransaction(): start begin transaction")
	amount := int(request.GetAmount())
	account_id := int(request.GetAccountId())
	fmt.Println("account :", account_id)
	v, ok := mykafka.QueryAccount(account_id)
	if !ok {
		log.Logger.Println("BeginTransaction(): Account doesn't exist")
		return nil, errors.New("account doesn't exist")
	} else if v < -int32(amount) {
		log.Logger.Println("BeginTransaction(): Not enough money")
		return nil, errors.New("not enough money")
	}

	log.Logger.Println("BeginTransaction(): begin transaction successfully")
	return &pb.Response{Msg: "begin transaction successfully"}, nil
}

func (s *Server) Commit(ctx context.Context, request *pb.CommitRequest) (*pb.Response, error) {
	// mykafka.KafkaLock.Lock()
	// defer mykafka.KafkaLock.Unlock()
	log.Logger.Println("Commit(): start commit")
	amount := int(request.GetAmount())
	account_id := int(request.GetAccountId())
	err := mykafka.SendPayment(account_id, amount)
	if err != nil {
		log.Logger.Println("Commit(): ", err)
		return nil, err
	}
	log.Logger.Print("Commit(): commit successfully")

	return &pb.Response{Msg: "commit successfully"}, nil
}
func (s *Server) Abort(ctx context.Context, request *pb.AbortRequest) (*pb.Response, error) {
	log.Logger.Println("Abort(): start abort")
	log.Logger.Println("Abort(): abort successfully")
	return &pb.Response{Msg: "abort successfully"}, nil
}

func (s *Server) Reset(ctx context.Context, e *emptypb.Empty) (*pb.Response, error) {
	log.Logger.Println("Reset(): start reset")
	ids := make([]int32, 0, len(mykafka.Records.Map))
	for k := range mykafka.Records.Map {
		ids = append(ids, k)
	}
	log.Logger.Println("Reset(): delete all account")
	for _, id := range ids {
		mykafka.DeleteAccount(int(id), int(mykafka.Records.Map[id]))
	}
	log.Logger.Println("Reset(): reset successfully")
	return &pb.Response{Msg: "reset successfully"}, nil
}
