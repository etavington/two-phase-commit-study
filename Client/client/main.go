package main

import (
	pb "Client"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	tr "Client/trans"

	"google.golang.org/grpc"
)

type accountData struct {
	serverIP  string
	accountID uint
}

func getLock(amount int64, transactionId uint64, data accountData, wg *sync.WaitGroup, flag *bool, c pb.TwoPhaseCommitServerClient, ctx context.Context) {
	defer wg.Done()

	r, err := c.GetLock(ctx, &pb.Payment{Id: transactionId, From: uint64(data.accountID), Amount: amount})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(data.serverIP, r.GetSuccessful())
	if *flag == true {
		*flag = r.GetSuccessful()
	}
}
func callTransfer(client tr.TransactionClient, payment *tr.Payment) {
	res, err := client.Transfer(context.Background(), payment)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res.Reply)
}

func send_transaction(sender accountData, receiver accountData, amount int64) {
	// setup sender grpc connection
	conn1, _ := grpc.Dial(sender.serverIP, grpc.WithInsecure())
	defer conn1.Close()
	c1 := pb.NewTwoPhaseCommitServerClient(conn1)
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
	defer cancel1()
	//setup receiver grpc connection
	conn2, _ := grpc.Dial(receiver.serverIP, grpc.WithInsecure())
	defer conn2.Close()
	c2 := tr.NewTransactionClient(conn2)

	var senderTransactionId, receiverTransactionId uint64
	rand.Seed(time.Now().UnixNano())
	senderTransactionId = rand.Uint64()
	if sender.serverIP == receiver.serverIP {
		fmt.Println("same server")
		rand.Seed(time.Now().UnixNano())
		receiverTransactionId = rand.Uint64()
	} else {
		receiverTransactionId = senderTransactionId
	}
	fmt.Println(senderTransactionId, receiverTransactionId)
	flag := true
	wg := sync.WaitGroup{}
	wg.Add(2)
	go getLock(amount, senderTransactionId, sender, &wg, &flag, c1, ctx1)
	p := &tr.Payment{
		Giver:        "1",
		GiverBank:    "bank1",
		Receiver:     "2",
		ReceiverBank: "bank2",
		Amount:       int32(amount)}
	go callTransfer(c2, p)
	wg.Wait()
	if flag {
		a, _ := c1.Commit(ctx1, &pb.PaymentID{Id: senderTransactionId})
		fmt.Println(a)
	}
}
func main() {
	send_transaction(accountData{"104.199.211.126:50051", 1}, accountData{"35.194.168.145:50051", 2}, 1)
}
