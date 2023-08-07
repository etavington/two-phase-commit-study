package main

import (
	pb "Client"
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
)

func send_test(serverIP string) {
	conn, err := grpc.Dial(serverIP, grpc.WithInsecure())
	if err != nil {
		fmt.Println(err)
	}
	defer conn.Close()
	c := pb.NewTwoPhaseCommitServerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.GetLock(ctx, &pb.Payment{Id: 0, From: 1, Amount: 1})
	if err != nil {
		fmt.Println(err)
	}
	c.Commit(ctx, &pb.PaymentID{Id: 0})
	fmt.Println(serverIP, r.GetSuccessful())
}
func main() {
	send_test("localhost:50051")
}
