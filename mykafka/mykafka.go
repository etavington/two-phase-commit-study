package mykafka

import (
	"context"
	"fmt"

	safe "Twopc-cli/container"
	log "Twopc-cli/logger"

	"github.com/thmeitz/ksqldb-go"
	knet "github.com/thmeitz/ksqldb-go/net"
)

// remember to change postfix of table/stream
// BALANCE and PAYMENT
var ksqlUrl = "http://10.140.0.4:8088"
var Records = safe.SafeMap{Map: make(map[int32]int32)}
var op = knet.Options{BaseUrl: ksqlUrl,
	AllowHTTP: true}
var ksqlcon, _ = ksqldb.NewClientWithOptions(op)
var KafkaLock = safe.InitDBlock()

func query(id int) (int32, bool) {
	stmnt, err := ksqldb.QueryBuilder("SELECT balance FROM BALANCE3 WHERE id=?;", id)
	if err != nil {
		log.Logger.Println("query ksqldb.QueryBuilder: ", err)
		return 0, false
	}
	ctx := context.TODO()
	qOpts := &ksqldb.QueryOptions{Sql: *stmnt}
	_, resp, err := ksqlcon.Pull(ctx, *qOpts)
	if err != nil {
		log.Logger.Println("query ksqlcon.Execute: ", err)
		return 0, false
	}
	fmt.Println("query respones: ", resp)
	if len(resp) == 0 {
		return 0, false
	}
	balance := resp[0][0].(float64)
	return int32(balance), true

}

func QueryAccount(id int) (int32, bool) {
	return Records.Get(int32(id))
	// return query(id)
}

func SendPayment(id int, amount int) error {
	stmt, err := ksqldb.QueryBuilder("INSERT INTO PAYMENT3 VALUES(?,?);", id, amount)
	if err != nil {
		// log.Logger.Println("SendPaymenta() ksqldb.QueryBuilder: error", err)
		return err
	}
	go func(con *ksqldb.KsqldbClient, ctx context.Context, stmt *string) {
		_, err := ksqlcon.Execute(ctx, ksqldb.ExecOptions{KSql: *stmt})
		if err != nil {
			log.Logger.Println("SendPaymenta() ksqlcon.Execute: error ", err)
		} else {
			// log.Logger.Println("SendPayment(): response", resp)
		}
	}(&ksqlcon, context.TODO(), stmt)
	Records.Add(int32(id), int32(amount))
	return nil
}

func DeleteAccount(id int, balance int) error {
	stmt, err := ksqldb.QueryBuilder("INSERT INTO BALANCE3 VALUES(?,null);", id)
	if err != nil {
		log.Logger.Println("DeleteAccount ksqldb.QueryBuilder: ", err)
		return err
	}
	ctx := context.TODO()
	resp, err := ksqlcon.Execute(ctx, ksqldb.ExecOptions{KSql: *stmt})
	if err != nil {
		log.Logger.Println("DeleteAccount ksqlcon.Execute: ", err)
		return err
	}
	log.Logger.Println("DeleteAccount respones: ", resp)
	return nil
}

func InitRecord() {
	stmnt, err := ksqldb.QueryBuilder("SELECT * FROM BALANCE3;")
	if err != nil {
		log.Logger.Println("InitRecord ksqldb.QueryBuilder: ", err)
		return
	}
	ctx := context.TODO()
	qOpts := &ksqldb.QueryOptions{Sql: *stmnt}
	_, resp, err := ksqlcon.Pull(ctx, *qOpts)
	if err != nil {
		log.Logger.Println("InitRecord ksqlcon.Execute: ", err)
		return
	}
	for _, r := range resp {
		id := r[0].(float64)
		balance := r[1].(float64)
		Records.Set(int32(id), int32(balance))
	}
	// log.Logger.Println("InitRecord respones: ", resp)
}
