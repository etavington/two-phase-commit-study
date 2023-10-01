package mykafka

import (
	"context"
	"time"

	safe "Twopc-cli/container"
	log "Twopc-cli/logger"

	"github.com/thmeitz/ksqldb-go"
	knet "github.com/thmeitz/ksqldb-go/net"
)

var ksqlUrl = "http://10.140.0.3:8088"

var Records = safe.SafeMap{Map: make(map[uint64]int64)}

var rowChannel = make(chan ksqldb.Row)
var headerChannel = make(chan ksqldb.Header, 1)

func Push_query() {
	log.Logger.Println("Push_query(): start push query")
	var options = knet.Options{BaseUrl: ksqlUrl,
		AllowHTTP: true}
	var kcl, _ = ksqldb.NewClientWithOptions(options)
	defer kcl.Close()
	var query = "SELECT * FROM BALANCE EMIT CHANGES;"
	ctx, cancel := context.WithTimeout(context.TODO(), 180*time.Second)
	defer cancel()
	e := kcl.Push(ctx, ksqldb.QueryOptions{Sql: query}, rowChannel, headerChannel)
	if e != nil {
		log.Logger.Println("Push_query(): error", e)
	}

}

func Modify_map() {
	log.Logger.Println("Modify_map(): start modify map")
	for row := range rowChannel {
		log.Logger.Println("Modify_map(): receive rows", row)
		if row != nil {
			var id = uint64(row[0].(float64))
			if row[1] == nil {
				log.Logger.Println("Modify_map(): deleted", id)
				Records.Delete(uint64(id))
				continue
			}
			var balance = int64(row[1].(float64))
			Records.Set(id, balance)
			log.Logger.Println("Modify_map(): update map", Records.Map)
		}
	}

}

var op = knet.Options{BaseUrl: ksqlUrl,
	AllowHTTP: true}
var ksqlcon, _ = ksqldb.NewClientWithOptions(op)

func SendPayment(from int, amount int) error {

	stmt, err := ksqldb.QueryBuilder("INSERT INTO PAYMENT VALUES(?,?);", from, amount)
	if err != nil {
		log.Logger.Println("SendPaymenta() ksqldb.QueryBuilder: error", err)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	resp, err := ksqlcon.Execute(ctx, ksqldb.ExecOptions{KSql: *stmt})
	if err != nil {
		log.Logger.Println("SendPaymenta() ksqlcon.Execute: error ", err)
		return err
	}
	log.Logger.Println("SendPayment(): response", resp)
	return nil
}

func DeleteAccount(id int, balance int) error {
	err := SendPayment(id, -balance)
	if err != nil {
		log.Logger.Println("DeleteAccount SendPayment: ", err)
		return err
	}
	stmt, err := ksqldb.QueryBuilder("INSERT INTO BALANCE VALUES(?,null);", id)
	if err != nil {
		log.Logger.Println("DeleteAccount ksqldb.QueryBuilder: ", err)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	resp, err := ksqlcon.Execute(ctx, ksqldb.ExecOptions{KSql: *stmt})
	if err != nil {
		log.Logger.Println("DeleteAccount ksqlcon.Execute: ", err)
		return err
	}
	log.Logger.Println("DeleteAccount respones: ", resp)
	return nil
}

func QueryAccount(id int) (int64, bool) {
	v, ok := Records.Get(uint64(id))
	return v, ok
}
