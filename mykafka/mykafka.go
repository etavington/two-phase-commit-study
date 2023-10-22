package mykafka

import (
	"context"
	"fmt"

	safe "Twopc-cli/container"
	log "Twopc-cli/logger"
	"sync"

	"github.com/thmeitz/ksqldb-go"
	knet "github.com/thmeitz/ksqldb-go/net"
)

var ksqlUrl = "http://10.140.0.4:8088"

var Records = safe.SafeMap{Map: make(map[int32]int32)}

var rowChannel = make(chan ksqldb.Row)
var headerChannel = make(chan ksqldb.Header, 1)

func Push_query() {
	var options = knet.Options{BaseUrl: ksqlUrl,
		AllowHTTP: true}
	var kcl, _ = ksqldb.NewClientWithOptions(options)
	defer kcl.Close()
	var query = "SELECT * FROM BALANCE EMIT CHANGES;"
	ctx := context.TODO()
	e := kcl.Push(ctx, ksqldb.QueryOptions{Sql: query}, rowChannel, headerChannel)
	if e != nil {
		log.Logger.Println("Push_query(): error", e)
	} else {
		log.Logger.Println("Push_query(): start push query")
	}

}

func Modify_map() {
	log.Logger.Println("Modify_map(): start modify map")
	for row := range rowChannel {
		log.Logger.Println("Modify_map(): receive rows", row)
		if row != nil {
			var id = int32(row[0].(float64))
			if row[1] == nil {
				log.Logger.Println("Modify_map(): deleted", id)
				Records.Delete(int32(id))
				continue
			}
			var balance = int32(row[1].(float64))
			Records.Set(id, balance)
			log.Logger.Println("Modify_map(): update map", Records.Map)
		}
	}

}

type dbLock struct {
	mu  sync.Mutex
	con ksqldb.KsqldbClient
}

func (db *dbLock) Execute(ctx context.Context, options ksqldb.ExecOptions) (*ksqldb.KsqlResponseSlice, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	fmt.Println(123)
	res, err := db.con.Execute(ctx, options)
	return res, err
}

var op = knet.Options{BaseUrl: ksqlUrl,
	AllowHTTP: true}
var con, _ = ksqldb.NewClientWithOptions(op)
var ksqlcon = dbLock{con: con}

func SendPayment(from int, amount int) error {

	stmt, err := ksqldb.QueryBuilder("INSERT INTO PAYMENT VALUES(?,?);", from, amount)
	if err != nil {
		log.Logger.Println("SendPaymenta() ksqldb.QueryBuilder: error", err)
		return err
	}
	ctx := context.TODO()
	resp, err := ksqlcon.Execute(ctx, ksqldb.ExecOptions{KSql: *stmt})
	if err != nil {
		log.Logger.Println("SendPaymenta() ksqlcon.Execute: error ", err)
		return err
	}
	log.Logger.Println("SendPayment(): response", resp)
	return nil
}

func DeleteAccount(id int, balance int) error {
	stmt, err := ksqldb.QueryBuilder("INSERT INTO BALANCE VALUES(?,null);", id)
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

func QueryAccount(id int) (int32, bool) {
	v, ok := Records.Get(int32(id))
	return v, ok
}
