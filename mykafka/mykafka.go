package mykafka

import (
	"context"
	"fmt"
	"time"

	"github.com/thmeitz/ksqldb-go"
	knet "github.com/thmeitz/ksqldb-go/net"

	safe "Twopc-cli/container"
)

var ksqlUrl = "http://localhost:8088"

var Records = safe.SafeMap{Map: make(map[uint64]int64)}

var rowChannel = make(chan ksqldb.Row)
var headerChannel = make(chan ksqldb.Header, 1)

// var f, _ = os.OpenFile("golog.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
// var logger = log.New(f, "ksqldb-go ", log.LstdFlags)

func Push_query() {
	var options = knet.Options{BaseUrl: ksqlUrl,
		AllowHTTP: true}
	var kcl, _ = ksqldb.NewClientWithOptions(options)
	defer kcl.Close()
	var query = "SELECT * FROM BALANCE EMIT CHANGES;"
	ctx, cancel := context.WithTimeout(context.TODO(), 180*time.Second)
	defer cancel()
	e := kcl.Push(ctx, ksqldb.QueryOptions{Sql: query}, rowChannel, headerChannel)
	if e != nil {
		fmt.Println(e)
	}

}

func Modify_map() {
	for row := range rowChannel {
		if row != nil {
			var id = uint64(row[0].(float64))
			if row[1] == nil {
				fmt.Println(id, "deleted")
				Records.Delete(uint64(id))
				continue
			}
			var balance = int64(row[1].(float64))
			Records.Set(id, balance)
			fmt.Println(id, balance)
		}
	}

}

var op = knet.Options{BaseUrl: ksqlUrl,
	AllowHTTP: true}
var ksqlcon, _ = ksqldb.NewClientWithOptions(op)

func SendPayment(from int, amount int) error {

	stmt, err := ksqldb.QueryBuilder("INSERT INTO PAYMENT VALUES(?,?);", from, amount)
	if err != nil {
		fmt.Println(err)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	resp, err := ksqlcon.Execute(ctx, ksqldb.ExecOptions{KSql: *stmt})
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Println(resp)
	return nil
}

func DeleteAccount(id int, balance int) error {
	err := SendPayment(id, -balance)
	if err != nil {
		fmt.Println(err)
		return err
	}
	stmt, err := ksqldb.QueryBuilder("INSERT INTO BALANCE VALUES(?,null);", id)
	if err != nil {
		fmt.Println(err)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	resp, err := ksqlcon.Execute(ctx, ksqldb.ExecOptions{KSql: *stmt})
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Println(resp)
	return nil
}

func QueryAccount(id int) (int64, bool) {
	v, ok := Records.Get(uint64(id))
	if v == 0 {
		return 0, false
	}
	return v, ok
}
