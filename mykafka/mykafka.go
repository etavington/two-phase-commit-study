package mykafka

import (
	"time"

	_ "github.com/go-kivik/couchdb/v3"
	"github.com/go-kivik/kivik/v3"

	//"sync"
	"context"
	//"math/rand"

	safe "Twopc-cli/container"
	"fmt"
)

type CouchDBAccount struct {
	Id        string `json:"_id,omitempty"`
	Rev       string `json:"_rev,omitempty"`
	AccountId int32  `json:"account_id,omitempty"`
	Deposit   int32  `json:"deposit,omitempty"`
}

var Records = safe.SafeMap{Map: make(map[int32]int32)}

// var Records = safe.SafeMap2{M: make(map[int32]*safe.SafeEntry)}

var client = CreatekivikClient()

func CreatekivikClient() *kivik.Client {
	client, err := kivik.New("couch", "http://admin:t102260424@34.80.195.25:5984")
	//改成http://admin:t102260424@http://34.80.195.25:5984
	if err != nil {
		panic(err)
	}
	return client
}

func query(id int) (int32, bool) {
	//client :=CreatekivikClient()
	//defer client.Close(context.Background())
	db := client.DB(context.TODO(), "bank1")
	var account CouchDBAccount
	account, err1 := FindAccount(int32(id), client, db)
	if err1 != nil {
		return -1, false
	}

	return account.Deposit, true

}

func QueryAccount(id int) (int32, bool) {
	return Records.Get(int32(id))
	// return query(id)
}

var buffer = safe.SafeBuffer{Buffer: make([]safe.CacheInfo, 0, 10000)}

func BackgroundSendPayment() {
	db := client.DB(context.TODO(), "bank1")
	for {
		if len(buffer.Buffer) > 0 {
			stmt := buffer.Get()
			_, err2 := db.Put(context.TODO(), stmt.Id, stmt.Account)
			if err2 != nil {
				fmt.Println(err2)
			}
			//account.Rev = newRev

		} else {
			time.Sleep(time.Second)
		}
	}

}

func FindAccount(id int32, client *kivik.Client, db *kivik.DB) (CouchDBAccount, error) {
	query := map[string]interface{}{
		"selector": map[string]interface{}{
			"account_id": id,
		},
	}

	rows, err1 := db.Find(context.TODO(), query)
	if err1 != nil {
		fmt.Printf("Error executing query: %v\n", err1)
	}
	defer rows.Close()
	var account CouchDBAccount
	for rows.Next() {
		if err2 := rows.ScanDoc(&account); err2 != nil {
			fmt.Printf("Error scanning document: %v", err2)
		}
	}
	return account, nil
}

func SendPayment(id int, amount int) error {
	//client :=CreatekivikClient()
	//defer client.Close(context.Background())
	// db := client.DB(context.TODO(), "bank1")
	// var account CouchDBAccount
	// account, err1 := FindAccount(int32(id), client, db)
	// if err1 != nil {
	// 	return err1
	// }
	// account.Rev = account.Rev // Must be set
	// account.Deposit += int32(amount)
	// newRev, err2 := db.Put(context.TODO(), account.Id, account)
	// if err2 != nil {
	// 	return err2
	// }
	// account.Rev = newRev
	// var account2 container.CouchDBAccount
	// account2.Id = account.Id
	// account2.AccountId = account.AccountId
	// account2.Rev = account.Rev
	// account2.Deposit = account.Deposit
	// var info safe.CacheInfo
	// info = safe.CacheInfo{
	// 	Id:      account.Id,
	// 	Account: account2,
	// }
	// buffer.Set(info)
	Records.Add(int32(id), int32(amount))
	return nil
}

func DeleteAccount(id int, balance int) error {
	/*var account CouchDBAccount
	  //client :=CreatekivikClient()
	  //defer client.Close(context.Background())
	  db := client.DB(context.TODO(),"bank1")
	  account ,err1:= FindAccount(int32(id),client,db)
	  if err1 !=nil{
	    fmt.Errorf("Error deleting document: %v", err1)
	    return "Error deleting document", err1
	  }
	  rev, err2 := db.Delete(context.TODO(),account.Id,account.Rev)
	  if err2 != nil {
	    return "Error deleting document:",err2
	  }
	  if rev=="0"{
	  }*/
	return nil
}

func InitRecord() {
	query := map[string]interface{}{
		"selector": map[string]interface{}{},
		"limit":    10000,
	}

	db := client.DB(context.TODO(), "bank1")
	rows, err1 := db.Find(context.TODO(), query)
	if err1 != nil {
		fmt.Printf("Error executing query: %v\n", err1)
	}
	defer rows.Close()
	var account CouchDBAccount

	for rows.Next() {
		if err2 := rows.ScanDoc(&account); err2 != nil {
			fmt.Printf("Error scanning document: %v", err2)
		}
		id := account.AccountId
		balance := account.Deposit
		// for dblock
		Records.Set(int32(id), int32(balance))
		//a, b := Records.Get(int32(id))

		// for account lock
		// Records.InitMap2(int32(id), int32(balance))
	}
	/*	for _, r := range resp {
		id := r[0].(float64)
		balance := r[1].(float64)
		// for dblock
		Records.Set(int32(id), int32(balance))
		// for account lock
		// Records.InitMap2(int32(id), int32(balance))
	} */
	// log.Logger.Println("InitRecord respones: ", resp)
}
