package GOCouchDBAPIs

import (
	_ "github.com/go-kivik/couchdb/v3"
	"github.com/go-kivik/kivik/v3"

	//"github.com/go-kivik/kivik/v4/driver/couchdb"
	"context"
	"fmt"
	"math/rand"

	//"strconv"
	//"log"
	safe "Twopc-cli/container"
)

var CouchLock = safe.InitDBlock()
var Cache = safe.SafeMap{Map: make(map[int32]int32)}

var DBconnection, _ = kivik.New("couch", "http://admin:t102260424@localhost:5984")

type CouchDBAccount struct {
	Id        string `json:"_id,omitempty"`
	Rev       string `json:"_rev,omitempty"`
	AccountId int32  `json:"account_id,omitempty"`
	Deposit   int32  `json:"deposit,omitempty"`
}

func CreatekivikClient() *kivik.Client {
	client, err := kivik.New("couch", "http://admin:t102260424@localhost:5984")
	if err != nil {
		panic(err)
	}
	return client
}

func CreateDBs(DBname string) {
	client := CreatekivikClient()
	defer client.Close(context.Background())
	client.CreateDB(context.TODO(), DBname)
}

func CreateAccounts(num int32, client *kivik.Client, DBname string, id int32) (string, error) {
	//client :=CreatekivikClient()
	//defer client.Close(context.Background())
	db := client.DB(context.TODO(), DBname)
	var i int32
	for i = 0; i < num; i++ {
		Account := CouchDBAccount{AccountId: id, Deposit: 100000000}
		id, rev, err := db.CreateDoc(context.TODO(), Account)
		if err != nil {
			return "There are some errors", err
		}
		Account.Rev = rev
		Account.Id = id
	}
	return "Successfully", nil
}

func AllDocuments(DBname string) error {
	client := CreatekivikClient()
	defer client.Close(context.Background())
	db := client.DB(context.TODO(), DBname)

	rows, err := db.AllDocs(context.Background(), kivik.Options{"include_docs": true})
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var account CouchDBAccount
		if err := rows.ScanDoc(&account); err != nil {
			return err
		}
		Cache.Set(account.AccountId, account.Deposit)
		// accounts = append(accounts, &account)
	}
	return nil
}
func GetRandomCouchDBAccount(accounts []*CouchDBAccount) (*CouchDBAccount, error) {
	if len(accounts) == 0 {
		return nil, fmt.Errorf("沒有可用的帳戶")
	}

	randomIndex := rand.Intn(len(accounts))
	//randomIndex := rand.Intn(200)
	return accounts[randomIndex], nil

}

func FindAccount(id int32, client *kivik.Client, db *kivik.DB) (CouchDBAccount, error) {
	// query := map[string]interface{}{
	// 	"selector": map[string]interface{}{
	// 		"account_id": id,
	// 	},
	// }

	// rows, err1 := db.Find(context.TODO(), query)
	// if err1 != nil {
	// 	fmt.Printf("Error executing query: %v\n", err1)
	// }
	// defer rows.Close()
	// var account CouchDBAccount
	// for rows.Next() {
	// 	if err2 := rows.ScanDoc(&account); err2 != nil {
	// 		fmt.Printf("Error scanning document: %v", err2)
	// 	}
	// }
	balance, ok := Cache.Get(id)
	if !ok {
		return CouchDBAccount{}, fmt.Errorf("account not found")
	}
	account := CouchDBAccount{AccountId: id, Deposit: balance}
	return account, nil
}

func DeleteAccount(id int32, client *kivik.Client, DBname string) (string, error) {
	var account CouchDBAccount
	//client :=CreatekivikClient()
	//defer client.Close(context.Background())
	db := client.DB(context.TODO(), DBname)
	account, err1 := FindAccount(id, client, db)
	if err1 != nil {
		fmt.Printf("Error deleting document: %v", err1)
		return "Error deleting document", err1
	}
	rev, err2 := db.Delete(context.TODO(), account.Id, account.Rev)
	if err2 != nil {
		return "Error deleting document:", err2
	}
	if rev == "0" {
	}
	return "Successfully", nil
}

func ReadAccount(id int32, client *kivik.Client, DBname string) (CouchDBAccount, error) {
	var account CouchDBAccount
	//client :=CreatekivikClient()
	//defer client.Close(context.Background())
	db := client.DB(context.TODO(), DBname)
	account, err := FindAccount(id, client, db)
	if err != nil {
		return CouchDBAccount{}, err
	}
	// println(account.AccountId,"\n",account.Deposit)
	//msg :=strconv.ormatInt.(account.AccountId,10)+"\n"+strconv.FormatInt.(account.Deposit,10)
	return account, nil
}

func UpdateAccount(id int32, client *kivik.Client, DBname string, amount int32) (string, error) {
	var account CouchDBAccount
	//client :=CreatekivikClient()
	//defer client.Close(context.Background())
	db := client.DB(context.TODO(), DBname)
	account, err1 := FindAccount(id, client, db)
	if err1 != nil {
		return "Account Not Found", err1
	}
	account.Rev = account.Rev // Must be set
	account.Deposit = amount
	go db.Put(context.TODO(), account.Id, account)
	Cache.Set(account.AccountId, account.Deposit)
	// if err2 != nil {
	// 	return "There are some errors:", err2
	// }
	// account.Rev = newRev

	return "Successfully", nil
}
