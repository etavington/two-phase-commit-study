package myKafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/thmeitz/ksqldb-go"
	knet "github.com/thmeitz/ksqldb-go/net"
)

type SafeMap struct {
	mu  sync.RWMutex
	Map map[uint64]int64
}

func (sm *SafeMap) Get(key uint64) int64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.Map[key]
}

func (sm *SafeMap) Set(key uint64, value int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Map[key] = value
}

var Records = SafeMap{Map: make(map[uint64]int64)}

var rowChannel = make(chan ksqldb.Row)
var headerChannel = make(chan ksqldb.Header, 1)

// var f, _ = os.OpenFile("golog.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
// var logger = log.New(f, "ksqldb-go ", log.LstdFlags)

func Exec_ksql() {
	var options = knet.Options{BaseUrl: "http://10.140.0.3:8088",
		AllowHTTP: true}
	var kcl, _ = ksqldb.NewClientWithOptions(options)
	defer kcl.Close()
	var query = "SELECT * FROM BALANCE EMIT CHANGES;"
	ctx, cancel := context.WithTimeout(context.TODO(), 1800000*time.Second)
	defer cancel()
	e := kcl.Push(ctx, ksqldb.QueryOptions{Sql: query}, rowChannel, headerChannel)
	if e != nil {
		fmt.Println(e)
	}

}

func Modify_map() {
	for row := range rowChannel {
		fmt.Println(row)
		if row != nil {
			var id = uint64(row[0].(float64))
			var balance = int64(row[1].(float64))
			Records.Set(id, balance)
		}
	}

}

type PaymentDetail struct {
	Id     uint64 `json:"ID"`
	Amount int64  `json:"AMOUNT"`
}

var p, _ = kafka.NewProducer(&kafka.ConfigMap{
	"bootstrap.servers": "10.140.0.3:9092",
	"acks":              "all"})
var delivery_chan = make(chan kafka.Event, 10000)
var topic = "Payment"

func SendPayment(from int, amount int) {
	a := PaymentDetail{Id: uint64(from), Amount: int64(amount)}
	b, _ := json.Marshal(a)
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(strconv.Itoa(from)),
		Value:          []byte(b)},
		delivery_chan,
	)
}
func ReceiveKafkaCallback() {
	for e := range delivery_chan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
			} else {
				fmt.Println(ev.TopicPartition)
			}
		}
	}
}
