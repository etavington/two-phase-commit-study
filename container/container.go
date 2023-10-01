package container

import (
	log "Twopc-cli/logger"
	"errors"
	"sync"
)

type SafeMap struct {
	mu  sync.RWMutex
	Map map[uint64]int64
}

func (sm *SafeMap) Get(key uint64) (int64, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	v, ok := sm.Map[key]
	log.Logger.Println("Get(): ", v, ok)
	return v, ok
}

func (sm *SafeMap) Set(key uint64, value int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Map[key] = value
	log.Logger.Println("Set(): ", sm.Map)
}

func (sm *SafeMap) Delete(key uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.Map, key)
	log.Logger.Println("Delete(): ", sm.Map)
}

type UserAccountChange struct {
	AccountId int
	Amount    int
}

// return get and set for the txnTable
func New2PCTxnTableAccessors() (func(uint) ([]UserAccountChange, error), func(uint, UserAccountChange) error, func(uint)) {
	txnTable := make(map[uint][]UserAccountChange)
	return func(id uint) ([]UserAccountChange, error) {
			v, ok := txnTable[id]
			log.Logger.Println("TxnGet(): ", v, ok)
			if ok {
				return v, nil
			} else {
				return nil, errors.New("account doesn't exist")
			}
		},
		func(id uint, value UserAccountChange) error {
			log.Logger.Println("TxnSet(): ", id, value)
			if len(txnTable[id]) == 2 {
				return errors.New("the transaction id has already been used")
			}
			txnTable[id] = append(txnTable[id], value)
			return nil
		},
		func(id uint) {
			log.Logger.Println("TxnDel(): ", id)
			delete(txnTable, id)
		}
}
