package container

import (
	"Twopc-cli/logger"
	"sync"
)

type SafeMap struct {
	mu  sync.RWMutex
	Map map[int32]int32
}

func (sm *SafeMap) Get(key int32) (int32, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	v, ok := sm.Map[key]
	// log.Logger.Println("Get(): ", v, ok)
	return v, ok
}

func (sm *SafeMap) Set(key int32, value int32) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Map[key] = value
	// log.Logger.Println("Set(): ", sm.Map)
}

func (sm *SafeMap) Delete(key int32) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.Map, key)
	// log.Logger.Println("Delete(): ", sm.Map)
}
func (sm *SafeMap) Add(key int32, delta int32) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Map[key] += delta
	logger.Logger.Println("Add(): ", sm.Map[key])
}

type SafeBuffer struct {
	mu     sync.Mutex
	Buffer []string
}

func (sb *SafeBuffer) Get() string {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	ret := sb.Buffer[0]
	sb.Buffer = sb.Buffer[1:]
	return ret
}
func (sb *SafeBuffer) Set(in string) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.Buffer = append(sb.Buffer, in)

}

type DBlock struct {
	cv           sync.Cond
	currend_uuid string
}

func InitDBlock() *DBlock {
	db := DBlock{cv: *sync.NewCond(&sync.Mutex{}), currend_uuid: ""}
	return &db
}

func (db *DBlock) GetLock(uuid string) bool {
	if db.currend_uuid == uuid {
		// fmt.Println("already lock", uuid)
		return true
	}
	db.cv.L.Lock()
	for db.currend_uuid != "" && db.currend_uuid != uuid {
		// fmt.Println("GetLock(): sleep", db.currend_uuid)
		db.cv.Wait()
	}
	db.currend_uuid = uuid
	db.cv.Broadcast()
	db.cv.L.Unlock()
	// fmt.Println("GetLock(): ", db.currend_uuid)
	return true
}
func (db *DBlock) ReleaseLock(uuid string) bool {
	db.cv.L.Lock()
	defer db.cv.L.Unlock()
	if db.currend_uuid != uuid {
		// fmt.Println("no lock", uuid)
		return true
	}
	db.currend_uuid = ""
	// db.cv.L.Unlock()
	db.cv.Signal()
	// fmt.Println("ReleaseLock(): ", db.currend_uuid)
	return true
}
