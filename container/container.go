package container

import (
	log "Twopc-cli/logger"
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
	log.Logger.Println("Get(): ", v, ok)
	return v, ok
}

func (sm *SafeMap) Set(key int32, value int32) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Map[key] = value
	log.Logger.Println("Set(): ", sm.Map)
}

func (sm *SafeMap) Delete(key int32) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.Map, key)
	log.Logger.Println("Delete(): ", sm.Map)
}
