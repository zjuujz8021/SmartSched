package state

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

var (
	ObjShardingNumber = 32
)

func fnv32(key common.Address) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < common.AddressLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

type ShardingMap struct {
	shards []sync.Map
}

func NewShardingAccountMap() *ShardingMap {
	shards := make([]sync.Map, ObjShardingNumber)
	return &ShardingMap{shards: shards}
}

func (m *ShardingMap) Load(key common.Address) (value *mvccBaseStateObject, ok bool) {
	ret, exists := m.shards[int(fnv32(key))%ObjShardingNumber].Load(key)
	if !exists {
		return nil, false
	}
	return ret.(*mvccBaseStateObject), true
}

func (m *ShardingMap) LoadOrStore(key common.Address, value *mvccBaseStateObject) (actual *mvccBaseStateObject, loaded bool) {
	ret, exists := m.shards[int(fnv32(key))%ObjShardingNumber].LoadOrStore(key, value)
	if !exists {
		return ret.(*mvccBaseStateObject), false
	}
	return ret.(*mvccBaseStateObject), true
}

func (m *ShardingMap) DeepCopy(newdb *MVCCBaseStateDB) *ShardingMap {
	cp := NewShardingAccountMap()
	for i := 0; i < ObjShardingNumber; i++ {
		m.shards[i].Range(func(key, value interface{}) bool {
			cp.shards[i].Store(key, value.(*mvccBaseStateObject).DeepCopy(newdb))
			return true
		})
	}
	return cp
}
