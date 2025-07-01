package core

import (
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
)

type stateSet struct {
	accountWrite map[common.Address][]int
	balanceWrite map[common.Address][]int
	nonceWrite   map[common.Address][]int
	storageWrite map[common.Address]map[common.Hash][]int

	lastWritePCs []map[StorageKey]uint64
}

func newStateSet(txCount int) *stateSet {
	return &stateSet{
		accountWrite: make(map[common.Address][]int),
		balanceWrite: make(map[common.Address][]int),
		nonceWrite:   make(map[common.Address][]int),
		storageWrite: make(map[common.Address]map[common.Hash][]int),
		lastWritePCs: make([]map[StorageKey]uint64, txCount),
	}
}

func (s *stateSet) set(txIndex int, rwset *state.RWSet) {
	for _, address := range rwset.AccountWrite {
		s.accountWrite[address] = append(s.accountWrite[address], txIndex)
	}
	for _, address := range rwset.BalanceWrite {
		s.balanceWrite[address] = append(s.balanceWrite[address], txIndex)
	}
	for _, address := range rwset.NonceWrite {
		s.nonceWrite[address] = append(s.nonceWrite[address], txIndex)
	}

	s.lastWritePCs[txIndex] = make(map[StorageKey]uint64)
	var storageMp map[common.Hash][]int
	var ok bool
	var storageKey StorageKey
	for _, account := range rwset.StorageWrite {
		copy(storageKey[:], account.Address[:])
		if storageMp, ok = s.storageWrite[account.Address]; !ok {
			storageMp = make(map[common.Hash][]int)
			s.storageWrite[account.Address] = storageMp
		}
		for i, key := range account.Keys {
			storageMp[key] = append(storageMp[key], txIndex)
			copy(storageKey[common.AddressLength:], key[:])
			s.lastWritePCs[txIndex][storageKey] = account.PCs[i]
		}
	}
}

type DependencyGraph struct {
	txCount    int
	rwSets     []*state.RWSet
	stateWrite *stateSet
}

func NewDependencyGraph(rwsets []*state.RWSet) *DependencyGraph {
	if len(rwsets) == 0 {
		return nil
	}
	stateWrite := newStateSet(len(rwsets))
	for i, rwset := range rwsets {
		if rwset == nil {
			continue
		}
		stateWrite.set(i, rwset)
	}
	return &DependencyGraph{
		txCount:    len(rwsets),
		rwSets:     rwsets,
		stateWrite: stateWrite,
		// done:       make([]bool, len(rwsets)),
	}

}

func (g *DependencyGraph) AccountDependent(txIndex int, address common.Address) int {
	if g == nil {
		return -1
	}

	arr := g.stateWrite.accountWrite[address]
	arrLen := len(arr)
	if arrLen == 0 {
		return -1
	}
	idx := sort.Search(arrLen, func(i int) bool {
		return arr[i] >= txIndex
	})
	if idx > 0 {
		dependent := arr[idx-1]
		return dependent
	}
	return -1
}

func (g *DependencyGraph) BalanceDependent(txIndex int, address common.Address) int {
	if g == nil {
		return -1
	}

	arr := g.stateWrite.balanceWrite[address]
	arrLen := len(arr)
	if arrLen == 0 {
		return -1
	}
	idx := sort.Search(arrLen, func(i int) bool {
		return arr[i] >= txIndex
	})
	if idx > 0 {
		dependent := arr[idx-1]
		return dependent
	}
	return -1
}

func (g *DependencyGraph) NonceDependent(txIndex int, address common.Address) int {
	if g == nil {
		return -1
	}

	arr := g.stateWrite.nonceWrite[address]
	arrLen := len(arr)
	if arrLen == 0 {
		return -1
	}
	idx := sort.Search(arrLen, func(i int) bool {
		return arr[i] >= txIndex
	})
	if idx > 0 {
		dependent := arr[idx-1]
		return dependent
	}
	return -1
}

func (g *DependencyGraph) StorageDependent(txIndex int, address common.Address, key common.Hash) int {
	if g == nil {
		return -1
	}

	storageMp, ok := g.stateWrite.storageWrite[address]
	if !ok {
		return -1
	}
	arr := storageMp[key]
	arrLen := len(arr)
	if arrLen == 0 {
		return -1
	}
	idx := sort.Search(arrLen, func(i int) bool {
		return arr[i] >= txIndex
	})
	if idx > 0 {
		dependent := arr[idx-1]
		return dependent
	}

	return -1
}

func (g *DependencyGraph) IsLastStorageWrite(txIndex int, pc uint64, storageKey StorageKey) bool {
	if g == nil {
		return false
	}
	lastPC, ok := g.stateWrite.lastWritePCs[txIndex][storageKey]
	return ok && pc == lastPC
}
