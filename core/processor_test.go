package core

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
)

func TestDmvccDeps(t *testing.T) {
	addr1 := common.HexToAddress("0xfffffffffffffffffffffffffffffffffffffff1")
	addr2 := common.HexToAddress("0xfffffffffffffffffffffffffffffffffffffff2")
	addr3 := common.HexToAddress("0xfffffffffffffffffffffffffffffffffffffff3")

	slot1 := common.HexToHash("0x01")
	slot2 := common.HexToHash("0x02")

	rwsets := []*state.RWSet{
		// tx0
		{
			BalanceRead:  []common.Address{addr1},
			BalanceWrite: []common.Address{addr1},
			StorageWrite: []state.RWSetStorage{
				{
					Address: addr1,
					Keys:    []common.Hash{slot1},
				},
			},
		},
		// tx1
		{
			BalanceRead:  []common.Address{addr1},
			AccountWrite: []common.Address{addr2},
		},
		// tx2
		{
			NonceRead:   []common.Address{addr1, addr2},
			BalanceRead: []common.Address{addr1, addr2},
			StorageRead: []state.RWSetStorage{
				{
					Address: addr1,
					Keys:    []common.Hash{slot1},
				},
				{
					Address: addr1,
					Keys:    []common.Hash{slot2},
				},
				{
					Address: addr3,
					Keys:    []common.Hash{slot1},
				},
			},
		},
	}

	deps := RWSetsToDmvccDependency(rwsets)
	data, _ := json.MarshalIndent(deps, "", "  ")
	fmt.Println(string(data))
}
