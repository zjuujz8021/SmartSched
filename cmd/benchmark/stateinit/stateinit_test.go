package stateinit

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/holiman/uint256"
)

func TestCreateEmptyStateDB(t *testing.T) {
	basedb := CreateEmptyMvccBaseStateDB()
	db := state.NewMVCCStateDB(basedb, 0, common.MaxHash, nil)

	fmt.Println(db.GetBalance(common.MaxAddress))
	fmt.Println(db.GetCode(common.MaxAddress))
	fmt.Println(db.GetState(common.MaxAddress, common.MaxHash))
	fmt.Println(basedb.Error())

	db.CreateAccount(common.MaxAddress)
	db.AddBalance(common.MaxAddress, uint256.NewInt(1000000000000000000))
	if db.Validate() != state.NoConflict {
		t.Fatal("validate failed")
	}
	db.FinalizeToBaseStateDB(true)

	db2 := state.NewMVCCStateDB(basedb, 0, common.MaxHash, nil)
	fmt.Println(db2.GetBalance(common.MaxAddress))
	fmt.Println(basedb.Error())
}

func TestCreateStateEnv(t *testing.T) {
	env := CreateStateEnv(10)
	mvccdb := state.NewMVCCStateDB(env.SmartSchedStateDB, 0, common.Hash{}, nil)
	fmt.Println(env.SerialStateDB.GetBalance(AdminAddress))
	fmt.Println(mvccdb.GetBalance(AdminAddress))
	for _, addr := range env.Accounts {
		fmt.Println(addr)
		if env.SerialStateDB.GetBalance(addr).Cmp(InitBalance) != 0 {
			t.Fatal("serial state db balance mismatch")
		}
		if mvccdb.GetBalance(addr).Cmp(InitBalance) != 0 {
			t.Fatal("mvcc base state db balance mismatch")
		}
	}
}
