package stateinit

import (
	"math/big"
	"time"

	blockstm "github.com/crypto-org-chain/go-block-stm"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

const (
	DefaultGasLimit      = 1000000
	DefaultBlockGasLimit = 1000000000000
)

var AdminAddress = common.HexToAddress("0xffffffffffffffffffffffffffffffff00000000")

var InitBalance = uint256.NewInt(2000000000000000000)

func NewMockHeader() *types.Header {
	header := &types.Header{
		Number:     big.NewInt(20000000),
		Difficulty: big.NewInt(0),
		GasLimit:   DefaultBlockGasLimit,
		BaseFee:    big.NewInt(0),
		Time:       uint64(time.Now().Unix()),
	}
	return header
}

func NewMessage() *core.Message {
	msg := &core.Message{
		Value:             big.NewInt(0),
		GasLimit:          DefaultGasLimit,
		GasPrice:          big.NewInt(0),
		GasFeeCap:         big.NewInt(0),
		GasTipCap:         big.NewInt(0),
		SkipAccountChecks: true,
	}
	return msg
}

type StateEnv struct {
	Accounts []common.Address
	// used for serial execution
	SerialStateDB *state.StateDB
	OCCStateDB    *state.MVCCBaseStateDB
	// used for SmartSched execution
	STMStateDB                     blockstm.MultiStore
	TwoPLStateDB                   *state.MVCCBaseStateDB
	SpectrumStateDB                *state.MVCCBaseStateDB
	DmvccStateDB                   *state.MVCCBaseStateDB
	ParEVMStateDB                  *state.MVCCBaseStateDB
	SmartSchedStateDB              *state.MVCCBaseStateDB
	SmartSchedOnlySchedStateDB     *state.MVCCBaseStateDB
	SmartSchedOnlyPreCommitStateDB *state.MVCCBaseStateDB
}

func CreateAddresses(n int) []common.Address {
	addresses := make([]common.Address, n)
	for i := 0; i < n; i++ {
		addresses[i] = common.BigToAddress(new(big.Int).Add(AdminAddress.Big(), big.NewInt(int64(i+1))))
	}
	return addresses
}

func CreateEmptyStateDB() *state.StateDB {
	db := rawdb.NewMemoryDatabase()
	sdb, _ := state.New(types.EmptyRootHash, state.NewDatabase(db), nil)
	return sdb
}

func CreateEmptyMvccBaseStateDB() *state.MVCCBaseStateDB {
	db := rawdb.NewMemoryDatabase()
	sdb := state.NewMVCCBaseStateDB(state.NewDatabase(db))
	return sdb
}

func CreateStateEnv(n int) *StateEnv {
	accts := CreateAddresses(n)
	env := &StateEnv{
		Accounts:          accts,
		SerialStateDB:     CreateEmptyStateDB(),
		OCCStateDB:        CreateEmptyMvccBaseStateDB(),
		SmartSchedStateDB: CreateEmptyMvccBaseStateDB(),
		STMStateDB:        blockstm.NewMultiMemDB(state.STMStores),
		TwoPLStateDB:      CreateEmptyMvccBaseStateDB(),
		SpectrumStateDB:   CreateEmptyMvccBaseStateDB(),
		DmvccStateDB:      CreateEmptyMvccBaseStateDB(),
		ParEVMStateDB:     CreateEmptyMvccBaseStateDB(),
	}
	occDb := state.NewMVCCStateDB(env.OCCStateDB, 0, common.Hash{}, nil)
	smartschedDb := state.NewMVCCStateDB(env.SmartSchedStateDB, 0, common.Hash{}, nil)
	stmmv := blockstm.NewMVMemory(1, state.STMStores, env.STMStateDB, blockstm.NewScheduler(1))
	stmview0 := stmmv.View(0)
	stmdb, _ := state.New(common.Hash{}, state.NewBlockSTMTrie(stmview0), nil)
	twoPlDb := state.NewMVCCStateDB(env.TwoPLStateDB, 0, common.Hash{}, nil)
	spectrumDb := state.NewMVCCStateDB(env.SpectrumStateDB, 0, common.Hash{}, nil)
	dmvccDb := state.NewMVCCStateDB(env.DmvccStateDB, 0, common.Hash{}, nil)
	parEvmDb := state.NewMVCCStateDB(env.ParEVMStateDB, 0, common.Hash{}, nil)

	env.SerialStateDB.AddBalance(AdminAddress, InitBalance)
	occDb.AddBalance(AdminAddress, InitBalance)
	smartschedDb.AddBalance(AdminAddress, InitBalance)
	stmdb.AddBalance(AdminAddress, InitBalance)
	twoPlDb.AddBalance(AdminAddress, InitBalance)
	spectrumDb.AddBalance(AdminAddress, InitBalance)
	dmvccDb.AddBalance(AdminAddress, InitBalance)
	parEvmDb.AddBalance(AdminAddress, InitBalance)

	env.SerialStateDB.AddBalance(common.Address{}, InitBalance)
	occDb.AddBalance(common.Address{}, InitBalance)
	smartschedDb.AddBalance(common.Address{}, InitBalance)
	stmdb.AddBalance(common.Address{}, InitBalance)
	twoPlDb.AddBalance(common.Address{}, InitBalance)
	spectrumDb.AddBalance(common.Address{}, InitBalance)
	dmvccDb.AddBalance(common.Address{}, InitBalance)
	parEvmDb.AddBalance(common.Address{}, InitBalance)

	for _, addr := range accts {
		env.SerialStateDB.AddBalance(addr, InitBalance)
		occDb.AddBalance(addr, InitBalance)
		smartschedDb.AddBalance(addr, InitBalance)
		stmdb.AddBalance(addr, InitBalance)
		twoPlDb.AddBalance(addr, InitBalance)
		spectrumDb.AddBalance(addr, InitBalance)
		dmvccDb.AddBalance(addr, InitBalance)
		parEvmDb.AddBalance(addr, InitBalance)
	}
	if occDb.Validate() != state.NoConflict {
		panic("occ db validate failed")
	}
	occDb.FinalizeToBaseStateDB(true)
	if smartschedDb.Validate() != state.NoConflict {
		panic("smartsched db validate failed")
	}
	smartschedDb.FinalizeToBaseStateDB(true)
	stmdb.WriteToSTMStore(true)
	stmmv.Record(blockstm.TxnVersion{Index: 0, Incarnation: 0}, stmview0)
	stmmv.WriteSnapshot(env.STMStateDB)
	if twoPlDb.Validate() != state.NoConflict {
		panic("two pl db validate failed")
	}
	twoPlDb.FinalizeToBaseStateDB(true)
	spectrumDb.FinalizeToBaseStateDB(true)
	dmvccDb.FinalizeToBaseStateDB(true)
	parEvmDb.FinalizeToBaseStateDB(true)

	env.SmartSchedOnlySchedStateDB = env.SmartSchedStateDB.DeepCopy()
	env.SmartSchedOnlyPreCommitStateDB = env.SmartSchedStateDB.DeepCopy()

	// sanity check
	occDb = state.NewMVCCStateDB(env.OCCStateDB, 0, common.Hash{}, nil)
	smartschedDb = state.NewMVCCStateDB(env.SmartSchedStateDB, 0, common.Hash{}, nil)
	stmmv = blockstm.NewMVMemory(1, state.STMStores, env.STMStateDB, blockstm.NewScheduler(1))
	stmdb, _ = state.New(common.Hash{}, state.NewBlockSTMTrie(stmmv.View(0)), nil)
	twoPlDb = state.NewMVCCStateDB(env.TwoPLStateDB, 0, common.Hash{}, nil)
	spectrumDb = state.NewMVCCStateDB(env.SpectrumStateDB, 0, common.Hash{}, nil)
	dmvccDb = state.NewMVCCStateDB(env.DmvccStateDB, 0, common.Hash{}, nil)
	parEvmDb = state.NewMVCCStateDB(env.ParEVMStateDB, 0, common.Hash{}, nil)
	schedOnlySchedDb := state.NewMVCCStateDB(env.SmartSchedOnlySchedStateDB, 0, common.Hash{}, nil)
	schedOnlyPreCommitDb := state.NewMVCCStateDB(env.SmartSchedOnlyPreCommitStateDB, 0, common.Hash{}, nil)

	for _, addr := range accts {
		if env.SerialStateDB.GetBalance(addr).Cmp(InitBalance) != 0 {
			panic("serial state db balance mismatch")
		}
		if smartschedDb.GetBalance(addr).Cmp(InitBalance) != 0 {
			panic("smartsched state db balance mismatch")
		}
		if occDb.GetBalance(addr).Cmp(InitBalance) != 0 {
			panic("occ state db balance mismatch")
		}
		if stmdb.GetBalance(addr).Cmp(InitBalance) != 0 {
			panic("stm state db balance mismatch")
		}
		if twoPlDb.GetBalance(addr).Cmp(InitBalance) != 0 {
			panic("two pl state db balance mismatch")
		}
		if spectrumDb.GetBalance(addr).Cmp(InitBalance) != 0 {
			panic("spectrum state db balance mismatch")
		}
		if dmvccDb.GetBalance(addr).Cmp(InitBalance) != 0 {
			panic("dmvcc state db balance mismatch")
		}
		if parEvmDb.GetBalance(addr).Cmp(InitBalance) != 0 {
			panic("parevm state db balance mismatch")
		}
		if schedOnlySchedDb.GetBalance(addr).Cmp(InitBalance) != 0 {
			panic("sched only sched state db balance mismatch")
		}
		if schedOnlyPreCommitDb.GetBalance(addr).Cmp(InitBalance) != 0 {
			panic("sched only precommit state db balance mismatch")
		}
	}
	return env
}
