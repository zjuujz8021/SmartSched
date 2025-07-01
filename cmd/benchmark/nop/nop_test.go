package nop

import (
	"testing"

	"github.com/ethereum/go-ethereum/cmd/benchmark/stateinit"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

func TestNopCreate(t *testing.T) {
	env := CreateNopEnv(10)
	statedb := state.NewMVCCStateDB(env.SmartSchedStateDB, 1, common.Hash{}, nil)
	code := statedb.GetCode(env.ContractAddr)
	t.Error("addr", env.ContractAddr, "codesize", len(code))
	results, _, _, _, err := core.NewSmartProcessor(params.MainnetChainConfig, nil, core.ParallelConfig{WorkerNum: 1}).
		ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, &core.ParallelInput{
			Msgs:              []*core.Message{CreateNopMsg(env.ContractAddr, common.Address{})},
			SmartSchedStateDB: env.SmartSchedStateDB,
		})
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.Err != nil {
			t.Fatal(result.Err)
		} else {
			t.Error("gasused", result.UsedGas, "ret", hexutil.Encode(result.ReturnData))
		}
	}
}
