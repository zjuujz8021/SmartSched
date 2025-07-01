package erc20

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/cmd/benchmark/stateinit"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

func TestPackERC20Transfer(t *testing.T) {
	// from := common.HexToAddress("0x0000000000000000000000000000000000000111")
	to := common.HexToAddress("0x0000000000000000000000000000000000000222")
	amount := big.NewInt(1000000000000000000)
	packed, err := erc20abi.Pack("transfer", to, amount)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(hex.EncodeToString(packed))
}

func TestCreateERC20CreationTx(t *testing.T) {
	base := stateinit.CreateEmptyMvccBaseStateDB()
	db := state.NewMVCCStateDB(base, 0, common.Hash{}, nil)
	db.AddBalance(stateinit.AdminAddress, uint256.NewInt(1000000000000000000))
	// db.AddBalance(common.Address{}, uint256.NewInt(1000000000000000000))
	if db.Validate() != state.NoConflict {
		t.Error("validate failed")
	}
	db.FinalizeToBaseStateDB(true)

	msg := CreateERC20CreationMsg(stateinit.AdminAddress, common.FromHex(ERC20DeployCode))
	header := &types.Header{
		Number:     big.NewInt(20000000),
		Difficulty: big.NewInt(0),
		GasLimit:   stateinit.DefaultBlockGasLimit,
		BaseFee:    big.NewInt(0),
		Time:       uint64(time.Now().Unix()),
	}
	p := core.NewSmartProcessor(params.MainnetChainConfig, nil, core.ParallelConfig{WorkerNum: 1, UseCommutative: true})
	res, _, _, gasUsed, err := p.ExecuteMsgs(header, vm.Config{NoBaseFee: true}, &core.ParallelInput{Msgs: []*core.Message{msg}, SmartSchedStateDB: base})
	if err != nil {
		t.Error(err)
	}
	fmt.Println("creation", gasUsed, res[0].UsedGas, res[0].Err)
	contractAddr := crypto.CreateAddress(stateinit.AdminAddress, 0)
	fmt.Println("create contract done", "contract addr", contractAddr, "code length", len(db.GetCode(contractAddr)), "nonce", db.GetNonce(contractAddr))

	// now mint tokens
	msg, err = CreateMintMsg(contractAddr, stateinit.AdminAddress, big.NewInt(1000000000000000000))
	if err != nil {
		t.Error(err)
	}
	res, _, _, gasUsed, err = p.ExecuteMsgs(header, vm.Config{NoBaseFee: true}, &core.ParallelInput{Msgs: []*core.Message{msg}, SmartSchedStateDB: base})
	if err != nil {
		t.Error(err)
	}
	fmt.Println("mint", gasUsed, res[0].UsedGas, res[0].Err)

	// now check balance
	msg, err = CreateBalanceOfMsg(contractAddr, stateinit.AdminAddress)
	if err != nil {
		t.Error(err)
	}
	res, _, _, _, err = p.ExecuteMsgs(header, vm.Config{NoBaseFee: true}, &core.ParallelInput{Msgs: []*core.Message{msg}, SmartSchedStateDB: base})
	if err != nil {
		t.Error(err)
	}
	fmt.Println("balance of admin", new(big.Int).SetBytes(res[0].ReturnData))

	// now transfer to max address
	msg, err = CreateTransferMsg(contractAddr, stateinit.AdminAddress, common.MaxAddress, big.NewInt(1))
	if err != nil {
		t.Error(err)
	}
	res, _, _, gasUsed, err = p.ExecuteMsgs(header, vm.Config{NoBaseFee: true}, &core.ParallelInput{Msgs: []*core.Message{msg}, SmartSchedStateDB: base})
	if err != nil {
		t.Error(err)
	}
	fmt.Println("transfer to max address", gasUsed, res[0].UsedGas, res[0].Err)

	// now check balance
	msg1, _ := CreateBalanceOfMsg(contractAddr, common.MaxAddress)
	msg2, _ := CreateBalanceOfMsg(contractAddr, stateinit.AdminAddress)
	res, _, _, _, err = p.ExecuteMsgs(header, vm.Config{NoBaseFee: true}, &core.ParallelInput{Msgs: []*core.Message{msg1, msg2}, SmartSchedStateDB: base})
	if err != nil {
		t.Error(err)
	}
	fmt.Println("balance of max address", new(big.Int).SetBytes(res[0].ReturnData))
	fmt.Println("balance of admin", new(big.Int).SetBytes(res[1].ReturnData))
}

func TestCreateERC20Env(t *testing.T) {
	n := 32
	env := CreateERC20Env(n)
	fmt.Println(env.TokenAddr)
	msgs := make([]*core.Message, n)
	for i := 0; i < n; i++ {
		msgs[i], _ = CreateBalanceOfMsg(env.TokenAddr, env.Accounts[i])
	}
	serialResults, _, _, err := core.NewReplayProcessor(params.MainnetChainConfig, nil, false).ExecuteMsgs(msgs, stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, env.SerialStateDB)
	if err != nil {
		t.Error(err)
	}
	cfg := core.ParallelConfig{
		WorkerNum: 1,
	}
	input := &core.ParallelInput{
		Msgs:              msgs,
		OCCStateDB:        env.OCCStateDB,
		TwoPLStateDB:      env.TwoPLStateDB,
		STMStateDB:        env.STMStateDB,
		SpectrumStateDB:   env.SpectrumStateDB,
		DmvccStateDB:      env.DmvccStateDB,
		SmartSchedStateDB: env.SmartSchedStateDB,
	}
	occResults, _, _, _, err := core.NewOccProcessor(params.MainnetChainConfig, nil, cfg).ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	if err != nil {
		t.Error(err)
	}
	smartschedResults, _, _, _, err := core.NewSmartProcessor(params.MainnetChainConfig, nil, cfg).ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	if err != nil {
		t.Error(err)
	}
	stm := core.NewSTMProcessor(params.MainnetChainConfig, nil, cfg)
	stmResults, _, _, _, err := stm.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	if err != nil {
		t.Error(err)
	}
	twoPl := core.NewTwoPLProcessor(params.MainnetChainConfig, nil, cfg)
	twoPlResults, _, _, _, err := twoPl.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	if err != nil {
		t.Error(err)
	}
	spectrum := core.NewOccProcessor(params.MainnetChainConfig, nil, cfg)
	spectrumResults, _, _, _, err := spectrum.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	if err != nil {
		t.Error(err)
	}
	dmvcc := core.NewDmvccProcessor(params.MainnetChainConfig, nil, cfg)
	dmvccResults, _, _, _, err := dmvcc.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < n; i++ {
		fmt.Println(env.Accounts[i], "serial", new(big.Int).SetBytes(serialResults[i].ReturnData), "occ", new(big.Int).SetBytes(occResults[i].ReturnData), "smartsched", new(big.Int).SetBytes(smartschedResults[i].ReturnData), "stm", new(big.Int).SetBytes(stmResults[i].ReturnData), "two pl", new(big.Int).SetBytes(twoPlResults[i].ReturnData), "spectrum", new(big.Int).SetBytes(spectrumResults[i].ReturnData), "dmvcc", new(big.Int).SetBytes(dmvccResults[i].ReturnData))
	}
}

func TestERC20Benchmark(t *testing.T) {
	accounts := 50
	txs := 500
	cfg := core.ParallelConfig{
		WorkerNum:            16,
		UseCommutative:       true,
		TrackRWSet:           true,
		DisableNonceValidate: true,
	}
	env := CreateERC20Env(accounts)
	input := &core.ParallelInput{
		Msgs:              CreateERC20TransferUniformDataSet(env, CreateUniformTransferPairs(accounts, 1, txs), big.NewInt(5))[0],
		OCCStateDB:        env.OCCStateDB,
		TwoPLStateDB:      env.TwoPLStateDB,
		STMStateDB:        env.STMStateDB,
		SpectrumStateDB:   env.SpectrumStateDB,
		DmvccStateDB:      env.DmvccStateDB,
		ParEVMStateDB:     env.ParEVMStateDB,
		SmartSchedStateDB: env.SmartSchedStateDB,
	}

	serial := core.NewReplayProcessor(params.MainnetChainConfig, nil, false)
	occ := core.NewOccProcessor(params.MainnetChainConfig, nil, cfg)
	smartsched := core.NewSmartProcessor(params.MainnetChainConfig, nil, cfg)
	stm := core.NewSTMProcessor(params.MainnetChainConfig, nil, cfg)
	twoPl := core.NewTwoPLProcessor(params.MainnetChainConfig, nil, cfg)
	spectrum := core.NewSpectrumProcessor(params.MainnetChainConfig, nil, cfg)
	dmvcc := core.NewDmvccProcessor(params.MainnetChainConfig, nil, cfg)
	parEVM := core.NewParEVMProcessor(params.MainnetChainConfig, nil, cfg)
	fmt.Println("start executing ...")
	serialResults, serialStats, _, err := serial.ExecuteMsgs(input.Msgs, stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, env.SerialStateDB)
	if err != nil {
		panic(err)
	}
	for _, result := range serialResults {
		if result.Err != nil {
			panic(result.Err)
		}
	}
	occResults, occStats, occdbg, _, err := occ.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	if err != nil {
		panic(err)
	}
	for _, result := range occResults {
		if result.Err != nil {
			panic(result.Err)
		}
	}
	input.RWSets = occdbg.RWSets
	// for _, rwset := range occdbg.RWSets {
	// 	rwset.AccountWrite = nil
	// 	rwset.BalanceWrite = nil
	// 	rwset.NonceWrite = nil
	// }
	smartschedResults, smartstats, _, _, err := smartsched.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	if err != nil {
		panic(err)
	}
	for _, result := range smartschedResults {
		if result.Err != nil {
			panic(result.Err)
		}
	}
	stmResults, stmStats, _, _, err := stm.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	if err != nil {
		panic(err)
	}
	for _, result := range stmResults {
		if result.Err != nil {
			panic(result.Err)
		}
	}
	twoPlResults, twoPlStats, _, _, err := twoPl.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	if err != nil {
		panic(err)
	}
	for _, result := range twoPlResults {
		if result.Err != nil {
			panic(result.Err)
		}
	}
	spectrumResults, spectrumStats, _, _, err := spectrum.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	if err != nil {
		panic(err)
	}
	for _, result := range spectrumResults {
		if result.Err != nil {
			panic(result.Err)
		}
	}
	dmvccResults, dmvccStats, _, _, err := dmvcc.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	if err != nil {
		panic(err)
	}
	for _, result := range dmvccResults {
		if result.Err != nil {
			panic(result.Err)
		}
	}
	parEVMResults, parEVMStats, _, _, err := parEVM.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)

	balanceOfMsgs := CreateERC20BalanceOfDataSet(env)
	input.Msgs = balanceOfMsgs
	input.RWSets = nil
	serialResults, _, _, _ = serial.ExecuteMsgs(balanceOfMsgs, stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, env.SerialStateDB)
	occResults, _, _, _, _ = occ.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	smartschedResults, _, _, _, _ = smartsched.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	stmResults, _, _, _, _ = stm.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	twoPlResults, _, _, _, _ = twoPl.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	spectrumResults, _, _, _, _ = spectrum.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	dmvccResults, _, _, _, _ = dmvcc.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)
	parEVMResults, _, _, _, _ = parEVM.ExecuteMsgs(stateinit.NewMockHeader(), vm.Config{NoBaseFee: true}, input)

	for i := 0; i < accounts; i++ {
		fmt.Println(env.Accounts[i], "serial", new(big.Int).SetBytes(serialResults[i].ReturnData), "occ", new(big.Int).SetBytes(occResults[i].ReturnData), "smartsched", new(big.Int).SetBytes(smartschedResults[i].ReturnData), "stm", new(big.Int).SetBytes(stmResults[i].ReturnData), "two pl", new(big.Int).SetBytes(twoPlResults[i].ReturnData), "spectrum", new(big.Int).SetBytes(spectrumResults[i].ReturnData), "dmvcc", new(big.Int).SetBytes(dmvccResults[i].ReturnData), "parEVM", new(big.Int).SetBytes(parEVMResults[i].ReturnData))
	}

	fmt.Println("process", serialStats.Process, occStats.Process, smartstats.Process, stmStats.Process, twoPlStats.Process, spectrumStats.Process, dmvccStats.Process, parEVMStats.Process)
	fmt.Println("validate", serialStats.Validate, occStats.Validate, smartstats.Validate, stmStats.Validate, twoPlStats.Validate, spectrumStats.Validate, dmvccStats.Validate, parEVMStats.Validate)
	fmt.Println("finalize", serialStats.Finalize, occStats.Finalize, smartstats.Finalize, stmStats.Finalize, twoPlStats.Finalize, spectrumStats.Finalize, dmvccStats.Finalize, parEVMStats.Finalize)
	fmt.Println("finalize", serialStats.Finalize, occStats.Finalize, smartstats.Finalize, stmStats.Finalize, twoPlStats.Finalize, spectrumStats.Finalize, dmvccStats.Finalize, parEVMStats.Finalize)
	fmt.Println("aborted", serialStats.Aborted, occStats.Aborted, smartstats.Aborted, stmStats.Aborted, twoPlStats.Aborted, spectrumStats.Aborted, dmvccStats.Aborted, parEVMStats.Aborted)
}
