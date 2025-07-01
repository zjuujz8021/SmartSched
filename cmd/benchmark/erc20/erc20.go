package erc20

import (
	"bytes"
	"math/big"
	"math/rand"
	"time"

	"github.com/ethereum/go-ethereum/cmd/benchmark/stateinit"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

type ERC20Env struct {
	*stateinit.StateEnv
	TokenAddr common.Address
}

func CreateERC20CreationMsg(from common.Address, code []byte) *core.Message {
	msg := stateinit.NewMessage()
	msg.From = from
	msg.Data = code
	return msg
}

func CreateMintMsg(tokenAddr, to common.Address, amount *big.Int) (*core.Message, error) {
	msg := stateinit.NewMessage()
	data, err := erc20abi.Pack("mint", to, amount)
	if err != nil {
		return nil, err
	}
	msg.To = &tokenAddr
	msg.Data = data
	return msg, nil
}

func CreateBalanceOfMsg(tokenAddr, owner common.Address) (*core.Message, error) {
	data, err := erc20abi.Pack("balanceOf", owner)
	if err != nil {
		return nil, err
	}
	msg := stateinit.NewMessage()
	msg.To = &tokenAddr
	msg.Data = data
	return msg, nil
}

func CreateTransferMsg(tokenAddr, from, to common.Address, amount *big.Int) (*core.Message, error) {
	data, err := erc20abi.Pack("transfer", to, amount)
	if err != nil {
		return nil, err
	}
	msg := stateinit.NewMessage()
	msg.From = from
	msg.To = &tokenAddr
	msg.Data = data
	return msg, nil
}

func CreateTransferDataSet(token common.Address, pairs [][2]common.Address, amounts []*big.Int) []*core.Message {
	msgs := make([]*core.Message, len(pairs))
	for i, pair := range pairs {
		msgs[i], _ = CreateTransferMsg(token, pair[0], pair[1], amounts[i])
	}
	return msgs
}

func CreateERC20Env(n int) *ERC20Env {
	env := &ERC20Env{
		StateEnv:  stateinit.CreateStateEnv(n),
		TokenAddr: crypto.CreateAddress(stateinit.AdminAddress, 0),
	}
	header := stateinit.NewMockHeader()
	msgs := make([]*core.Message, n+1)
	msgs[0] = CreateERC20CreationMsg(stateinit.AdminAddress, common.FromHex(ERC20DeployCode))
	for i := 0; i < n; i++ {
		msgs[i+1], _ = CreateMintMsg(env.TokenAddr, env.Accounts[i], stateinit.InitBalance.ToBig())
	}

	// deploy on serial state db
	serial := core.NewReplayProcessor(params.MainnetChainConfig, nil, false)
	results, _, _, err := serial.ExecuteMsgs(msgs, header, vm.Config{NoBaseFee: true}, env.SerialStateDB)
	if err != nil {
		panic(err)
	}
	for _, result := range results {
		if result.Err != nil {
			panic(result.Err)
		}
	}
	cfg := core.ParallelConfig{WorkerNum: 1}
	input := &core.ParallelInput{
		Msgs:                           msgs,
		OCCStateDB:                     env.OCCStateDB,
		TwoPLStateDB:                   env.TwoPLStateDB,
		STMStateDB:                     env.STMStateDB,
		SpectrumStateDB:                env.SpectrumStateDB,
		DmvccStateDB:                   env.DmvccStateDB,
		ParEVMStateDB:                  env.ParEVMStateDB,
		SmartSchedStateDB:              env.SmartSchedStateDB,
		SmartSchedOnlySchedStateDB:     env.SmartSchedOnlySchedStateDB,
		SmartSchedOnlyPreCommitStateDB: env.SmartSchedOnlyPreCommitStateDB,
	}

	processors := core.CreateParallelProcessors(params.MainnetChainConfig, nil, core.AllParallelProcessors, cfg)

	for _, p := range processors {
		results, _, _, _, err := p.ExecuteMsgs(header, vm.Config{}, input)
		if err != nil {
			panic(err)
		}
		for _, result := range results {
			if result.Err != nil {
				panic(result.Err)
			}
		}
	}

	// sanity check
	// serial first
	readBalanceMsgs := make([]*core.Message, n)
	for i := 0; i < n; i++ {
		readBalanceMsgs[i], _ = CreateBalanceOfMsg(env.TokenAddr, env.Accounts[i])
	}
	input.Msgs = readBalanceMsgs
	serialResults, _, _, err := serial.ExecuteMsgs(readBalanceMsgs, header, vm.Config{NoBaseFee: true}, env.SerialStateDB)
	if err != nil {
		panic(err)
	}

	parallelResults := make([][]*core.ExecutionResult, len(processors))
	for i, p := range processors {
		results, _, _, _, err := p.ExecuteMsgs(header, vm.Config{}, input)
		if err != nil {
			panic(err)
		}
		parallelResults[i] = results
	}

	for i := 0; i < n; i++ {
		if stateinit.InitBalance.ToBig().Cmp(new(big.Int).SetBytes(serialResults[i].ReturnData)) != 0 {
			panic("balance mismatch")
		}
		for j := range parallelResults {
			if !bytes.Equal(serialResults[i].ReturnData, parallelResults[j][i].ReturnData) {
				panic("balance mismatch")
			}
		}
	}
	return env
}

func CreateUniformTransferPairs(accounts, blockNum, txsPerBlock int) [][][2]int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	pairs := make([][][2]int, blockNum)
	for i := 0; i < blockNum; i++ {
		pairs[i] = make([][2]int, txsPerBlock)
		for j := 0; j < txsPerBlock; j++ {
			from := r.Intn(accounts)
			to := r.Intn(accounts)
			if from == to {
				to = (to + 1) % accounts
			}
			pairs[i][j] = [2]int{from, to}
		}
	}
	return pairs
}

func CreateERC20TransferUniformDataSet(env *ERC20Env, pairs [][][2]int, amount *big.Int) [][]*core.Message {
	blockNum := len(pairs)
	txsPerBlock := len(pairs[0])
	blocks := make([][]*core.Message, blockNum)
	for i := 0; i < blockNum; i++ {
		msgs := make([]*core.Message, txsPerBlock)
		for j := 0; j < txsPerBlock; j++ {
			msgs[j], _ = CreateTransferMsg(env.TokenAddr, env.Accounts[pairs[i][j][0]], env.Accounts[pairs[i][j][1]], amount)
		}
		blocks[i] = msgs
	}
	return blocks
}

func CreateERC20BalanceOfDataSet(env *ERC20Env) []*core.Message {
	msgs := make([]*core.Message, len(env.Accounts))
	for i := 0; i < len(env.Accounts); i++ {
		msgs[i], _ = CreateBalanceOfMsg(env.TokenAddr, env.Accounts[i])
	}
	return msgs
}
