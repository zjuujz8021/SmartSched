package nop

import (
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/cmd/benchmark/stateinit"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

const (
	nopBytecode = `0x608060405234801561001057600080fd5b5060b38061001f6000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c8063c040622614602d575b600080fd5b60336035565b005b60008090505b60c8811015607b5760008160405160200180828152602001915050604051602081830303815290604052805190602001209050508080600101915050603b565b5056fea265627a7a72315820634011b1ff88e6f9e78fc23d4002ec8876ee855144e09189433e0f715dd6a79e64736f6c63430005110032`

	abiString = `[{"constant":true,"inputs":[],"name":"run","outputs":[],"payable":false,"stateMutability":"pure","type":"function"}]`
)

var nopabi abi.ABI

func init() {
	var err error
	nopabi, err = abi.JSON(strings.NewReader(abiString))
	if err != nil {
		panic(err)
	}
}

type NopEnv struct {
	*stateinit.StateEnv
	ContractAddr common.Address
}

func nopCreateMsg(sender common.Address) *core.Message {
	msg := stateinit.NewMessage()
	msg.From = sender
	msg.Data = common.FromHex(nopBytecode)
	return msg
}

func CreateNopEnv(n int) *NopEnv {
	env := &NopEnv{
		StateEnv:     stateinit.CreateStateEnv(n),
		ContractAddr: crypto.CreateAddress(stateinit.AdminAddress, 0),
	}
	header := stateinit.NewMockHeader()
	msgs := []*core.Message{nopCreateMsg(stateinit.AdminAddress)}
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
		Msgs:              msgs,
		OCCStateDB:        env.OCCStateDB,
		TwoPLStateDB:      env.TwoPLStateDB,
		STMStateDB:        env.STMStateDB,
		SpectrumStateDB:   env.SpectrumStateDB,
		DmvccStateDB:      env.DmvccStateDB,
		SmartSchedStateDB: env.SmartSchedStateDB,
	}

	processors := core.CreateParallelProcessors(params.MainnetChainConfig, nil, core.ParallelProcessors, cfg)

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
	return env
}

func CreateNopMsg(contractAddr, fromAddr common.Address) *core.Message {
	msg := stateinit.NewMessage()
	data, err := nopabi.Pack("run")
	if err != nil {
		panic(err)
	}
	msg.From = fromAddr
	msg.To = &contractAddr
	msg.Data = data
	return msg
}
