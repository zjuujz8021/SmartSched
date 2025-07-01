package core

import (
	"context"
	"time"

	blockstm "github.com/crypto-org-chain/go-block-stm"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

var _ ParallelProcessor = (*STMProcessor)(nil)

type STMProcessor struct {
	config *params.ChainConfig
	bc     *BlockChain

	ctx   *ProcessContext
	stats *ProcessStats

	parallelConfig ParallelConfig
}

func NewSTMProcessor(config *params.ChainConfig, bc *BlockChain, parallelConfig ParallelConfig) *STMProcessor {
	return &STMProcessor{
		config:         config,
		bc:             bc,
		parallelConfig: parallelConfig,
	}
}

func (p *STMProcessor) Name() string {
	return STM
}

func (p *STMProcessor) ExecuteMsgs(header *types.Header, cfg vm.Config, input *ParallelInput) ([]*ExecutionResult, *ProcessStats, *DebugInfo, uint64, error) {
	txCount := len(input.Msgs)
	if txCount == 0 {
		return nil, &ProcessStats{}, nil, 0, nil
	}
	p.stats = &ProcessStats{
		TxCount: txCount,
		TxStats: make([]TxExecStats, txCount),
	}
	blkCtx := NewEVMBlockContext(header, p.bc, &header.Coinbase)
	p.ctx = &ProcessContext{
		results:             make([]*ExecutionResult, txCount),
		errors:              make([]error, txCount),
		blockCtx:            blkCtx,
		commutativeBlockCtx: blkCtx,
		msgs:                input.Msgs,
		vmConfig:            cfg,
		is158:               p.config.IsEIP158(header.Number),
	}

	start := time.Now()
	blockstm.ExecuteBlock(context.Background(), txCount, state.STMStores, input.STMStateDB, p.parallelConfig.WorkerNum, p.Executor)
	p.stats.Process = time.Since(start)
	usedGas := uint64(0)
	for i := 0; i < txCount; i++ {
		usedGas += p.ctx.results[i].UsedGas
	}
	for _, stat := range p.stats.TxStats {
		p.stats.TxProcessTime += stat.ProcessTime
		p.stats.TxProcessCnt += stat.ExecCount
	}
	return p.ctx.results, p.stats, nil, usedGas, nil
}

func (p *STMProcessor) Executor(txIndex blockstm.TxnIndex, store blockstm.MultiStore) {
	statedb, _ := state.New(common.Hash{}, state.NewBlockSTMTrie(store), nil)
	vmConfig := p.ctx.vmConfig
	vmenv := vm.NewEVM(p.ctx.blockCtx, NewEVMTxContext(p.ctx.msgs[txIndex]), statedb, p.config, vmConfig)

	start := time.Now()
	execResult, err := ApplyMessage(vmenv, p.ctx.msgs[txIndex], new(GasPool).AddGas(p.ctx.blockCtx.GasLimit))
	statedb.WriteToSTMStore(p.ctx.is158)
	p.stats.TxStats[txIndex].ProcessTime += time.Since(start)
	p.stats.TxStats[txIndex].ExecCount++
	p.ctx.errors[txIndex] = err

	if err != nil {
		p.ctx.results[txIndex] = &ExecutionResult{Err: err}
		return
	}
	p.ctx.results[txIndex] = execResult
}
