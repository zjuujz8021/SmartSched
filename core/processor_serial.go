package core

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

type ProcessStats struct {
	TxCount int
	// used for concurrent execution
	Aborted                int
	StaticCriticalPathLen  int
	DynamicCriticalPathLen int

	Init             time.Duration
	Process          time.Duration
	CriticalPathExec time.Duration
	Validate         time.Duration
	Finalize         time.Duration

	AvgRunningWorker float64

	TxStats []TxExecStats

	TxProcessTime time.Duration
	TxProcessCnt  int
}

func (s *ProcessStats) Add(other *ProcessStats) {
	s.TxCount += other.TxCount
	s.Aborted += other.Aborted
	s.StaticCriticalPathLen += other.StaticCriticalPathLen
	s.DynamicCriticalPathLen += other.DynamicCriticalPathLen

	s.Init += other.Init
	s.Process += other.Process
	s.CriticalPathExec += other.CriticalPathExec
	s.Validate += other.Validate
	s.Finalize += other.Finalize

	s.AvgRunningWorker += other.AvgRunningWorker
	s.TxProcessTime += other.TxProcessTime
	s.TxProcessCnt += other.TxProcessCnt
}

type TxExecStats struct {
	ExecCount   int
	ProcessTime time.Duration
}

type ReplayProcessor struct {
	config *params.ChainConfig
	bc     *BlockChain

	useTxLevelState bool
}

func NewReplayProcessor(config *params.ChainConfig, bc *BlockChain, useTxLevelState bool) *ReplayProcessor {
	return &ReplayProcessor{
		config:          config,
		bc:              bc,
		useTxLevelState: useTxLevelState,
	}
}

func (p *ReplayProcessor) ExecuteMsgs(msgs []*Message, header *types.Header, cfg vm.Config, statedb *state.StateDB) ([]*ExecutionResult, *ProcessStats, uint64, error) {
	pstart := time.Now()
	var (
		results  = make([]*ExecutionResult, 0)
		gp       = new(GasPool).AddGas(header.GasLimit)
		usedGas  = uint64(0)
		is158    = p.config.IsEIP158(header.Number)
		blockCtx = NewEVMBlockContext(header, p.bc, &header.Coinbase)
		vmenv    = vm.NewEVM(blockCtx, vm.TxContext{}, statedb, p.config, vm.Config{})
		stats    = &ProcessStats{TxCount: len(msgs)}
	)
	for i, msg := range msgs {
		if p.useTxLevelState {
			statedb, _ = p.bc.GetStateDB(header.Number.Uint64(), i)
		}
		statedb.SetTxContext(common.BigToHash(big.NewInt(int64(i))), i)
		vmenv.Reset(NewEVMTxContext(msg), statedb)
		txstart := time.Now()
		result, err := ApplyMessage(vmenv, msg, gp)
		stats.TxProcessTime += time.Since(txstart)
		if err != nil {
			return nil, nil, 0, err
		} else if statedb.Error() != nil {
			return nil, nil, 0, statedb.Error()
		}
		usedGas += result.UsedGas
		results = append(results, result)
		statedb.Finalise(is158)
	}
	stats.Process = time.Since(pstart)
	return results, stats, usedGas, nil
}

func (p *ReplayProcessor) ProcessBlock(block *types.Block, cfg vm.Config, statedb *state.StateDB) ([]*ExecutionResult, *ProcessStats, uint64, error) {
	start := time.Now()
	var (
		results  = make([]*ExecutionResult, 0)
		gp       = new(GasPool).AddGas(block.GasLimit())
		usedGas  = uint64(0)
		txs      = block.Transactions()
		is158    = p.config.IsEIP158(block.Number())
		blockCtx = NewEVMBlockContext(block.Header(), p.bc, &block.Header().Coinbase)
		signer   = types.MakeSigner(p.config, block.Number(), block.Time())
		vmenv    = vm.NewEVM(blockCtx, vm.TxContext{}, statedb, p.config, vm.Config{})
	)
	for i, tx := range txs {
		msg, err := TransactionToMessage(tx, signer, block.BaseFee())
		if err != nil {
			return nil, nil, 0, err
		}
		if p.useTxLevelState {
			statedb, _ = p.bc.GetStateDB(block.NumberU64(), i)
		}
		statedb.SetTxContext(tx.Hash(), i)
		vmenv.Reset(NewEVMTxContext(msg), statedb)
		result, err := ApplyMessage(vmenv, msg, gp)
		if err != nil {
			return nil, nil, 0, err
		} else if statedb.Error() != nil {
			return nil, nil, 0, statedb.Error()
		}
		usedGas += result.UsedGas
		results = append(results, result)
		statedb.Finalise(is158)
	}
	return results, &ProcessStats{TxCount: block.Transactions().Len(), Process: time.Since(start)}, usedGas, nil
}
