package core

import (
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

var _ ParallelProcessor = (*TwoPLProcessor)(nil)

type TwoPLProcessor struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain

	lock   sync.Mutex
	txnSet *TxnSet
	ctx    *ProcessContext
	stats  *ProcessStats

	parallelConfig ParallelConfig

	evms   []*vm.EVM
	basedb *state.MVCCBaseStateDB
}

func NewTwoPLProcessor(config *params.ChainConfig, bc *BlockChain, parallelConfig ParallelConfig) *TwoPLProcessor {
	return &TwoPLProcessor{
		config:         config,
		bc:             bc,
		parallelConfig: parallelConfig,
	}
}

func (p *TwoPLProcessor) Name() string {
	return TWOPL
}

func (p *TwoPLProcessor) ExecuteMsgs(header *types.Header, cfg vm.Config, input *ParallelInput) ([]*ExecutionResult, *ProcessStats, *DebugInfo, uint64, error) {
	statedb := input.TwoPLStateDB
	msgs := input.Msgs
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
		mvccBaseDB:          statedb,
		mvccStateDBs:        make([]*state.MVCCStateDB, txCount),
		results:             make([]*ExecutionResult, txCount),
		errors:              make([]error, txCount),
		blockCtx:            blkCtx,
		commutativeBlockCtx: blkCtx,
		msgs:                msgs,
		vmConfig:            cfg,
		is158:               p.config.IsEIP158(header.Number),
	}
	p.ctx.commutativeBlockCtx.CanTransfer = OptimisticTransfer

	p.txnSet = NewTxnSet(txCount)

	completeCh := make(chan struct{})

	p.evms = make([]*vm.EVM, txCount)
	statedb.Init2PL(txCount, func(txIndex int) {
		p.evms[txIndex].Cancel()
	})
	p.basedb = statedb

	for i := range p.ctx.msgs {
		p.ctx.mvccStateDBs[i] = state.NewMVCCStateDB(statedb, i, common.BigToHash(big.NewInt(int64(i))), nil)
		// p.ctx.mvccStateDBs[i].LoadAccount(msg.From)
		// if msg.To != nil {
		// 	p.ctx.mvccStateDBs[i].LoadAccountAndCode(*msg.To)
		// }
		if p.parallelConfig.DisableNonceValidate {
			p.ctx.mvccStateDBs[i].DisableNonceCheck()
		}
	}
	start := time.Now()
	for i := 0; i < p.parallelConfig.WorkerNum; i++ {
		go p.Worker(completeCh)
	}
	<-completeCh
	p.stats.Process = time.Since(start)
	usedGas := uint64(0)
	for i := 0; i < txCount; i++ {
		usedGas += p.ctx.results[i].UsedGas
	}
	for _, stat := range p.stats.TxStats {
		p.stats.TxProcessTime += stat.ProcessTime
		p.stats.TxProcessCnt += stat.ExecCount
	}
	return p.ctx.results, p.stats, nil, usedGas, statedb.Error()
}

func (p *TwoPLProcessor) Worker(completeCh chan<- struct{}) {
	var (
		task    = WorkerIdle
		txIndex int
	)

	for {
		if task == WorkerIdle {
			p.lock.Lock()
			if p.txnSet.ExistValTxn() {
				task = WorkerValidate
				txIndex = p.txnSet.GetValTxn()
			} else if p.txnSet.ExistIdleTxn() {
				task = WorkerExecute
				txIndex = p.txnSet.GetIdleTxn()
			}
			p.lock.Unlock()
		}
		switch task {
		case WorkerIdle:
			return
		case WorkerValidate:
			start := time.Now()
			if !p.validate(txIndex) {
				p.ctx.mvccStateDBs[txIndex].Abort()
				p.stats.Aborted++
				p.execute(txIndex, false)
				// sanity check
				// if !p.validate(txIndex) {
				// 	// panic(fmt.Sprintf("block %v - tx %d is not validated after execution, exec err: %v",
				// 	// 	p.ctx.blockCtx.BlockNumber.Uint64(), txIndex, p.ctx.errors[txIndex]))
				// 	log.Warn("Invalid tx", "block", p.ctx.blockCtx.BlockNumber.Uint64(), "tx", txIndex,
				// 		"exec err", p.ctx.errors[txIndex], "scheme", p.Name())
				// }
			}
			p.stats.Validate += time.Since(start)
			start = time.Now()
			p.ctx.mvccStateDBs[txIndex].FinalizeToBaseStateDB(p.ctx.is158)
			p.basedb.ReleaseLocks(txIndex)
			p.stats.Finalize += time.Since(start)
			p.lock.Lock()
			ok := p.txnSet.CommitTxn()
			p.lock.Unlock()
			if ok {
				close(completeCh)
				return
			}
			task = WorkerIdle
		case WorkerExecute:
			needReexecute := p.execute(txIndex, p.parallelConfig.UseCommutative)
			if needReexecute {
				p.ctx.mvccStateDBs[txIndex].Abort()
				p.stats.Aborted++
				continue
			} else {
				task = WorkerIdle
			}
			p.lock.Lock()
			p.txnSet.PutValTxn(txIndex)
			p.lock.Unlock()
		}
	}
}

func (p *TwoPLProcessor) execute(txIndex int, useCommutative bool) (needReexecute bool) {
	statedb := p.ctx.mvccStateDBs[txIndex]
	if useCommutative {
		statedb.EnableCommutative()
	} else {
		statedb.DisableCommutative()
	}
	if !p.ctx.msgs[txIndex].SkipAccountChecks {
		err := statedb.InitSender(p.ctx.msgs[txIndex].From, p.ctx.msgs[txIndex].Nonce)
		if err != nil {
			p.ctx.errors[txIndex] = err
			return false
		}
	}

	var vmenv *vm.EVM
	if useCommutative {
		vmenv = vm.NewEVM(p.ctx.commutativeBlockCtx, NewEVMTxContext(p.ctx.msgs[txIndex]), statedb, p.config, p.ctx.vmConfig)
	} else {
		vmenv = vm.NewEVM(p.ctx.blockCtx, NewEVMTxContext(p.ctx.msgs[txIndex]), statedb, p.config, p.ctx.vmConfig)
	}
	p.evms[txIndex] = vmenv
	statedb.Init2PL(vmenv.Cancelled)

	start := time.Now()
	execResult, err := ApplyMessage(vmenv, p.ctx.msgs[txIndex], new(GasPool).AddGas(p.ctx.blockCtx.GasLimit))
	p.stats.TxStats[txIndex].ProcessTime += time.Since(start)
	p.stats.TxStats[txIndex].ExecCount++
	p.ctx.errors[txIndex] = err

	if err != nil {
		// special cases: consensus precedure adjustment causes some txs to be not applied
		// assign gasUsed to be 0 to indicate that the tx is not applied
		p.ctx.results[txIndex] = &ExecutionResult{Err: err}
		return false
	}
	if vmenv.Cancelled() {
		return true
	}
	p.ctx.results[txIndex] = execResult
	return false
}

func (p *TwoPLProcessor) validate(txIndex int) bool {
	// some txs may be not applied, because we adjust the consensus precedure, we should omit them
	if p.ctx.errors[txIndex] != nil {
		return false
	}
	if p.evms[txIndex].Cancelled() {
		return false
	}
	if conflict := p.ctx.mvccStateDBs[txIndex].Validate(); conflict != state.NoConflict {
		// return false
		// log.Warn("conflict detected", "txIndex", txIndex, "conflict", conflict)
		// fmt.Println("conflict detected", txIndex, conflict)
		return false
	}
	return true
}
