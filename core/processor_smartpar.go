package core

import (
	"encoding/json"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/tracers/logger"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

var _ ParallelProcessor = (*SmartProcessor)(nil)

type SmartProcessor struct {
	config *params.ChainConfig
	bc     *BlockChain

	scheduler *SmartScheduler

	ctx   *ProcessContext
	stats *ProcessStats

	parallelConfig ParallelConfig

	disablePreCommitState bool
}

var OptimisticTransfer = func(vm.StateDB, common.Address, *uint256.Int) bool { return true }

func NewSmartProcessor(config *params.ChainConfig, bc *BlockChain, parallelConfig ParallelConfig) *SmartProcessor {
	return &SmartProcessor{
		config:         config,
		bc:             bc,
		parallelConfig: parallelConfig,
	}
}

func NewSmartProcessorOnlyScheduling(config *params.ChainConfig, bc *BlockChain, parallelConfig ParallelConfig) *SmartProcessor {
	return &SmartProcessor{
		config:                config,
		bc:                    bc,
		parallelConfig:        parallelConfig,
		disablePreCommitState: true,
	}
}

func (p *SmartProcessor) Name() string {
	if p.disablePreCommitState {
		return SMARTSCHED_ONLYSCHED
	}
	return SMARTSCHED
}

func (p *SmartProcessor) ExecuteMsgs(header *types.Header, cfg vm.Config, input *ParallelInput) ([]*ExecutionResult, *ProcessStats, *DebugInfo, uint64, error) {
	// log.Info("processing", "block", header.Number, "rwsets", len(input.RWSets))
	msgs := input.Msgs
	var statedb *state.MVCCBaseStateDB
	if p.disablePreCommitState {
		statedb = input.SmartSchedOnlySchedStateDB
	} else {
		statedb = input.SmartSchedStateDB
	}
	txCount := len(msgs)
	if txCount == 0 {
		return nil, &ProcessStats{}, nil, 0, nil
	}
	p.stats = &ProcessStats{
		TxCount: txCount,
		TxStats: make([]TxExecStats, txCount),
	}
	start := time.Now()
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

	if p.parallelConfig.EnableTrace {
		p.ctx.traceResult = make([]json.RawMessage, txCount)
	}
	p.scheduler = NewSmartScheduler(txCount, p, input.RWSets, p.parallelConfig.TrackDependency)
	// go func() {
	// 	for i := 0; i < txCount; i++ {
	// 		go p.execute(i, p.useCommutative, true)
	// 	}
	// }()
	// p.ctx.mvccBaseDB.SetSyncronizer(p.scheduler)
	for i, msg := range p.ctx.msgs {
		statedb.LoadAccount(msg.From)
		if msg.To != nil {
			statedb.LoadAccount(*msg.To)
		}
		p.ctx.mvccStateDBs[i] = state.NewMVCCStateDB(statedb, i, common.BigToHash(big.NewInt(int64(i))), p.scheduler)
		// p.ctx.mvccStateDBs[i].LoadAccount(msg.From)
		// if msg.To != nil {
		// 	p.ctx.mvccStateDBs[i].LoadAccountAndCode(*msg.To)
		// }
		if p.parallelConfig.DisableNonceValidate {
			p.ctx.mvccStateDBs[i].DisableNonceCheck()
		}
	}
	p.stats.Init = time.Since(start)
	start = time.Now()
	p.scheduler.Start()
	<-p.scheduler.completeCh
	p.stats.Process = time.Since(start)
	// time.Sleep(5 * time.Millisecond)
	// close(p.scheduler.validationCh)
	usedGas := uint64(0)
	for _, result := range p.ctx.results {
		usedGas += result.UsedGas
	}
	for _, stat := range p.stats.TxStats {
		p.stats.TxProcessTime += stat.ProcessTime
		p.stats.TxProcessCnt += stat.ExecCount
	}
	if p.parallelConfig.TrackDependency {
		// log.Info("dynamic critical path length", "val", p.scheduler.GetCriticalPath())
		maxCrit := []int{}
		paths := state.FindCriticalPath(input.RWSets)
		for _, path := range paths {
			if len(path) > len(maxCrit) {
				maxCrit = path
			}
		}
		p.stats.StaticCriticalPathLen = len(maxCrit)

		dynamicCrit := p.scheduler.GetDynamicCriticalPath()
		p.stats.DynamicCriticalPathLen = len(dynamicCrit)
		for _, crit := range dynamicCrit {
			p.stats.CriticalPathExec += p.stats.TxStats[crit].ProcessTime
		}

		if len(p.scheduler.runnningSamples) > 0 {
			sum := 0.0
			for _, r := range p.scheduler.runnningSamples {
				sum += float64(r)
			}
			p.stats.AvgRunningWorker = sum / float64(len(p.scheduler.runnningSamples))
			// log.Info("sample length", "val", len(p.scheduler.runnningSamples))
		}

		// log.Info("static critical path length", "val", maxCrit)
	}
	return p.ctx.results, p.stats, &DebugInfo{p.ctx.traceResult, p.ctx.mvccStateDBs, nil}, usedGas, statedb.Error()
}

func (p *SmartProcessor) execute(txIndex int, useCommutative, useSched bool) {
	statedb := p.ctx.mvccStateDBs[txIndex]
	if useCommutative {
		statedb.EnableCommutative()
	} else {
		statedb.DisableCommutative()
	}
	if useSched {
		statedb.EnableSyncronizer()
	}
	if !p.ctx.msgs[txIndex].SkipAccountChecks {
		err := statedb.InitSender(p.ctx.msgs[txIndex].From, p.ctx.msgs[txIndex].Nonce)
		if err != nil {
			p.ctx.errors[txIndex] = err
			return
		}
	}

	vmConfig := p.ctx.vmConfig
	if p.parallelConfig.EnableTrace {
		vmConfig.Tracer = logger.NewStructLogger(&logger.Config{})
	}
	var vmenv *vm.EVM
	if useCommutative {
		vmenv = vm.NewEVM(p.ctx.commutativeBlockCtx, NewEVMTxContextWithIndex(p.ctx.msgs[txIndex], txIndex), statedb, p.config, vmConfig)
	} else {
		vmenv = vm.NewEVM(p.ctx.blockCtx, NewEVMTxContextWithIndex(p.ctx.msgs[txIndex], txIndex), statedb, p.config, vmConfig)
	}
	if !p.disablePreCommitState {
		vmenv.SetSSTORECallback(p.scheduler.PreCommitState)
	}
	// TODO: validate gas pool
	start := time.Now()
	execResult, err := ApplyMessage(vmenv, p.ctx.msgs[txIndex], new(GasPool).AddGas(p.ctx.blockCtx.GasLimit))
	p.stats.TxStats[txIndex].ProcessTime += time.Since(start)
	p.stats.TxStats[txIndex].ExecCount++
	p.ctx.errors[txIndex] = err
	if p.parallelConfig.EnableTrace {
		p.ctx.traceResult[txIndex], _ = vmConfig.Tracer.(*logger.StructLogger).GetResult()
	}
	if err != nil {
		// special cases: consensus precedure adjustment causes some txs to be not applied
		// assign gasUsed to be 0 to indicate that the tx is not applied
		p.ctx.results[txIndex] = &ExecutionResult{Err: err}
		return
	}
	p.ctx.results[txIndex] = execResult
	if useSched {
		statedb.DisableSyncronizer()
		p.scheduler.completeExec(txIndex)
	}
}

func (p *SmartProcessor) validate(txIndex int) (state.ConflictType, bool) {
	// some txs may be not applied, because we adjust the consensus precedure, we should omit them
	if p.ctx.errors[txIndex] != nil {
		return state.NoConflict, false
	}
	if conflict := p.ctx.mvccStateDBs[txIndex].Validate(); conflict != state.NoConflict {
		return conflict, false
	}
	return state.NoConflict, true
}
