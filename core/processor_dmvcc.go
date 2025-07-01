package core

import (
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

const (
	AcctTpy byte = iota + 1
	NonceTpy
	BalanceTpy
)

func acctToStorageKey(addr common.Address, tpy byte) StorageKey {
	var key StorageKey
	key[0] = tpy
	copy(key[common.HashLength:], addr[:])
	// key[common.HashLength-1] = tpy
	return key
}

func storageToStorageKey(addr common.Address, slot common.Hash) StorageKey {
	var key StorageKey
	copy(key[:], addr[:])
	copy(key[common.AddressLength:], slot[:])
	return key
}

type DmvccDependency struct {
	// Dependent: each tx's succeeding Dependent state
	Dependent []map[StorageKey]map[int]struct{}
	// Referred: each tx's pre-acquired state
	Referred []map[StorageKey]struct{}
}

func RWSetsToDmvccDependency(rwSets []*state.RWSet) *DmvccDependency {
	dependent := make([]map[StorageKey]map[int]struct{}, len(rwSets))
	referred := make([]map[StorageKey]struct{}, len(rwSets))
	for i := range rwSets {
		dependent[i] = make(map[StorageKey]map[int]struct{})
		referred[i] = make(map[StorageKey]struct{})
	}

	writeSet := make(map[StorageKey]int)
	var key StorageKey
	var keys map[StorageKey]struct{}

	for tx, rwSet := range rwSets {
		if rwSet == nil {
			continue
		}
		// check read set
		keys = make(map[StorageKey]struct{})
		for _, addr := range rwSet.BalanceRead {
			keys[acctToStorageKey(addr, BalanceTpy)] = struct{}{}
		}
		for _, addr := range rwSet.NonceRead {
			keys[acctToStorageKey(addr, NonceTpy)] = struct{}{}
		}
		for _, addr := range rwSet.CodeRead {
			keys[acctToStorageKey(addr, AcctTpy)] = struct{}{}
		}
		for _, storageRead := range rwSet.StorageRead {
			for _, slot := range storageRead.Keys {
				key = storageToStorageKey(storageRead.Address, slot)
				keys[key] = struct{}{}
			}
		}
		for key = range keys {
			if preceeding, ok := writeSet[key]; ok {
				// this state is written by previous tx
				if _, ok := dependent[preceeding][key]; !ok {
					dependent[preceeding][key] = make(map[int]struct{})
				}
				dependent[preceeding][key][tx] = struct{}{}
				referred[tx][key] = struct{}{}
			}
		}

		// update write set
		keys = make(map[StorageKey]struct{})
		for _, addr := range rwSet.AccountWrite {
			keys[acctToStorageKey(addr, AcctTpy)] = struct{}{}
		}
		for _, addr := range rwSet.BalanceWrite {
			keys[acctToStorageKey(addr, BalanceTpy)] = struct{}{}
		}
		for _, addr := range rwSet.NonceWrite {
			keys[acctToStorageKey(addr, NonceTpy)] = struct{}{}
		}
		for _, storageWrite := range rwSet.StorageWrite {
			for _, slot := range storageWrite.Keys {
				key = storageToStorageKey(storageWrite.Address, slot)
				keys[key] = struct{}{}
			}
		}
		for key = range keys {
			writeSet[key] = tx
		}
	}

	// sanity check
	depCnt := 0
	refCnt := 0
	for preceeding, deps := range dependent {
		for key, succeedings := range deps {
			depCnt += len(succeedings)
			for succeeding := range succeedings {
				if succeeding <= preceeding {
					panic("invalid dependent and referred!")
				}
				if _, ok := referred[succeeding][key]; !ok {
					panic("invalid dependent and referred!")
				}
			}
		}
	}
	for _, refs := range referred {
		refCnt += len(refs)
	}
	if depCnt != refCnt {
		panic("invalid dependent and referred!")
	}

	return &DmvccDependency{
		Dependent: dependent,
		Referred:  referred,
	}
}

var _ ParallelProcessor = (*DmvccProcessor)(nil)

type DmvccProcessor struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain

	ctx   *ProcessContext
	stats *ProcessStats

	parallelConfig ParallelConfig

	lock           sync.Mutex
	nextValidation atomic.Int32
	ready          TxQueue

	executed  []atomic.Bool
	validated []atomic.Bool

	deps *DmvccDependency

	preCommitStorages []map[StorageKey]common.Hash

	dependency *DependencyGraph
}

func NewDmvccProcessor(config *params.ChainConfig, bc *BlockChain, parallelConfig ParallelConfig) *DmvccProcessor {
	return &DmvccProcessor{
		config:         config,
		bc:             bc,
		parallelConfig: parallelConfig,
	}
}

func (p *DmvccProcessor) Name() string {
	return DMVCC
}

func (p *DmvccProcessor) ExecuteMsgs(header *types.Header, cfg vm.Config, input *ParallelInput) ([]*ExecutionResult, *ProcessStats, *DebugInfo, uint64, error) {
	msgs := input.Msgs
	statedb := input.DmvccStateDB
	rwSets := input.RWSets
	txCount := len(msgs)
	if txCount == 0 {
		return nil, &ProcessStats{}, nil, 0, nil
	}
	if rwSets == nil {
		rwSets = make([]*state.RWSet, txCount)
	}
	if len(rwSets) != txCount {
		panic("invalid DMVCC rwsets")
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

	p.nextValidation.Store(0)
	p.ready = TxQueue{}
	p.executed = make([]atomic.Bool, txCount)
	p.validated = make([]atomic.Bool, txCount)
	p.deps = RWSetsToDmvccDependency(rwSets)
	p.preCommitStorages = make([]map[StorageKey]common.Hash, txCount)
	p.dependency = NewDependencyGraph(rwSets)

	completeCh := make(chan struct{})

	for i := range p.ctx.msgs {
		p.ctx.mvccStateDBs[i] = state.NewMVCCStateDB(statedb, i, common.BigToHash(big.NewInt(int64(i))), p)
		// p.ctx.mvccStateDBs[i].LoadAccount(msg.From)
		// if msg.To != nil {
		// 	p.ctx.mvccStateDBs[i].LoadAccountAndCode(*msg.To)
		// }
		if p.parallelConfig.DisableNonceValidate {
			p.ctx.mvccStateDBs[i].DisableNonceCheck()
		}

		if len(p.deps.Referred[i]) == 0 {
			p.ready.Push(i)
		}
		p.preCommitStorages[i] = make(map[StorageKey]common.Hash)
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
	return p.ctx.results, p.stats, &DebugInfo{p.ctx.traceResult, p.ctx.mvccStateDBs, p.ctx.rwSets}, usedGas, statedb.Error()
}

func (p *DmvccProcessor) Worker(completeCh chan<- struct{}) {
	var (
		task    WorkerTask
		txIndex int
		nextVal int
		ok      bool
	)
	for {
		task = WorkerIdle
		nextVal = int(p.nextValidation.Load())
		if nextVal >= p.stats.TxCount {
			return
		}
		// check available validation task
		if p.executed[nextVal].Load() && p.validated[nextVal].CompareAndSwap(false, true) {
			task = WorkerValidate
			txIndex = nextVal
		} else {
			p.lock.Lock()
			txIndex, ok = p.ready.Pop()
			p.lock.Unlock()
			if ok {
				task = WorkerExecute
			}
		}

		if task == WorkerIdle {
			// avoid busy-loop
			runtime.Gosched()
			continue
		}

		switch task {
		case WorkerValidate:
			start := time.Now()
			if !p.validate(txIndex) {
				p.ctx.mvccStateDBs[txIndex].Abort()
				p.stats.Aborted++
				p.execute(txIndex, false, false)
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
			p.stats.Finalize += time.Since(start)

			p.nextValidation.Store(int32(txIndex + 1))

			// release succeeding txs
			deps := make(map[int]struct{})
			p.lock.Lock()
			for key, txs := range p.deps.Dependent[txIndex] {
				for tx := range txs {
					delete(p.deps.Referred[tx], key)
					deps[tx] = struct{}{}
				}
			}
			for tx := range deps {
				if len(p.deps.Referred[tx]) == 0 {
					p.ready.Push(tx)
				}
			}
			p.lock.Unlock()

			if txIndex+1 >= p.stats.TxCount {
				close(completeCh)
				return
			}
		case WorkerExecute:
			if p.executed[txIndex].Load() {
				fmt.Printf("[WARN] tx: %v has been executed\n", txIndex)
			}
			p.execute(txIndex, p.parallelConfig.UseCommutative, true)
			p.executed[txIndex].Store(true)
		}
	}
}

func (p *DmvccProcessor) execute(txIndex int, useCommutative bool, useSched bool) {
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
	var vmenv *vm.EVM
	if useCommutative {
		vmenv = vm.NewEVM(p.ctx.commutativeBlockCtx, NewEVMTxContextWithIndex(p.ctx.msgs[txIndex], txIndex), statedb, p.config, vmConfig)
	} else {
		vmenv = vm.NewEVM(p.ctx.blockCtx, NewEVMTxContextWithIndex(p.ctx.msgs[txIndex], txIndex), statedb, p.config, vmConfig)
	}
	vmenv.SetSSTORECallback(p.PreCommitState)

	start := time.Now()
	execResult, err := ApplyMessage(vmenv, p.ctx.msgs[txIndex], new(GasPool).AddGas(p.ctx.blockCtx.GasLimit))
	p.stats.TxStats[txIndex].ProcessTime += time.Since(start)
	p.stats.TxStats[txIndex].ExecCount++
	p.ctx.errors[txIndex] = err

	if err != nil {
		// special cases: consensus precedure adjustment causes some txs to be not applied
		// assign gasUsed to be 0 to indicate that the tx is not applied
		p.ctx.results[txIndex] = &ExecutionResult{Err: err}
		return
	}
	p.ctx.results[txIndex] = execResult
	if useSched {
		statedb.DisableSyncronizer()
	}
}

func (p *DmvccProcessor) validate(txIndex int) bool {
	// some txs may be not applied, because we adjust the consensus precedure, we should omit them
	if p.ctx.errors[txIndex] != nil {
		return false
	}
	if conflict := p.ctx.mvccStateDBs[txIndex].Validate(); conflict != state.NoConflict {
		return false
	}
	return true
}

func (p *DmvccProcessor) PreCommitState(tx int, pc uint64, addr common.Address, key, value common.Hash) {
	var storageKey StorageKey
	copy(storageKey[:], addr[:])
	copy(storageKey[common.AddressLength:], key[:])
	if !p.dependency.IsLastStorageWrite(tx, pc, storageKey) {
		return
	}
	p.lock.Lock()
	p.preCommitStorages[tx][storageKey] = value
	if deps, ok := p.deps.Dependent[tx][storageKey]; ok {
		for tx := range deps {
			delete(p.deps.Referred[tx], storageKey)
		}
		for dep := range deps {
			if len(p.deps.Referred[dep]) == 0 {
				p.ready.Push(dep)
			}
		}
		delete(p.deps.Dependent[tx], storageKey)
	}
	p.lock.Unlock()
}

func (p *DmvccProcessor) WaitForAccountReady(txIndex int, address common.Address) {
}

func (p *DmvccProcessor) WaitForBalanceReady(txIndex int, address common.Address) {
}
func (p *DmvccProcessor) WaitForNonceReady(txIndex int, address common.Address) {
}

func (p *DmvccProcessor) WaitForStorageReady(txIndex int, address common.Address, key common.Hash, result *state.ReadPrecommitResult) {
	dep := p.dependency.StorageDependent(txIndex, address, key)
	if dep < 0 || p.nextValidation.Load() > int32(dep) {
		return
	}

	var storageKey StorageKey
	copy(storageKey[:], address[:])
	copy(storageKey[common.AddressLength:], key[:])
	p.lock.Lock()
	val, ok := p.preCommitStorages[dep][storageKey]
	p.lock.Unlock()
	if ok {
		result.Storage = val
		result.Ready = true
	}
}
