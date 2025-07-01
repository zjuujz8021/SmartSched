package core

import (
	"container/heap"
	"encoding/json"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/tracers/logger"
	"github.com/ethereum/go-ethereum/params"
)

type IntHeap []int

func (h IntHeap) Len() int            { return len(h) }
func (h IntHeap) Less(i, j int) bool  { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *IntHeap) Push(x interface{}) { *h = append(*h, x.(int)) }
func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type TxnSet struct {
	txCount     int
	nextIdleTxn int
	nextValTxn  int
	valQueue    IntHeap
}

func NewTxnSet(txCount int) *TxnSet {
	txnset := &TxnSet{
		txCount:  txCount,
		valQueue: make(IntHeap, 0, txCount),
	}
	return txnset
}

func (s *TxnSet) ExistIdleTxn() bool { return s.nextIdleTxn < s.txCount }

func (s *TxnSet) GetIdleTxn() int {
	next := s.nextIdleTxn
	s.nextIdleTxn++
	return next
}

func (s *TxnSet) ExistValTxn() bool {
	return s.valQueue.Len() > 0 && s.valQueue[0] == s.nextValTxn
}
func (s *TxnSet) PutValTxn(txn int) { heap.Push(&s.valQueue, txn) }

func (s *TxnSet) GetValTxn() int { return heap.Pop(&s.valQueue).(int) }

func (s *TxnSet) CommitTxn() bool {
	s.nextValTxn++
	return s.nextValTxn == s.txCount
}

type WorkerTask int

const (
	WorkerIdle WorkerTask = iota
	WorkerExecute
	WorkerValidate
)

type ProcessContext struct {
	mvccBaseDB   *state.MVCCBaseStateDB
	mvccStateDBs []*state.MVCCStateDB
	rwSets       []*state.RWSet
	results      []*ExecutionResult
	errors       []error
	traceResult  []json.RawMessage
	// gp           *GasPool

	blockCtx            vm.BlockContext
	commutativeBlockCtx vm.BlockContext
	msgs                []*Message

	vmConfig vm.Config
	is158    bool
}

var _ ParallelProcessor = (*OccProcessor)(nil)

type SpectrumWaitRequest struct {
	dep        int
	address    common.Address
	storageKey common.Hash
	tpy        byte

	result *state.ReadPrecommitResult
	cond   *state.Condvar
}

type OccProcessor struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain

	lock   sync.Mutex
	txnSet *TxnSet
	ctx    *ProcessContext
	stats  *ProcessStats

	parallelConfig ParallelConfig

	// only used for spectrum
	useSpectrum    bool
	enableSpectrum bool
	dependency     *DependencyGraph
	waitNotify     [][]*SpectrumWaitRequest
	waitLock       []sync.Mutex
	taskPassed     []atomic.Bool
	//used for smartsched without instruction-level scheduling
	enablePrecommit   bool
	preCommitStorages []map[StorageKey]common.Hash

	// only used for parevm
	EnableRedo bool
}

func NewOccProcessor(config *params.ChainConfig, bc *BlockChain, parallelConfig ParallelConfig) *OccProcessor {
	return &OccProcessor{
		config:         config,
		bc:             bc,
		parallelConfig: parallelConfig,
	}
}

func NewSpectrumProcessor(config *params.ChainConfig, bc *BlockChain, parallelConfig ParallelConfig) *OccProcessor {
	return &OccProcessor{
		config:         config,
		bc:             bc,
		parallelConfig: parallelConfig,

		useSpectrum: true,
	}
}

func NewSmartProcessorOnlyPrecommit(config *params.ChainConfig, bc *BlockChain, parallelConfig ParallelConfig) *OccProcessor {
	return &OccProcessor{
		config:         config,
		bc:             bc,
		parallelConfig: parallelConfig,

		useSpectrum:     true,
		enablePrecommit: true,
	}
}

func NewParEVMProcessor(config *params.ChainConfig, bc *BlockChain, parallelConfig ParallelConfig) *OccProcessor {
	return &OccProcessor{
		config:         config,
		bc:             bc,
		parallelConfig: parallelConfig,

		EnableRedo: true,
	}
}

func (p *OccProcessor) Name() string {
	if p.useSpectrum {
		if p.enablePrecommit {
			return SMARTSCHED_ONLYPRECOMMIT
		}
		return SPECTRUM
	} else if p.EnableRedo {
		return PAREVM
	}
	return OCC
}

func (p *OccProcessor) ExecuteMsgs(header *types.Header, cfg vm.Config, input *ParallelInput) ([]*ExecutionResult, *ProcessStats, *DebugInfo, uint64, error) {
	msgs := input.Msgs
	var statedb *state.MVCCBaseStateDB
	if p.useSpectrum {
		if p.enablePrecommit {
			statedb = input.SmartSchedOnlyPreCommitStateDB
		} else {
			statedb = input.SpectrumStateDB
		}
	} else if p.EnableRedo {
		statedb = input.ParEVMStateDB
	} else {
		statedb = input.OCCStateDB
	}
	txCount := len(msgs)
	if txCount == 0 {
		return nil, &ProcessStats{}, nil, 0, nil
	}

	if p.useSpectrum && input.RWSets != nil {
		// use spectrum
		if len(input.RWSets) != txCount {
			panic("invalid rwsets")
		}
		p.enableSpectrum = true
		p.dependency = NewDependencyGraph(input.RWSets)
		p.waitNotify = make([][]*SpectrumWaitRequest, txCount)
		p.waitLock = make([]sync.Mutex, txCount)
		p.taskPassed = make([]atomic.Bool, txCount)
		if p.enablePrecommit {
			p.preCommitStorages = make([]map[StorageKey]common.Hash, txCount)
			for i := 0; i < txCount; i++ {
				p.preCommitStorages[i] = make(map[StorageKey]common.Hash)
			}
		}
	} else {
		p.enableSpectrum = false
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
	if p.parallelConfig.EnableTrace {
		p.ctx.traceResult = make([]json.RawMessage, txCount)
	}
	if p.parallelConfig.TrackRWSet {
		p.ctx.rwSets = make([]*state.RWSet, txCount)
	}

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

func (p *OccProcessor) Worker(completeCh chan<- struct{}) {
	for {
		var (
			task    = WorkerIdle
			txIndex int
		)
		p.lock.Lock()
		if p.txnSet.ExistValTxn() {
			task = WorkerValidate
			txIndex = p.txnSet.GetValTxn()
		} else if p.txnSet.ExistIdleTxn() {
			task = WorkerExecute
			txIndex = p.txnSet.GetIdleTxn()
		}
		p.lock.Unlock()

		switch task {
		case WorkerIdle:
			return
		case WorkerValidate:
			start := time.Now()
			if !p.validate(txIndex) {
				if p.EnableRedo {
					if p.ctx.errors[txIndex] == nil && p.ctx.mvccStateDBs[txIndex].RedoCtx.Redo() == state.Pass {
						goto PASS
					}
				}
				p.ctx.mvccStateDBs[txIndex].Abort()
				p.stats.Aborted++
				p.execute(txIndex, false, false)
				// sanity check
				// if !p.validate(txIndex) {
				// 	// panic(fmt.Sprintf("block %v - tx %d is not validated after execution, exec err: %v",
				// 	// p.ctx.blockCtx.BlockNumber.Uint64(), txIndex, p.ctx.errors[txIndex]))
				// 	log.Warn("Invalid tx", "block", p.ctx.blockCtx.BlockNumber.Uint64(), "tx", txIndex,
				// 		"exec err", p.ctx.errors[txIndex], "scheme", p.Name())
				// }
			}
		PASS:
			p.stats.Validate += time.Since(start)
			start = time.Now()
			p.ctx.mvccStateDBs[txIndex].FinalizeToBaseStateDB(p.ctx.is158)
			p.stats.Finalize += time.Since(start)
			p.lock.Lock()
			ok := p.txnSet.CommitTxn()
			p.lock.Unlock()

			if p.enableSpectrum {
				p.taskPassed[txIndex].Store(true)
				p.waitLock[txIndex].Lock()
				for _, wait := range p.waitNotify[txIndex] {
					wait.cond.Signal()
				}
				p.waitNotify[txIndex] = p.waitNotify[txIndex][:0]
				p.waitLock[txIndex].Unlock()
			}
			if ok {
				close(completeCh)
				return
			}
		case WorkerExecute:
			p.execute(txIndex, p.parallelConfig.UseCommutative, p.enableSpectrum)
			p.lock.Lock()
			p.txnSet.PutValTxn(txIndex)
			p.lock.Unlock()
		}
	}
}

func (p *OccProcessor) execute(txIndex int, useCommutative bool, useSched bool) {
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
	vmConfig.EnableRedo = p.EnableRedo
	var vmenv *vm.EVM
	if useCommutative {
		vmenv = vm.NewEVM(p.ctx.commutativeBlockCtx, NewEVMTxContext(p.ctx.msgs[txIndex]), statedb, p.config, vmConfig)
	} else {
		vmenv = vm.NewEVM(p.ctx.blockCtx, NewEVMTxContext(p.ctx.msgs[txIndex]), statedb, p.config, vmConfig)
	}
	if p.enableSpectrum && p.enablePrecommit {
		vmenv.SetSSTORECallback(p.PreCommitState)
	}
	if p.EnableRedo {
		redoCtx := state.NewRedoContext(statedb)
		statedb.ResetRedoCtx(redoCtx)
		vmenv.ResetRedoCtx(redoCtx)
	}
	if p.parallelConfig.TrackRWSet {
		vmenv.SetSSTORECallback(statedb.SSTORECallback)
	}

	start := time.Now()
	execResult, err := ApplyMessage(vmenv, p.ctx.msgs[txIndex], new(GasPool).AddGas(p.ctx.blockCtx.GasLimit))
	p.stats.TxStats[txIndex].ProcessTime += time.Since(start)
	p.stats.TxStats[txIndex].ExecCount++
	p.ctx.errors[txIndex] = err
	if p.parallelConfig.EnableTrace {
		p.ctx.traceResult[txIndex], _ = vmConfig.Tracer.(*logger.StructLogger).GetResult()
	}
	if p.parallelConfig.TrackRWSet {
		p.ctx.rwSets[txIndex] = statedb.GetRWSet(p.ctx.is158)
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
	}
}

func (p *OccProcessor) validate(txIndex int) bool {
	// some txs may be not applied, because we adjust the consensus precedure, we should omit them
	if p.ctx.errors[txIndex] != nil {
		return false
	}
	if p.EnableRedo {
		p.ctx.mvccStateDBs[txIndex].ValidateAndSetRedoContext()
		if !p.ctx.mvccStateDBs[txIndex].RedoCtx.IsSimplePass() {
			return false
		}
	} else {
		if conflict := p.ctx.mvccStateDBs[txIndex].Validate(); conflict != state.NoConflict {
			return false
		}
	}
	return true
}

func (s *OccProcessor) addWaitingDependency(req *SpectrumWaitRequest) *state.Condvar {
	if s.taskPassed[req.dep].Load() {
		return nil
	}

	s.waitLock[req.dep].Lock()
	defer s.waitLock[req.dep].Unlock()

	// check again, because the task may be passed by other goroutine
	if s.taskPassed[req.dep].Load() {
		return nil
	}

	if s.enableSpectrum && s.enablePrecommit && req.tpy == StorageWait {
		var storageKey StorageKey
		copy(storageKey[:], req.address[:])
		copy(storageKey[common.AddressLength:], req.storageKey[:])
		if precommitted, ok := s.preCommitStorages[req.dep][storageKey]; ok {
			req.result.Storage = precommitted
			req.result.Ready = true
			return nil
		}
	}

	cond := state.NewCondvar()

	req.cond = cond

	s.waitNotify[req.dep] = append(s.waitNotify[req.dep], req)

	return cond
}

func (s *OccProcessor) WaitForAccountReady(txIndex int, address common.Address) {
	dep := s.dependency.AccountDependent(txIndex, address)
	if dep < 0 {
		return
	}
	if cond := s.addWaitingDependency(&SpectrumWaitRequest{
		dep: dep,
		tpy: OtherWait,
	}); cond != nil {
		cond.Wait()
	}
}

func (s *OccProcessor) WaitForBalanceReady(txIndex int, address common.Address) {
	dep := s.dependency.BalanceDependent(txIndex, address)
	if dep < 0 {
		return
	}
	if cond := s.addWaitingDependency(&SpectrumWaitRequest{
		dep: dep,
		tpy: OtherWait,
	}); cond != nil {
		cond.Wait()
	}
}
func (s *OccProcessor) WaitForNonceReady(txIndex int, address common.Address) {
	dep := s.dependency.NonceDependent(txIndex, address)
	if dep < 0 {
		return
	}
	if cond := s.addWaitingDependency(&SpectrumWaitRequest{
		dep: dep,
		tpy: OtherWait,
	}); cond != nil {
		cond.Wait()
	}
}

func (s *OccProcessor) WaitForStorageReady(txIndex int, address common.Address, key common.Hash, result *state.ReadPrecommitResult) {
	dep := s.dependency.StorageDependent(txIndex, address, key)
	if dep < 0 {
		return
	}
	if cond := s.addWaitingDependency(&SpectrumWaitRequest{
		dep:        dep,
		address:    address,
		storageKey: key,
		tpy:        StorageWait,
		result:     result,
	}); cond != nil {
		cond.Wait()
	}
}

func (s *OccProcessor) PreCommitState(tx int, pc uint64, addr common.Address, key, value common.Hash) {
	var storageKey StorageKey
	copy(storageKey[:], addr[:])
	copy(storageKey[common.AddressLength:], key[:])
	if !s.dependency.IsLastStorageWrite(tx, pc, storageKey) {
		return
	}
	s.waitLock[tx].Lock()
	s.preCommitStorages[tx][storageKey] = value
	if len(s.waitNotify[tx]) == 0 {
		s.waitLock[tx].Unlock()
		return
	}
	leftWaiting := s.waitNotify[tx][:0]
	for _, req := range s.waitNotify[tx] {
		if req.tpy == StorageWait && req.address == addr && req.storageKey == key {
			req.result.Storage = value
			req.result.Ready = true
			req.cond.Signal()
		} else {
			leftWaiting = append(leftWaiting, req)
		}
	}
	s.waitNotify[tx] = leftWaiting
	s.waitLock[tx].Unlock()
}
