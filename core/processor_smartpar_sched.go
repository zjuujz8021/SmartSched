package core

import (
	"container/heap"
	"encoding/binary"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	_ "unsafe"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/state"
)

//go:linkname procyield runtime.procyield
func procyield(cycles uint32)

type TxQueue struct {
	h IntHeap
}

func (q *TxQueue) Push(txs ...int) {
	for _, tx := range txs {
		heap.Push(&q.h, tx)
	}
}

func (q *TxQueue) Empty() bool {
	empty := q.h.Len() == 0
	return empty
}

func (q *TxQueue) Pop() (int, bool) {
	if q.h.Len() == 0 {
		return 0, false
	}
	tx := heap.Pop(&q.h).(int)
	return tx, true
}

func (q *TxQueue) PopN(n int) []int {
	l := q.h.Len()
	if l == 0 {
		return nil
	}
	n = min(n, l)
	txs := make([]int, 0, n)
	for i := 0; i < n; i++ {
		txs = append(txs, heap.Pop(&q.h).(int))
	}
	return txs
}

func (q *TxQueue) PushPop(txs []int, n int) []int {
	for _, tx := range txs {
		heap.Push(&q.h, tx)
	}
	n = min(n, q.h.Len())
	if n == 0 {
		return nil
	}
	txs = make([]int, 0, n)
	for i := 0; i < n; i++ {
		txs = append(txs, heap.Pop(&q.h).(int))
	}
	return txs
}

func (q *TxQueue) Front() (int, bool) {
	if q.h.Len() == 0 {
		return 0, false
	}
	tx := q.h[0]
	return tx, true
}

// type TaskStatus int32

const (
	TaskReady int32 = iota
	TaskRunning
	TaskWaiting
	TaskDone
	TaskValidation
	TaskPass
)

const (
	OtherWait byte = iota
	BalanceWait
	StorageWait
	NonceWait
	AccountWait
)

const (
	TriggerTypeChan int = iota
	TriggerTypeCursor
)

type StorageKey [common.AddressLength + common.HashLength]byte

func (k StorageKey) String() string {
	return hexutil.Encode(k[:])
}

func (k StorageKey) MarshalText() ([]byte, error) {
	return hexutil.Bytes(k[:]).MarshalText()
}

const prime32 = uint32(16777619)

func storagekeySharding(key StorageKey) uint32 {
	addrPart := binary.BigEndian.Uint32(key[common.AddressLength-4 : common.AddressLength])
	storagePart := binary.BigEndian.Uint32(key[len(key)-4:])
	return (addrPart + prime32) * (storagePart + prime32)
}

type padding [128]byte

type waitRequest struct {
	src        int
	dst        int
	address    common.Address
	storageKey common.Hash
	tpy        byte

	result *state.ReadPrecommitResult
}

type SmartScheduler struct {
	processor  *SmartProcessor
	dependency *DependencyGraph
	total      int
	_          padding
	taskStatus []atomic.Int32
	_          padding
	ready      TxQueue
	// validation TxQueue

	started []bool

	_              padding
	running        atomic.Int64
	_              padding
	nextValidation atomic.Int32
	_              padding
	wgs            []*sync.WaitGroup

	// validationCh chan struct{}

	// used for waiting schedule
	waitNotify [][]*waitRequest
	// leftWaiting [][]*waitRequest
	readyTxs [][]int
	waitLock []sync.Mutex

	// addr => hash => hash
	// preCommitStorages []cmap.ConcurrentMap[StorageKey, common.Hash]
	preCommitStorages []map[StorageKey]common.Hash

	// used for commit (validation) schedule
	completeCh chan struct{}

	trackDependent bool
	dependent      [][]int

	runnningSamples []int

	schedLock sync.Mutex
	// releasedTxs []int
}

func NewSmartScheduler(txCount int, processor *SmartProcessor, rwSets []*state.RWSet, trackDependent bool) *SmartScheduler {
	// Initialize the ready heap with all transactions
	var wg *sync.WaitGroup
	wgs := make([]*sync.WaitGroup, txCount)
	taskStatus := make([]atomic.Int32, txCount)
	ready := make(IntHeap, txCount)
	waitNotify := make([][]*waitRequest, txCount)
	// leftWaiting := make([][]*waitRequest, txCount)
	readyTxs := make([][]int, txCount)
	// preCommitStorages := make([]cmap.ConcurrentMap[StorageKey, common.Hash], txCount)
	preCommitStorages := make([]map[StorageKey]common.Hash, txCount)
	for i := 0; i < txCount; i++ {
		ready[i] = i
		wg = new(sync.WaitGroup)
		// wg.Add(1)
		wgs[i] = wg
		taskStatus[i].Store(TaskReady)
		waitNotify[i] = make([]*waitRequest, 0, txCount)
		// leftWaiting[i] = make([]*waitRequest, 0, txCount)
		readyTxs[i] = make([]int, 0, txCount)
		preCommitStorages[i] = make(map[StorageKey]common.Hash)
	}
	s := &SmartScheduler{
		processor:  processor,
		dependency: NewDependencyGraph(rwSets),
		total:      txCount,
		taskStatus: taskStatus,
		ready:      TxQueue{h: ready},

		started: make([]bool, txCount),

		wgs:        wgs,
		waitNotify: waitNotify,
		// leftWaiting: leftWaiting,
		readyTxs: readyTxs,
		waitLock: make([]sync.Mutex, txCount),
		// validationCh: make(chan struct{}, txCount),
		completeCh: make(chan struct{}),

		preCommitStorages: preCommitStorages,

		dependent:       make([][]int, txCount),
		trackDependent:  trackDependent,
		runnningSamples: make([]int, 0, 64),

		// releasedTxs: make([]int, 0, txCount),
	}
	return s
}

func (s *SmartScheduler) sampling(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// log.Info("start sampling")
	for {
		select {
		case <-ticker.C:
			running := s.running.Load()
			// ready, ok := s.ready.Front()
			// taskStatus := make([]int, s.total)
			// for i := 0; i < s.total; i++ {
			// 	taskStatus[i] = int(s.taskStatus[i].Load())
			// }
			// fmt.Println("running", running, "nextValidation", s.nextValidation, "existReady", ok, "ready", ready)
			// fmt.Println(taskStatus)
			s.runnningSamples = append(s.runnningSamples, int(running))
		case <-s.completeCh:
			// log.Info("stop sampling")
			return
		}
	}
}

func (s *SmartScheduler) GetDynamicCriticalPath() []int {
	if len(s.dependent) == 0 {
		return nil
	}
	// log.Info("dependent", "dependent", s.dependent)

	maxPath := make([][]int, s.total)
	for i := range maxPath {
		maxPath[i] = []int{i}
	}

	// dp to find critical path, let f(i) is the slice of critical path if tx i is the last tx
	// f(i) = maxlen(append(f(j),i)) for all 0 <= j < i and j in conflictTxs
	for i := 1; i < s.total; i++ {
		for _, dependent := range s.dependent[i] {
			// sanity check
			if dependent >= i {
				panic("dependent tx index should be less than i")
			}
			if len(maxPath[dependent])+1 > len(maxPath[i]) {
				maxPath[i] = slices.Clone(maxPath[dependent])
				maxPath[i] = append(maxPath[i], i)
			}
		}
	}

	crit := maxPath[0]
	for i := 1; i < s.total; i++ {
		if len(maxPath[i]) > len(crit) {
			crit = maxPath[i]
		}
	}
	return crit
}

func (s *SmartScheduler) Sched(ready []int) {
	s.schedLock.Lock()
	maxPops := s.processor.parallelConfig.WorkerNum - int(s.running.Load())
	txs := s.ready.PushPop(ready, maxPops)
	cnt := len(txs)
	if cnt > 0 {
		s.running.Add(int64(cnt))
	}
	s.schedLock.Unlock()
	for _, tx := range txs {
		s.taskStatus[tx].Store(TaskRunning)
		if !s.started[tx] {
			s.started[tx] = true
			go s.processor.execute(tx, s.processor.parallelConfig.UseCommutative, true)
		} else {
			s.wgs[tx].Done()
		}
	}
}

func (s *SmartScheduler) validation() bool {
	var nextValidation int
	for {
		nextValidation = int(s.nextValidation.Load())
		done := s.taskStatus[nextValidation].CompareAndSwap(TaskDone, TaskValidation)
		if done {
			// log.Info("validating", "tx", nextValidation)
			start := time.Now()
			if _, ok := s.processor.validate(nextValidation); !ok {
				s.processor.ctx.mvccStateDBs[nextValidation].Abort()
				s.processor.stats.Aborted++
				// log.Info("re-executing", "tx", nextValidation)
				s.processor.execute(nextValidation, false, false)
				// if conflict, ok := s.processor.validate(nextValidation); !ok {
				// 	// sanity check, it should be never reached
				// 	// panic(fmt.Sprintf("block %v - tx %d is not validated after execution, exec err: %v, conflict: %v",
				// 	// 	s.processor.ctx.blockCtx.BlockNumber.Uint64(), nextValidation, s.processor.ctx.errors[nextValidation], conflict))
				// 	log.Warn("Invalid tx", "block", s.processor.ctx.blockCtx.BlockNumber.Uint64(), "tx", nextValidation,
				// 		"exec err", s.processor.ctx.errors[nextValidation], "conflict", conflict, "scheme", s.processor.Name())
				// }
			}
			s.processor.stats.Validate += time.Since(start)
			start = time.Now()
			// log.Info("tx validated", "txIndex", nextValidation)
			s.processor.ctx.mvccStateDBs[nextValidation].FinalizeToBaseStateDB(s.processor.ctx.is158)
			s.processor.stats.Finalize += time.Since(start)
			s.taskStatus[nextValidation].Store(TaskPass)
			if nextValidation+1 == s.total {
				close(s.completeCh)
				return true
			}

			s.nextValidation.Add(1)
			// s.running.Add(-1)
			// s.Sched(nil)
			s.waitLock[nextValidation].Lock()
			if len(s.waitNotify[nextValidation]) == 0 {
				s.waitLock[nextValidation].Unlock()
				continue
			}
			// once a transaction is validated, we should notify its dependents
			releasedTxs := make([]int, 0, len(s.waitNotify[nextValidation]))
			for _, dep := range s.waitNotify[nextValidation] {
				s.taskStatus[dep.dst].Store(TaskReady)
				releasedTxs = append(releasedTxs, dep.dst)
			}
			// clear pending queue
			s.waitNotify[nextValidation] = s.waitNotify[nextValidation][:0]
			s.waitLock[nextValidation].Unlock()
			s.Sched(releasedTxs)
			// s.releasedTxs = s.releasedTxs[:0]
		} else {
			return false
		}
	}
}

// validationloop is used for validating (and re-executing if validation fails) transactions
// func (s *SmartScheduler) validationloop() {
// 	for range s.validationCh {
// 		if s.validation() {
// 			return
// 		}
// 	}
// }

func (s *SmartScheduler) completeExec(txIndex int) {
	s.running.Add(-1)
	s.taskStatus[txIndex].Store(TaskDone)
	s.Sched(nil)
	// s.validationCh <- struct{}{}
	s.validation()
}

func (s *SmartScheduler) Start() {
	// go s.validationloop()
	go s.Sched(nil)
	// if err := s.schedCh.Start(); err != nil {
	// 	panic(err)
	// }
	// s.schedCh.Producer().Write(struct{}{})
	if s.trackDependent {
		go s.sampling(2 * time.Millisecond)
	}
}

func (s *SmartScheduler) PreCommitState(tx int, pc uint64, addr common.Address, key, value common.Hash) {
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
			s.taskStatus[req.dst].Store(TaskReady)
			s.readyTxs[tx] = append(s.readyTxs[tx], req.dst)
		} else {
			leftWaiting = append(leftWaiting, req)
		}
	}
	s.waitNotify[tx] = leftWaiting
	s.waitLock[tx].Unlock()
	if len(s.readyTxs[tx]) > 0 {
		s.Sched(s.readyTxs[tx])
		s.readyTxs[tx] = s.readyTxs[tx][:0]
	}
}

func (s *SmartScheduler) getPreCommittedState(tx int, addr common.Address, key common.Hash) (common.Hash, bool) {
	var storageKey StorageKey
	copy(storageKey[:], addr[:])
	copy(storageKey[common.AddressLength:], key[:])
	if val, ok := s.preCommitStorages[tx][storageKey]; ok {
		return val, true
	}
	return common.Hash{}, false
}

func (s *SmartScheduler) registerWaitingDependency(req *waitRequest) bool {
	s.waitLock[req.src].Lock()

	// double check if the task is already done or target state is precommitted
	if s.taskStatus[req.src].Load() == TaskPass {
		s.waitLock[req.src].Unlock()
		return false
	}
	if req.tpy == StorageWait {
		if precommitted, ok := s.getPreCommittedState(req.src, req.address, req.storageKey); ok {
			s.waitLock[req.src].Unlock()
			req.result.Storage = precommitted
			req.result.Ready = true
			return false
		}
	}

	s.taskStatus[req.dst].Store(TaskWaiting)

	s.waitNotify[req.src] = append(s.waitNotify[req.src], req)
	s.wgs[req.dst].Add(1)
	s.waitLock[req.src].Unlock()

	s.running.Add(-1)
	s.Sched(nil)
	if s.trackDependent {
		s.dependent[req.dst] = append(s.dependent[req.dst], req.src)
	}
	return true
}

func (s *SmartScheduler) WaitForAccountReady(txIndex int, address common.Address) {
	dep := s.dependency.AccountDependent(txIndex, address)
	if dep < 0 || s.taskStatus[dep].Load() == TaskPass {
		return
	}
	if s.registerWaitingDependency(&waitRequest{src: dep, dst: txIndex, address: address, tpy: AccountWait}) {
		s.wgs[txIndex].Wait()
	}
}

func (s *SmartScheduler) WaitForBalanceReady(txIndex int, address common.Address) {
	dep := s.dependency.BalanceDependent(txIndex, address)
	if dep < 0 || s.taskStatus[dep].Load() == TaskPass {
		return
	}
	if s.registerWaitingDependency(&waitRequest{src: dep, dst: txIndex, address: address, tpy: BalanceWait}) {
		s.wgs[txIndex].Wait()
	}
}
func (s *SmartScheduler) WaitForNonceReady(txIndex int, address common.Address) {
	dep := s.dependency.NonceDependent(txIndex, address)
	if dep < 0 || s.taskStatus[dep].Load() == TaskPass {
		return
	}
	if s.registerWaitingDependency(&waitRequest{src: dep, dst: txIndex, address: address, tpy: NonceWait}) {
		s.wgs[txIndex].Wait()
	}
}

func (s *SmartScheduler) WaitForStorageReady(txIndex int, address common.Address, key common.Hash, result *state.ReadPrecommitResult) {
	dep := s.dependency.StorageDependent(txIndex, address, key)
	if dep < 0 || s.taskStatus[dep].Load() == TaskPass {
		return
	}
	if s.registerWaitingDependency(&waitRequest{
		src:        dep,
		dst:        txIndex,
		address:    address,
		storageKey: key,
		tpy:        StorageWait,
		result:     result,
	}) {
		s.wgs[txIndex].Wait()
	}
}
