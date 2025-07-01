package state

import (
	"fmt"
	"math/big"
	"slices"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

type ConflictType int

const (
	NoConflict ConflictType = 1 << iota
	AccountExistConflict
	AccountReplaceConflict
	AccountDanglingConflict
	AccountDeletedConflict
	NonceConflict
	BalanceConflict
	BalanceDeltaConflict
	CodeConflict
	StorageConflict
)

type StateSyncronizer interface {
	// account ready is used to wait for account create or delete
	WaitForAccountReady(txIndex int, address common.Address)
	WaitForBalanceReady(txIndex int, address common.Address)
	WaitForNonceReady(txIndex int, address common.Address)
	// WaitForCodeReady(txIndex int, address common.Address)
	WaitForStorageReady(txIndex int, address common.Address, key common.Hash, result *ReadPrecommitResult)
}

type MVCCBaseStateDB struct {
	db   Database
	trie Trie

	// common.Address => *mvccBaseStateObject (non-nil, non-deleted)
	mvccBaseStateObjects *ShardingMap
	// syncronizer          StateSyncronizer
	// common.Address => bool
	// deletedStateObjects sync.Map
	err     error
	errOnce sync.Once

	// impl read-write locks, used for 2pl
	acctLock    *TwoPhaseLock[common.Address]
	balanceLock *TwoPhaseLock[common.Address]
	nonceLock   *TwoPhaseLock[common.Address]
	storageLock *TwoPhaseLock[StorageKey]
}

func NewMVCCBaseStateDB(db Database) *MVCCBaseStateDB {
	trie, err := db.OpenTrie(types.EmptyRootHash)
	if err != nil {
		panic(err)
	}
	return &MVCCBaseStateDB{
		db:   db,
		trie: trie,

		mvccBaseStateObjects: NewShardingAccountMap(),
	}
}

// func (s *MVCCBaseStateDB) SetSyncronizer(syncronizer StateSyncronizer) {
// 	s.syncronizer = syncronizer
// }

func (s *MVCCBaseStateDB) Init2PL(txs int, abort func(txIndex int)) {
	s.acctLock = NewTwoPhaseLock(txs, func(key common.Address, txIndex int) { abort(txIndex) })
	s.balanceLock = NewTwoPhaseLock(txs, func(key common.Address, txIndex int) { abort(txIndex) })
	s.nonceLock = NewTwoPhaseLock(txs, func(key common.Address, txIndex int) { abort(txIndex) })
	s.storageLock = NewTwoPhaseLock(txs, func(key StorageKey, txIndex int) { abort(txIndex) })
}

func (s *MVCCBaseStateDB) ReleaseLocks(txIndex int) {
	s.acctLock.ReleaseLocks(txIndex)
	s.balanceLock.ReleaseLocks(txIndex)
	s.nonceLock.ReleaseLocks(txIndex)
	s.storageLock.ReleaseLocks(txIndex)
}

func (s *MVCCBaseStateDB) SetError(err error) {
	s.errOnce.Do(func() {
		s.err = err
	})
}

func (s *MVCCBaseStateDB) Error() error {
	return s.err
}

func (s *MVCCBaseStateDB) getDeletedStateObject(addr common.Address) *mvccBaseStateObject {
	if obj, ok := s.mvccBaseStateObjects.Load(addr); ok {
		return obj
	}

	if sobj, err := s.trie.GetAccount(addr); err != nil {
		s.SetError(err)
	} else if sobj != nil {
		actual, _ := s.mvccBaseStateObjects.LoadOrStore(addr, newMvccBaseStateObject(s, addr, sobj))
		return actual
	}
	return nil
}

func (s *MVCCBaseStateDB) getStateObject(addr common.Address) *mvccBaseStateObject {
	if obj := s.getDeletedStateObject(addr); obj != nil && !obj.getDeleted() {
		return obj
	}
	return nil
}

func (s *MVCCBaseStateDB) LoadAccount(addr common.Address) {
	s.getStateObject(addr)
}

func (s *MVCCBaseStateDB) resetStateObject(newobj *mvccBaseStateObject) {
	actual, loaded := s.mvccBaseStateObjects.LoadOrStore(newobj.address, newobj)
	if loaded {
		actual.resetByBaseMVCCStateObject(newobj)
	}
}

func (s *MVCCBaseStateDB) DeepCopy() *MVCCBaseStateDB {
	cp := NewMVCCBaseStateDB(s.db)
	cp.mvccBaseStateObjects = s.mvccBaseStateObjects.DeepCopy(cp)
	return cp
}

type MVCCStateDB struct {
	baseStateDB *MVCCBaseStateDB

	mvccStateObjects      map[common.Address]*MVCCStateObject
	replacedStateObjects  []*MVCCStateObject
	danglingObjects       []*MVCCStateObject
	originNonExistObjects map[common.Address]struct{}

	refund     uint64
	thash      common.Hash
	txIndex    int
	logs       []*types.Log
	accessList *accessList

	transientStorage transientStorage

	journal        *mvccJournal
	validRevisions []revision
	nextRevisionId int

	useCommutative bool

	enableSyncronizer bool
	syncronizer       StateSyncronizer

	disableNonceCheck bool

	lastWritePCs map[common.Address]map[common.Hash]uint64

	// used for 2pl
	enable2PL bool
	cancelled func() bool

	// used for parevm
	RedoCtx *RedoContext
}

func NewMVCCStateDB(baseStateDB *MVCCBaseStateDB, txIndex int, txhash common.Hash, syncronizer StateSyncronizer) *MVCCStateDB {
	return &MVCCStateDB{
		baseStateDB: baseStateDB,

		mvccStateObjects:      make(map[common.Address]*MVCCStateObject),
		replacedStateObjects:  make([]*MVCCStateObject, 0),
		danglingObjects:       make([]*MVCCStateObject, 0),
		originNonExistObjects: make(map[common.Address]struct{}),

		thash:            txhash,
		txIndex:          txIndex,
		journal:          newMvccJournal(),
		accessList:       newAccessList(),
		transientStorage: newTransientStorage(),

		syncronizer: syncronizer,
	}
}

func (s *MVCCStateDB) Init2PL(cancelled func() bool) {
	s.enable2PL = true
	s.cancelled = cancelled
}

func (s *MVCCStateDB) ResetRedoCtx(ctx *RedoContext) {
	s.RedoCtx = ctx
}

// func (s *MVCCStateDB) getBaseStateDB() *mvccBaseStateDB {
// return s.baseStateDB
// }
func (s *MVCCStateDB) Abort() {
	s.mvccStateObjects = make(map[common.Address]*MVCCStateObject)
	s.replacedStateObjects = s.replacedStateObjects[:0]
	s.danglingObjects = s.danglingObjects[:0]
	s.originNonExistObjects = make(map[common.Address]struct{})
	s.refund = 0
	s.logs = s.logs[:0]
	s.accessList = newAccessList()
	s.transientStorage = newTransientStorage()
	s.journal = newMvccJournal()
	s.validRevisions = s.validRevisions[:0]
	s.nextRevisionId = 0
	s.lastWritePCs = nil

	s.enable2PL = false
	s.cancelled = nil
}

func (s *MVCCStateDB) Error() error {
	return s.baseStateDB.Error()
}

func (s *MVCCStateDB) SSTORECallback(tx int, pc uint64, addr common.Address, key, value common.Hash) {
	if s.lastWritePCs == nil {
		s.lastWritePCs = make(map[common.Address]map[common.Hash]uint64)
	}
	if _, ok := s.lastWritePCs[addr]; !ok {
		s.lastWritePCs[addr] = make(map[common.Hash]uint64)
	}
	s.lastWritePCs[addr][key] = pc
}

func (s *MVCCStateDB) DisableNonceCheck() {
	s.disableNonceCheck = true
}

func (s *MVCCStateDB) EnableCommutative() {
	s.useCommutative = true
}

func (s *MVCCStateDB) DisableCommutative() {
	s.useCommutative = false
}

func (s *MVCCStateDB) EnableSyncronizer() {
	s.enableSyncronizer = true
}

func (s *MVCCStateDB) DisableSyncronizer() {
	s.enableSyncronizer = false
}

var errMVCCStateObjectNotFound = fmt.Errorf("mvcc state object not found")

func (s *MVCCStateDB) InitSender(sender common.Address, txNonce uint64) error {
	obj := s.getStateObject(sender)
	if obj == nil {
		return errMVCCStateObjectNotFound
		// panic(fmt.Sprintf("sender %v does not exist, tx: %s", sender, s.thash.Hex()))
	}
	obj.setOriginNonce(txNonce)
	return nil
}

func (s *MVCCStateDB) LoadAccount(addr common.Address) {
	s.getStateObject(addr)
}

func (s *MVCCStateDB) LoadAccountAndCode(addr common.Address) {
	if obj := s.getStateObject(addr); obj != nil {
		obj.Code()
	}
}

func (s *MVCCStateDB) Validate() ConflictType {
	for addr := range s.originNonExistObjects {
		if s.baseStateDB.getStateObject(addr) != nil {
			return AccountExistConflict
		}
	}

	for _, obj := range s.replacedStateObjects {
		if conflict := obj.validate(); conflict != NoConflict {
			return AccountReplaceConflict | conflict
		}
	}

	for _, obj := range s.danglingObjects {
		if conflict := obj.validate(); conflict != NoConflict {
			return AccountDanglingConflict | conflict
		}
	}

	for _, obj := range s.mvccStateObjects {
		if conflict := obj.validate(); conflict != NoConflict {
			return conflict
		}
	}

	return NoConflict
}

func (s *MVCCStateDB) ValidateAndSetRedoContext() {
	for addr := range s.originNonExistObjects {
		if s.baseStateDB.getStateObject(addr) != nil {
			s.RedoCtx.IsUnrecoverableStorageConflict = true
			return
		}
	}

	var conflictStorage map[common.Address]map[common.Hash]common.Hash

	for _, obj := range s.replacedStateObjects {
		if conflictSlot, cont := obj.validateAndSetRedoContext(); !cont {
			return
		} else if conflictSlot != nil {
			if conflictStorage == nil {
				conflictStorage = make(map[common.Address]map[common.Hash]common.Hash)
			}
			conflictStorage[obj.Address()] = conflictSlot
		}
	}

	for _, obj := range s.danglingObjects {
		if conflictSlot, cont := obj.validateAndSetRedoContext(); !cont {
			return
		} else if conflictSlot != nil {
			if conflictStorage == nil {
				conflictStorage = make(map[common.Address]map[common.Hash]common.Hash)
			}
			conflictStorage[obj.Address()] = conflictSlot
		}
	}

	for _, obj := range s.mvccStateObjects {
		if conflictSlot, cont := obj.validateAndSetRedoContext(); !cont {
			return
		} else if conflictSlot != nil {
			if conflictStorage == nil {
				conflictStorage = make(map[common.Address]map[common.Hash]common.Hash)
			}
			conflictStorage[obj.Address()] = conflictSlot
		}
	}

	if conflictStorage != nil {
		s.RedoCtx.ConflictStorage = conflictStorage
	}
}

func (s *MVCCStateDB) FinalizeToBaseStateDB(deleteEmptyObjects bool) {
	for addr := range s.journal.dirties {
		obj, exist := s.mvccStateObjects[addr]
		if !exist {
			continue
		}
		// if obj.selfDestructed || (deleteEmptyObjects && obj.empty()) {
		if obj.selfDestructed {
			obj.baseObj.setDeleted(true)
		} else {
			obj.FinalizeToBaseObject()
		}
		if obj.created {
			// newly created account is not registered to base state db
			// thus, we need to update base state obj here
			s.baseStateDB.resetStateObject(obj.baseObj)
		}
	}
}

func (s *MVCCStateDB) getStateObject(address common.Address) *MVCCStateObject {
	if obj := s.mvccStateObjects[address]; obj != nil {
		return obj
	}

	if _, ok := s.originNonExistObjects[address]; ok {
		return nil
	}
	if s.enableSyncronizer {
		s.syncronizer.WaitForAccountReady(s.txIndex, address)
	}
	if s.enable2PL {
		s.baseStateDB.acctLock.AcquireReadLock(address, s.txIndex, s.cancelled)
	}
	if baseObj := s.baseStateDB.getStateObject(address); baseObj != nil {
		obj := newMvccStateObject(s, baseObj, false)
		s.mvccStateObjects[address] = obj
		return obj
	}

	s.originNonExistObjects[address] = struct{}{}
	return nil
}

func (s *MVCCStateDB) createObject(addr common.Address) (newobj, prev *MVCCStateObject) {
	prev = s.getStateObject(addr) // Note, prev might have been deleted, we need that!
	newobj = newMvccStateObject(s, newMvccBaseStateObject(s.baseStateDB, addr, nil), true)
	if prev == nil {
		s.journal.append(mvccCreateObjectChange{account: &addr})
	} else {
		s.journal.append(mvccResetObjectChange{
			account: &addr,
			prev:    prev,
		})
		// prev.created is true => tx: transfer funds then create contract
		// if !prev.created {
		// ONLY track replaced objects that were not created in this transaction, i.e., the first read object
		// s.replacedStateObjects[addr] = prev
		s.replacedStateObjects = append(s.replacedStateObjects, prev)
		// }
	}
	s.mvccStateObjects[addr] = newobj
	// prev can NOT be deleted.
	return newobj, prev
}

func (s *MVCCStateDB) getOrNewStateObject(addr common.Address) *MVCCStateObject {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		stateObject, _ = s.createObject(addr)
	}
	return stateObject
}

func (s *MVCCStateDB) CreateAccount(addr common.Address) {
	if s.enable2PL {
		s.baseStateDB.acctLock.AcquireWriteLock(addr, s.txIndex, s.cancelled)
	}

	newObj, prev := s.createObject(addr)
	if prev != nil {
		prevBalance := prev.Balance()
		if !prevBalance.IsZero() {
			if s.useCommutative {
				newObj.setBalanceDelta(new(big.Int).Set(prevBalance.ToBig()))
			} else {
				newObj.setBalance(new(uint256.Int).Set(prevBalance))
			}
		}
	}
}

func (s *MVCCStateDB) SetBalance(addr common.Address, amount *uint256.Int) {
	stateObject := s.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetBalance(amount)
	}
}

func (s *MVCCStateDB) SubBalance(addr common.Address, amount *uint256.Int) {
	stateObject := s.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SubBalance(amount)
	}
}

func (s *MVCCStateDB) AddBalance(addr common.Address, amount *uint256.Int) {
	stateObject := s.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.AddBalance(amount)
	}
}

func (s *MVCCStateDB) GetBalance(addr common.Address) *uint256.Int {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Balance()
	}
	return common.U2560
}

func (s *MVCCStateDB) revertDirtyBalance(addr common.Address) {
	obj := s.getStateObject(addr)
	if obj != nil {
		obj.dirty_balance = nil
	}
}

func (s *MVCCStateDB) GetNonce(addr common.Address) uint64 {
	if s.disableNonceCheck {
		return 0
	}
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Nonce()
	}

	return 0
}

func (s *MVCCStateDB) SetNonce(addr common.Address, nonce uint64) {
	if s.disableNonceCheck {
		return
	}
	stateObject := s.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetNonce(nonce)
	}
}

func (s *MVCCStateDB) revertDirtyNonce(addr common.Address) {
	obj := s.getStateObject(addr)
	if obj != nil {
		obj.dirty_nonce = nil
	}
}

func (s *MVCCStateDB) GetCodeHash(addr common.Address) common.Hash {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return common.BytesToHash(stateObject.CodeHash())
	}
	return common.Hash{}
}

func (s *MVCCStateDB) GetCode(addr common.Address) []byte {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Code()
	}
	return nil
}

func (s *MVCCStateDB) SetCode(addr common.Address, code []byte) {
	stateObject := s.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetCode(crypto.Keccak256Hash(code), code)
	}
}

func (s *MVCCStateDB) GetCodeSize(addr common.Address) int {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.CodeSize()
	}
	return 0
}

func (s *MVCCStateDB) AddRefund(gas uint64) {
	s.journal.append(mvccRefundChange{prev: s.refund})
	s.refund += gas
}

func (s *MVCCStateDB) SubRefund(gas uint64) {
	s.journal.append(mvccRefundChange{prev: s.refund})
	if gas > s.refund {
		panic(fmt.Sprintf("Refund counter below zero (gas: %d > refund: %d)", gas, s.refund))
	}
	s.refund -= gas
}

func (s *MVCCStateDB) GetRefund() uint64 {
	return s.refund
}

func (s *MVCCStateDB) GetCommittedState(addr common.Address, hash common.Hash) common.Hash {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.GetCommittedState(hash)
	}
	return common.Hash{}
}

func (s *MVCCStateDB) GetState(addr common.Address, hash common.Hash) common.Hash {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.GetState(hash)
	}
	return common.Hash{}
}

func (s *MVCCStateDB) SetState(addr common.Address, key, value common.Hash) {
	stateObject := s.getOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetState(key, value)
	}
}

func (s *MVCCStateDB) GetTransientState(addr common.Address, key common.Hash) common.Hash {
	return s.transientStorage.Get(addr, key)
}

func (s *MVCCStateDB) setTransientState(addr common.Address, key, value common.Hash) {
	s.transientStorage.Set(addr, key, value)
}

func (s *MVCCStateDB) SetTransientState(addr common.Address, key, value common.Hash) {
	prev := s.GetTransientState(addr, key)
	if prev == value {
		return
	}
	s.journal.append(mvccTransientStorageChange{
		account:  &addr,
		key:      key,
		prevalue: prev,
	})
	s.setTransientState(addr, key, value)
}

func (s *MVCCStateDB) SelfDestruct(addr common.Address) {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return
	}
	if s.useCommutative {
		s.journal.append(mvccCommutativeSelfDestructChange{
			account:          &addr,
			prev:             stateObject.selfDestructed,
			prevbalancedelta: stateObject.balanceDelta,
		})
		stateObject.setBalanceDelta(nil)
	} else {
		s.journal.append(mvccSelfDestructChange{
			account:     &addr,
			prev:        stateObject.selfDestructed,
			prevbalance: new(uint256.Int).Set(stateObject.Balance()),
		})
		stateObject.setBalance(new(uint256.Int))
	}
	stateObject.markSelfdestructed()
}

func (s *MVCCStateDB) HasSelfDestructed(addr common.Address) bool {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.selfDestructed
	}
	return false
}

func (s *MVCCStateDB) Selfdestruct6780(addr common.Address) {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return
	}

	if stateObject.created {
		s.SelfDestruct(addr)
	}
}

func (s *MVCCStateDB) Exist(addr common.Address) bool {
	return s.getStateObject(addr) != nil
}

func (s *MVCCStateDB) Empty(addr common.Address) bool {
	so := s.getStateObject(addr)
	return so == nil || so.empty()
}

func (s *MVCCStateDB) AddressInAccessList(addr common.Address) bool {
	return s.accessList.ContainsAddress(addr)
}

func (s *MVCCStateDB) SlotInAccessList(addr common.Address, slot common.Hash) (addressPresent bool, slotPresent bool) {
	return s.accessList.Contains(addr, slot)
}

func (s *MVCCStateDB) AddAddressToAccessList(addr common.Address) {
	if s.accessList.AddAddress(addr) {
		s.journal.append(mvccAccessListAddAccountChange{&addr})
	}
}

func (s *MVCCStateDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	addrMod, slotMod := s.accessList.AddSlot(addr, slot)
	if addrMod {
		// In practice, this should not happen, since there is no way to enter the
		// scope of 'address' without having the 'address' become already added
		// to the access list (via call-variant, create, etc).
		// Better safe than sorry, though
		s.journal.append(mvccAccessListAddAccountChange{&addr})
	}
	if slotMod {
		s.journal.append(mvccAccessListAddSlotChange{
			address: &addr,
			slot:    &slot,
		})
	}
}

func (s *MVCCStateDB) Prepare(rules params.Rules, sender, coinbase common.Address, dst *common.Address, precompiles []common.Address, list types.AccessList) {
	if rules.IsBerlin {
		// Clear out any leftover from previous executions
		al := s.accessList
		s.accessList = al

		al.AddAddress(sender)
		if dst != nil {
			al.AddAddress(*dst)
			// If it's a create-tx, the destination will be added inside evm.create
		}
		for _, addr := range precompiles {
			al.AddAddress(addr)
		}
		for _, el := range list {
			al.AddAddress(el.Address)
			for _, key := range el.StorageKeys {
				al.AddSlot(el.Address, key)
			}
		}
		if rules.IsShanghai { // EIP-3651: warm coinbase
			al.AddAddress(coinbase)
		}
	}
	// Reset transient storage at the beginning of transaction execution
	// s.transientStorage = newTransientStorage()
}

func (s *MVCCStateDB) RevertToSnapshot(revid int) {
	// Find the snapshot in the stack of valid snapshots.
	idx := sort.Search(len(s.validRevisions), func(i int) bool {
		return s.validRevisions[i].id >= revid
	})
	if idx == len(s.validRevisions) || s.validRevisions[idx].id != revid {
		panic(fmt.Errorf("revision id %v cannot be reverted", revid))
	}
	snapshot := s.validRevisions[idx].journalIndex

	// Replay the journal to undo changes and remove invalidated snapshots
	s.journal.revert(s, snapshot)
	s.validRevisions = s.validRevisions[:idx]
	if s.RedoCtx != nil {
		s.RedoCtx.RevertToSnapshot(revid)
	}
}

func (s *MVCCStateDB) Snapshot() int {
	id := s.nextRevisionId
	s.nextRevisionId++
	s.validRevisions = append(s.validRevisions, revision{id, s.journal.length()})
	return id
}

func (s *MVCCStateDB) AddLog(log *types.Log) {
	s.journal.append(mvccAddLogChange{txhash: s.thash})

	log.TxHash = s.thash
	log.TxIndex = uint(s.txIndex)
	// log.Index = s.logSize
	s.logs = append(s.logs, log)
}

func (s *MVCCStateDB) AddPreimage(common.Hash, []byte) {

}

type RWSetStorage struct {
	Address common.Address
	Keys    []common.Hash
	PCs     []uint64
}

type RWSet struct {
	BalanceRead []common.Address
	NonceRead   []common.Address
	CodeRead    []common.Address
	StorageRead []RWSetStorage

	// account write is used to track account creation and selfdestruct
	// we do not track code write, which is implicitly tracked by account write
	AccountWrite []common.Address
	BalanceWrite []common.Address
	NonceWrite   []common.Address
	StorageWrite []RWSetStorage
}

func FindCriticalPath(rwSets []*RWSet) [][]int {
	// find critical path from rwSets
	criticalPath := make([][]int, len(rwSets))
	for i := range criticalPath {
		criticalPath[i] = []int{i}
	}
	accountWrite := make(map[common.Address]int)
	balanceWrite := make(map[common.Address]int)
	nonceWrite := make(map[common.Address]int)
	storageWrite := make(map[common.Address]map[common.Hash]int)
	for idx, rwset := range rwSets {
		conflictTxs := make(map[int]struct{})
		// check read set
		for _, address := range rwset.BalanceRead {
			if tx, ok := accountWrite[address]; ok {
				conflictTxs[tx] = struct{}{}
			} else if tx, ok := balanceWrite[address]; ok {
				conflictTxs[tx] = struct{}{}
			}
		}
		for _, address := range rwset.NonceRead {
			if tx, ok := accountWrite[address]; ok {
				conflictTxs[tx] = struct{}{}
			} else if tx, ok := nonceWrite[address]; ok {
				conflictTxs[tx] = struct{}{}
			}
		}
		for _, address := range rwset.CodeRead {
			if tx, ok := accountWrite[address]; ok {
				conflictTxs[tx] = struct{}{}
			}
		}
		for _, account := range rwset.StorageRead {
			if len(account.Keys) == 0 {
				continue
			}
			if tx, ok := accountWrite[account.Address]; ok {
				conflictTxs[tx] = struct{}{}
			} else if _, ok := storageWrite[account.Address]; ok {
				for _, key := range account.Keys {
					if tx, ok := storageWrite[account.Address][key]; ok {
						conflictTxs[tx] = struct{}{}
					}
				}
			}
		}
		// dp to find critical path, let f(i) is the slice of critical path if tx i is the last tx
		// f(i) = maxlen(append(f(j),i)) for all 0 <= j < i and j in conflictTxs
		for j := 0; j < idx; j++ {
			if _, ok := conflictTxs[j]; ok {
				if len(criticalPath[j])+1 > len(criticalPath[idx]) {
					criticalPath[idx] = slices.Clone(criticalPath[j])
					criticalPath[idx] = append(criticalPath[idx], idx)
					//  = append(criticalPath[j], idx)
				}
			}
		}

		// update write set
		for _, address := range rwset.AccountWrite {
			accountWrite[address] = idx
		}
		for _, address := range rwset.BalanceWrite {
			balanceWrite[address] = idx
		}
		for _, address := range rwset.NonceWrite {
			nonceWrite[address] = idx
		}
		for _, account := range rwset.StorageWrite {
			if _, ok := storageWrite[account.Address]; !ok {
				storageWrite[account.Address] = make(map[common.Hash]int)
			}
			for _, key := range account.Keys {
				storageWrite[account.Address][key] = idx
			}
		}
	}
	return criticalPath
}

// GetRWSet should be called before Validate and FinalizeToBaseStateDB
func (s *MVCCStateDB) GetRWSet(deleteEmptyObjects bool) *RWSet {
	rwset := &RWSet{}
	balanceReadMp := make(map[common.Address]struct{})
	nonceReadMp := make(map[common.Address]struct{})
	codeReadMp := make(map[common.Address]struct{})
	storageReadMp := make(map[common.Address]map[common.Hash]struct{})

	objs := make([]*MVCCStateObject, 0)

	objs = append(objs, s.replacedStateObjects...)
	objs = append(objs, s.danglingObjects...)

	// track write set
	for addr, obj := range s.mvccStateObjects {
		objs = append(objs, obj)
		// if obj.created || (obj.selfDestructed || (deleteEmptyObjects && obj.empty())) {
		if obj.created || obj.selfDestructed {
			rwset.AccountWrite = append(rwset.AccountWrite, addr)
			// if the account tracked in accountWrite, we do not need to track it in the following write sets
			continue
		}
		if obj.balanceDelta != nil || obj.dirty_balance != nil {
			rwset.BalanceWrite = append(rwset.BalanceWrite, addr)
		}
		if obj.dirty_nonce != nil {
			rwset.NonceWrite = append(rwset.NonceWrite, addr)
		}
		storageKeys := make([]common.Hash, 0, len(obj.dirtyStorage))
		storagePCs := make([]uint64, 0, len(obj.dirtyStorage))
		if len(obj.dirtyStorage) > 0 {
			for key, val := range obj.dirtyStorage {
				// sanity check
				if originVal, exist := obj.originStorage[key]; !exist {
					panic(fmt.Sprintf("storage key %v not exist in origin storage", key))
				} else if val == originVal {
					continue
				}
				storageKeys = append(storageKeys, key)
				if pc, ok := s.lastWritePCs[addr][key]; ok {
					storagePCs = append(storagePCs, pc)
				} else {
					panic(fmt.Sprintf("storage key %v not exist in lastWritePCs", key))
				}
			}
		}
		if len(storageKeys) > 0 {
			rwset.StorageWrite = append(rwset.StorageWrite, RWSetStorage{
				Address: addr,
				Keys:    storageKeys,
				PCs:     storagePCs,
			})
		}
	}
	// track read set
	for _, obj := range objs {
		addr := obj.Address()
		if obj.origin_balance != nil {
			// rwset.BalanceRead = append(rwset.BalanceRead, addr)
			balanceReadMp[addr] = struct{}{}
		}
		if obj.origin_nonce != nil {
			// rwset.NonceRead = append(rwset.NonceRead, addr)
			nonceReadMp[addr] = struct{}{}
		}
		if obj.origin_codehash != nil {
			// rwset.CodeRead = append(rwset.CodeRead, addr)
			codeReadMp[addr] = struct{}{}
		}
		if len(obj.originStorage) > 0 {
			keys := make([]common.Hash, 0, len(obj.originStorage))
			for key := range obj.originStorage {
				keys = append(keys, key)
			}
			// rwset.StorageRead = append(rwset.StorageRead, RWSetStorage{
			// 	Address: addr,
			// 	Keys:    keys,
			// })
			if _, ok := storageReadMp[addr]; !ok {
				storageReadMp[addr] = make(map[common.Hash]struct{})
			}
			for _, key := range keys {
				storageReadMp[addr][key] = struct{}{}
			}
		}
	}

	for addr := range balanceReadMp {
		rwset.BalanceRead = append(rwset.BalanceRead, addr)
	}
	for addr := range nonceReadMp {
		rwset.NonceRead = append(rwset.NonceRead, addr)
	}
	for addr := range codeReadMp {
		rwset.CodeRead = append(rwset.CodeRead, addr)
	}
	for addr, keys := range storageReadMp {
		keysSlice := make([]common.Hash, 0, len(keys))
		for key := range keys {
			keysSlice = append(keysSlice, key)
		}
		rwset.StorageRead = append(rwset.StorageRead, RWSetStorage{
			Address: addr,
			Keys:    keysSlice,
		})
	}

	return rwset
}

func (s *MVCCStateDB) Report() *StateReadInfo {
	info := &StateReadInfo{}
	for _, addr := range s.replacedStateObjects {
		info.Replaced = append(info.Replaced, addr.Address().Hex())
	}
	for addr := range s.originNonExistObjects {
		info.OriginNonExist = append(info.OriginNonExist, addr.Hex())
	}
	for addr, obj := range s.mvccStateObjects {
		if obj.origin_balance != nil {
			info.BalanceRead = append(info.BalanceRead, BalanceRead{
				Address: addr.Hex(),
				Val:     obj.origin_balance.String(),
			})
		}
		if obj.minBalanceDelta != nil {
			info.MinBalance = append(info.MinBalance, MinBalanceDelta{
				Address: addr.Hex(),
				Val:     obj.minBalanceDelta.String(),
			})
		}
		info.CodeRead = append(info.CodeRead, CodeRead{
			Address:        addr.Hex(),
			OriginCodeHash: hexutil.Encode(obj.origin_codehash),
			DirtyCodeHash:  hexutil.Encode(obj.dirty_codehash),
			// Code:           hexutil.Encode(obj.code),
		})
		if len(obj.originStorage) > 0 {
			slots := make([]Slot, 0, len(obj.originStorage))
			for key, value := range obj.originStorage {
				slots = append(slots, Slot{
					Key:   hexutil.Encode(key.Bytes()),
					Value: hexutil.Encode(value.Bytes()),
				})
			}
			sort.Slice(slots, func(i, j int) bool { return slots[i].Key < slots[j].Key })
			info.StorageRead = append(info.StorageRead, StorageRead{
				Address: addr.Hex(),
				Slots:   slots,
			})
		}

		if len(obj.dirtyStorage) > 0 {
			slots := make([]Slot, 0, len(obj.dirtyStorage))
			for key, value := range obj.dirtyStorage {
				slots = append(slots, Slot{
					Key:   hexutil.Encode(key.Bytes()),
					Value: hexutil.Encode(value.Bytes()),
				})
			}
			sort.Slice(slots, func(i, j int) bool { return slots[i].Key < slots[j].Key })
			info.StorageWrite = append(info.StorageWrite, StorageWrite{
				Address: addr.Hex(),
				Slots:   slots,
			})
		}
		if obj.created {
			info.CreatedAccount = append(info.CreatedAccount, addr.Hex())
		}
		info.BaseAccountsInfo = append(info.BaseAccountsInfo, BaseAccountInfo{
			Address:        addr.Hex(),
			ObjectPtrInMem: fmt.Sprintf("%p", obj.baseObj),
		})
	}
	sort.Slice(info.Replaced, func(i, j int) bool { return info.Replaced[i] < info.Replaced[j] })
	sort.Slice(info.OriginNonExist, func(i, j int) bool { return info.OriginNonExist[i] < info.OriginNonExist[j] })
	sort.Slice(info.BalanceRead, func(i, j int) bool { return info.BalanceRead[i].Address < info.BalanceRead[j].Address })
	sort.Slice(info.MinBalance, func(i, j int) bool { return info.MinBalance[i].Address < info.MinBalance[j].Address })
	sort.Slice(info.CodeRead, func(i, j int) bool { return info.CodeRead[i].Address < info.CodeRead[j].Address })
	sort.Slice(info.StorageRead, func(i, j int) bool { return info.StorageRead[i].Address < info.StorageRead[j].Address })
	sort.Slice(info.StorageWrite, func(i, j int) bool { return info.StorageWrite[i].Address < info.StorageWrite[j].Address })
	sort.Slice(info.CreatedAccount, func(i, j int) bool { return info.CreatedAccount[i] < info.CreatedAccount[j] })
	sort.Slice(info.BaseAccountsInfo, func(i, j int) bool { return info.BaseAccountsInfo[i].Address < info.BaseAccountsInfo[j].Address })
	return info
}

type BalanceRead struct {
	Address string
	Val     string
}

type MinBalanceDelta struct {
	Address string
	Val     string
}

type CodeRead struct {
	Address        string
	OriginCodeHash string
	DirtyCodeHash  string
	// Code           string
}

type Slot struct {
	Key   string
	Value string
}

type StorageRead struct {
	Address string
	Slots   []Slot
}

type StorageWrite struct {
	Address string
	Slots   []Slot
}

type BaseAccountInfo struct {
	Address        string
	ObjectPtrInMem string
}

// for debug
type StateReadInfo struct {
	Replaced         []string
	OriginNonExist   []string
	BalanceRead      []BalanceRead
	MinBalance       []MinBalanceDelta
	CodeRead         []CodeRead
	StorageRead      []StorageRead
	StorageWrite     []StorageWrite
	CreatedAccount   []string
	BaseAccountsInfo []BaseAccountInfo
}
