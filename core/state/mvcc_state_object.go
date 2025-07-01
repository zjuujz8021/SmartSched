package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type mvccBaseStateObject struct {
	// obj must NOT be nil
	// obj *stateObject
	db *MVCCBaseStateDB

	address common.Address

	nonce   atomic.Uint64
	balance atomic.Pointer[uint256.Int]

	root common.Hash // merkle root of the storage trie

	codeHash []byte
	code     Code // contract bytecode, which gets set when code is loaded
	codeMu   sync.RWMutex

	trie     Trie
	trieOnce sync.Once

	originStorage cmap.ConcurrentMap[common.Hash, common.Hash]
	// originStorageMu sync.RWMutex

	diskStorage cmap.ConcurrentMap[common.Hash, common.Hash]
	// diskStorageMu sync.RWMutex

	deleted atomic.Bool
	created atomic.Bool
}

func storagesharding(key common.Hash) uint32 {
	return binary.BigEndian.Uint32(key[common.HashLength-4:])
}

func newMvccBaseStateObject(db *MVCCBaseStateDB, addr common.Address, data *types.StateAccount) *mvccBaseStateObject {
	obj := &mvccBaseStateObject{
		db:            db,
		address:       addr,
		originStorage: cmap.NewWithCustomShardingFunction[common.Hash, common.Hash](storagesharding),
		diskStorage:   cmap.NewWithCustomShardingFunction[common.Hash, common.Hash](storagesharding),
	}
	if data != nil {
		obj.nonce.Store(data.Nonce)
		obj.balance.Store(data.Balance)
		obj.codeHash = data.CodeHash
		obj.root = data.Root
	} else {
		obj.balance.Store(new(uint256.Int))
		obj.codeHash = types.EmptyCodeHash[:]
		obj.root = types.EmptyRootHash
	}
	return obj
}

func (s *mvccBaseStateObject) DeepCopy(newdb *MVCCBaseStateDB) *mvccBaseStateObject {
	cp := &mvccBaseStateObject{
		db:            newdb,
		address:       s.address,
		root:          s.root,
		codeHash:      bytes.Clone(s.codeHash),
		code:          bytes.Clone(s.code),
		originStorage: cmap.NewWithCustomShardingFunction[common.Hash, common.Hash](storagesharding),
		diskStorage:   cmap.NewWithCustomShardingFunction[common.Hash, common.Hash](storagesharding),
	}

	cp.nonce.Store(s.nonce.Load())
	cp.balance.Store(new(uint256.Int).Set(s.balance.Load()))
	cp.deleted.Store(s.deleted.Load())
	cp.created.Store(s.created.Load())

	s.originStorage.IterCb(func(key, v common.Hash) {
		cp.originStorage.Set(key, v)
	})
	s.diskStorage.IterCb(func(key, v common.Hash) {
		cp.diskStorage.Set(key, v)
	})

	return cp
}

func (s *mvccBaseStateObject) getDeleted() bool {
	return s.deleted.Load()
}

func (s *mvccBaseStateObject) setDeleted(deleted bool) {
	s.deleted.Store(deleted)
}

func (s *mvccBaseStateObject) getNonce() uint64 {
	return s.nonce.Load()
}

func (s *mvccBaseStateObject) getBalance() *uint256.Int {
	return s.balance.Load()
}

func (s *mvccBaseStateObject) getCodeHashAndCode() ([]byte, []byte) {
	s.codeMu.RLock()
	code := s.code
	codeHash := s.codeHash
	s.codeMu.RUnlock()
	if code != nil {
		return codeHash, code
	}
	if bytes.Equal(codeHash, types.EmptyCodeHash[:]) {
		return types.EmptyCodeHash[:], nil
	}
	code, err := s.db.db.ContractCode(s.address, common.BytesToHash(codeHash))
	if err != nil {
		s.db.SetError(fmt.Errorf("can't load code hash %x: %v", codeHash, err))
	}

	s.codeMu.Lock()
	s.code = code
	s.codeMu.Unlock()
	return codeHash, code
}

// func (s *mvccBaseStateObject) getCodeSize() int {
// 	return len(s.getCode())
// }

// this function should ONLY be used for validation
func (s *mvccBaseStateObject) getCodeHash() []byte {
	s.codeMu.RLock()
	defer s.codeMu.RUnlock()
	return s.codeHash
}

func (s *mvccBaseStateObject) getCommittedState(key common.Hash) common.Hash {
	// s.pendingStorageMu.RLock()
	// value, ok := s.obj.pendingStorage[key]
	// s.pendingStorageMu.RUnlock()
	// if ok {
	// return value
	// }

	value, ok := s.originStorage.Get(key)
	if ok {
		return value
	}

	if s.created.Load() {
		return common.Hash{}
	}

	value, ok = s.diskStorage.Get(key)
	if ok {
		return value
	}
	s.trieOnce.Do(func() {
		var err error
		// if s.Root == types.EmptyRootHash {
		// 	return
		// }
		s.trie, err = s.db.db.OpenStorageTrie(common.Hash{}, s.address, common.Hash{}, nil)
		if err != nil {
			s.db.SetError(err)
		}
	})
	if s.trie == nil {
		return common.Hash{}
	}
	ret, err := s.trie.GetStorage(s.address, key[:])
	if err != nil {
		s.db.SetError(err)
		return common.Hash{}
	}
	loaded := common.Hash{}
	loaded.SetBytes(ret)
	// IMPORTANT: committted state from trie should NOT write to origin storage !!!
	// because the write will cause inconsistency between trie and origin storage (write by another tx)
	// thus, we add another diskStorage to store the committed state from trie.
	s.diskStorage.Set(key, loaded)
	return loaded
}

func (s *mvccBaseStateObject) resetByBaseMVCCStateObject(newobj *mvccBaseStateObject) {
	// this obj may be deleted or reset by previous tx
	// if deleted, we should "re-activate" this object
	if newobj.getDeleted() {
		s.setDeleted(true)
		return
	}
	s.setDeleted(false)

	// reset means the object is created by current tx
	s.created.Store(true)

	s.nonce.Store(newobj.getNonce())
	s.balance.Store(newobj.getBalance())

	s.codeMu.Lock()
	s.codeHash = newobj.codeHash
	s.code = newobj.code
	s.codeMu.Unlock()

	s.originStorage = newobj.originStorage

	// s.diskStorageMu.Lock()
	// for k, v := range newobj.diskStorage {
	// 	s.diskStorage[k] = v
	// }
	// s.diskStorageMu.Unlock()
}

// finalize should be called after the transaction is done.
func (s *mvccBaseStateObject) updateByMVCCStateObject(obj *MVCCStateObject) {
	// if obj.selfDestructed || (deleteEmptyObjects && obj.empty()) {
	// 	s.deleted.Store(true)
	// 	return
	// }

	if obj.created {
		s.created.Store(true)
	}

	if !obj.db.disableNonceCheck && obj.dirty_nonce != nil {
		s.nonce.Store(*obj.dirty_nonce)
	}

	if obj.dirty_balance != nil || obj.balanceDelta != nil {
		currBalance := obj.Balance()
		if !currBalance.Eq(s.getBalance()) {
			s.balance.Store(obj.Balance())
		}
	}

	if obj.dirty_codehash != nil {
		s.codeMu.Lock()
		s.codeHash = obj.dirty_codehash
		s.code = obj.code
		s.codeMu.Unlock()
	}

	if len(obj.dirtyStorage) > 0 {
		for k, v := range obj.dirtyStorage {
			s.originStorage.Set(k, v)
		}
	}
}

type MVCCStateObject struct {
	db *MVCCStateDB

	// baseObj may be nil, if the previous state is not exist (deleted or nil).
	baseObj *mvccBaseStateObject

	// TODO: consider init nonce before tx start
	// nonce will never be used except for tx creation
	origin_nonce *uint64
	dirty_nonce  *uint64

	origin_balance *uint256.Int

	dirty_balance   *uint256.Int
	balanceDelta    *big.Int
	minBalanceDelta *big.Int

	code            Code
	origin_codehash []byte
	dirty_codehash  []byte

	originStorage Storage
	dirtyStorage  Storage

	// // originDeleted means nil or deleted by previous tx.
	// originDeleted bool

	// Flag whether the account was marked as self-destructed. The self-destructed account
	// is still accessible in the scope of same transaction.
	// smartsched: this could be considered "currentDeleted".
	selfDestructed bool

	// Flag whether the object was created in the current transaction
	// NOTE: if baseObj is nil, created will be true.
	created bool
}

func newMvccStateObject(statedb *MVCCStateDB, baseObj *mvccBaseStateObject, created bool) *MVCCStateObject {
	obj := &MVCCStateObject{
		// address:       baseObj.obj.address,
		// addrhash:      baseObj.obj.addrHash,
		db:            statedb,
		baseObj:       baseObj,
		originStorage: make(Storage),
		dirtyStorage:  make(Storage),
	}

	if created {
		obj.dirty_nonce = new(uint64)
		// obj.dirty_balance = new(uint256.Int)
		obj.dirty_codehash = types.EmptyCodeHash[:]
		obj.created = true
	}
	return obj
}

func (s *MVCCStateObject) FinalizeToBaseObject() {
	s.baseObj.updateByMVCCStateObject(s)
}

func (s *MVCCStateObject) Address() common.Address {
	return s.baseObj.address
}

func (s *MVCCStateObject) empty() bool {
	return s.Nonce() == 0 && bytes.Equal(s.CodeHash(), types.EmptyCodeHash[:]) && s.Balance().IsZero()
}

func (s *MVCCStateObject) markSelfdestructed() {
	s.selfDestructed = true
}

func (s *MVCCStateObject) touch() {
	// TODO: why touch?
	s.db.journal.append(mvccTouchChange{
		account: &s.baseObj.address,
	})
	if s.baseObj.address == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		s.db.journal.dirty(s.baseObj.address)
	}
}

func (s *MVCCStateObject) validate() ConflictType {
	// the validation for created account is done with originNonExistObjects and replacedStateObjects
	if s.created {
		if s.minBalanceDelta != nil {
			minBalance := new(big.Int).Add(s.baseObj.getBalance().ToBig(), s.minBalanceDelta)
			if minBalance.Sign() < 0 {
				return BalanceDeltaConflict
			}
		}
		return NoConflict
	}

	// check if a account is deleted
	if s.baseObj.getDeleted() {
		return AccountDeletedConflict
	}

	if s.origin_codehash != nil && !bytes.Equal(s.origin_codehash, s.baseObj.getCodeHash()) {
		return CodeConflict
	}

	if !s.db.disableNonceCheck && s.origin_nonce != nil && *s.origin_nonce != s.baseObj.getNonce() {
		return NonceConflict
	}

	if s.origin_balance != nil && s.origin_balance.Cmp(s.baseObj.getBalance()) != 0 {
		return BalanceConflict
	}

	for key, value := range s.originStorage {
		if value != s.baseObj.getCommittedState(key) {
			return StorageConflict
		}
	}

	if s.minBalanceDelta != nil {
		minBalance := new(big.Int).Add(s.baseObj.getBalance().ToBig(), s.minBalanceDelta)
		if minBalance.Sign() < 0 {
			return BalanceDeltaConflict
		}
	}

	return NoConflict
}

func (s *MVCCStateObject) validateAndSetRedoContext() (map[common.Hash]common.Hash, bool) {
	// the validation for created account is done with originNonExistObjects and replacedStateObjects
	if s.created {
		if s.minBalanceDelta != nil {
			minBalance := new(big.Int).Add(s.baseObj.getBalance().ToBig(), s.minBalanceDelta)
			if minBalance.Sign() < 0 {
				s.db.RedoCtx.IsUnrecoverableStorageConflict = true
				return nil, false
			}
		}
		return nil, true
	}

	// check if a account is deleted
	if s.baseObj.getDeleted() {
		s.db.RedoCtx.IsUnrecoverableStorageConflict = true
		return nil, false
	}

	if s.origin_codehash != nil && !bytes.Equal(s.origin_codehash, s.baseObj.getCodeHash()) {
		s.db.RedoCtx.IsCodeConflict = true
		return nil, false
	}

	if s.minBalanceDelta != nil {
		minBalance := new(big.Int).Add(s.baseObj.getBalance().ToBig(), s.minBalanceDelta)
		if minBalance.Sign() < 0 {
			s.db.RedoCtx.IsUnrecoverableStorageConflict = true
			return nil, false
		}
	}

	if !s.db.disableNonceCheck && s.origin_nonce != nil {
		originNonce := s.baseObj.getNonce()
		if originNonce != *s.origin_nonce {
			*s.origin_nonce = originNonce
			s.db.RedoCtx.IsNonceConflict = true
		}
	}

	if s.origin_balance != nil && s.origin_balance.Cmp(s.baseObj.getBalance()) != 0 {
		s.db.RedoCtx.IsBalanceConflict = true
	}

	var conflictSlot Storage
	for key, value := range s.originStorage {
		committedValue := s.baseObj.getCommittedState(key)
		if value != committedValue {
			s.db.RedoCtx.IsStorageConflict = true
			if conflictSlot == nil {
				conflictSlot = make(Storage)
			}
			conflictSlot[key] = committedValue
		}
	}

	return conflictSlot, true
}

func (s *MVCCStateObject) Nonce() uint64 {
	if s.db.disableNonceCheck {
		return 0
	}
	if s.dirty_nonce != nil {
		return *s.dirty_nonce
	}
	if s.origin_nonce == nil {
		s.origin_nonce = new(uint64)
		if s.db.enableSyncronizer {
			s.db.syncronizer.WaitForNonceReady(s.db.txIndex, s.Address())
		}
		if s.db.enable2PL {
			s.db.baseStateDB.nonceLock.AcquireReadLock(s.Address(), s.db.txIndex, s.db.cancelled)
		}
		*s.origin_nonce = s.baseObj.getNonce()
	}
	return *s.origin_nonce
}

func (s *MVCCStateObject) setOriginNonce(nonce uint64) {
	s.origin_nonce = &nonce
}

func (s *MVCCStateObject) setNonce(nonce uint64) {
	if s.dirty_nonce == nil {
		s.dirty_nonce = new(uint64)
	}
	*s.dirty_nonce = nonce
}

func (s *MVCCStateObject) SetNonce(nonce uint64) {
	if s.db.enable2PL {
		s.db.baseStateDB.nonceLock.AcquireWriteLock(s.Address(), s.db.txIndex, s.db.cancelled)
	}

	s.db.journal.append(mvccNonceChange{
		account: &s.baseObj.address,
		prev:    s.Nonce(),
	})
	s.setNonce(nonce)
}

func (s *MVCCStateObject) OriginBalance() *uint256.Int {
	if s.origin_balance == nil {
		if s.db.enableSyncronizer {
			s.db.syncronizer.WaitForBalanceReady(s.db.txIndex, s.Address())
		}
		if s.db.enable2PL {
			s.db.baseStateDB.balanceLock.AcquireReadLock(s.Address(), s.db.txIndex, s.db.cancelled)
		}
		s.origin_balance = new(uint256.Int).Set(s.baseObj.getBalance())
	}
	return s.origin_balance
}

func (s *MVCCStateObject) Balance() *uint256.Int {
	if s.dirty_balance != nil {
		// TODO: fix it: commutative and dirty balance both exist
		// use commutative and dirty balance both exist: used by ParallelEVM - redo balance log
		return s.dirty_balance
	}
	if s.db.useCommutative {
		if s.balanceDelta == nil {
			return s.OriginBalance()
		}
		balanceBig := new(big.Int).Add(s.OriginBalance().ToBig(), s.balanceDelta)
		balance := new(uint256.Int)
		balance.SetFromBig(balanceBig)
		return balance
	}
	return s.OriginBalance()
}

// func (s *MVCCStateObject) setOriginBalance(amount *uint256.Int) {
// 	s.origin_balance = amount
// }

func (s *MVCCStateObject) setBalanceDelta(delta *big.Int) {
	s.balanceDelta = delta
}

func (s *MVCCStateObject) SetBalanceDelta(delta *big.Int) {
	s.db.journal.append(mvccCommutativeBalanceChange{
		account: &s.baseObj.address,
		prev:    s.balanceDelta,
	})
	s.setBalanceDelta(delta)
}

func (s *MVCCStateObject) setBalance(amount *uint256.Int) {
	s.dirty_balance = amount
}

func (s *MVCCStateObject) SetBalance(amount *uint256.Int) {
	s.db.journal.append(mvccBalanceChange{
		account: &s.baseObj.address,
		prev:    new(uint256.Int).Set(s.Balance()),
	})
	s.setBalance(amount)
}

func (s *MVCCStateObject) AddBalance(delta *uint256.Int) {
	if delta.IsZero() {
		// if s.empty() {
		// s.touch()
		// }
		return
	}

	if s.db.enable2PL {
		s.db.baseStateDB.balanceLock.AcquireWriteLock(s.Address(), s.db.txIndex, s.db.cancelled)
	}

	if s.db.useCommutative {
		if s.balanceDelta == nil {
			s.balanceDelta = new(big.Int)
		}
		s.SetBalanceDelta(new(big.Int).Add(s.balanceDelta, delta.ToBig()))
	} else {
		s.SetBalance(new(uint256.Int).Add(s.Balance(), delta))
	}
}

func (s *MVCCStateObject) SubBalance(delta *uint256.Int) {
	if delta.IsZero() {
		return
	}

	if s.db.enable2PL {
		s.db.baseStateDB.balanceLock.AcquireWriteLock(s.Address(), s.db.txIndex, s.db.cancelled)
	}

	if s.db.useCommutative {
		if s.balanceDelta == nil {
			s.balanceDelta = new(big.Int)
		}
		s.SetBalanceDelta(new(big.Int).Sub(s.balanceDelta, delta.ToBig()))
		if s.balanceDelta.Sign() < 0 && (s.minBalanceDelta == nil || s.balanceDelta.Cmp(s.minBalanceDelta) < 0) {
			s.minBalanceDelta = new(big.Int).Set(s.balanceDelta)
		}
	} else {
		s.SetBalance(new(uint256.Int).Sub(s.Balance(), delta))
	}
}

// func (s *MVCCStateObject) AddBalance(amount *uint256.Int) {
// 	// EIP161: We must check emptiness for the objects such that the account
// 	// clearing (0,0,0 objects) can take effect.
// 	if amount.IsZero() {
// 		if s.empty() {
// 			s.touch()
// 		}
// 		return
// 	}
// 	s.SetBalance(new(uint256.Int).Add(s.Balance(), amount))
// }

// func (s *MVCCStateObject) SubBalance(amount *uint256.Int) {
// 	if amount.IsZero() {
// 		return
// 	}
// 	s.SetBalance(new(uint256.Int).Sub(s.Balance(), amount))
// }

func (s *MVCCStateObject) CodeHash() []byte {
	if s.dirty_codehash != nil {
		return s.dirty_codehash
	}
	if s.origin_codehash == nil {
		// if s.db.syncronizer != nil {
		// 	s.db.syncronizer.WaitForCodeReady(s.db.txIndex, s.Address())
		// }
		s.origin_codehash, s.code = s.baseObj.getCodeHashAndCode()
	}
	return s.origin_codehash
}

func (s *MVCCStateObject) CodeSize() int {
	if s.origin_codehash == nil && s.dirty_codehash == nil {
		// if s.db.syncronizer != nil {
		// 	s.db.syncronizer.WaitForCodeReady(s.db.txIndex, s.Address())
		// }
		s.origin_codehash, s.code = s.baseObj.getCodeHashAndCode()
	}
	return len(s.code)
}

func (s *MVCCStateObject) Code() []byte {
	if s.origin_codehash == nil && s.dirty_codehash == nil {
		// if s.db.syncronizer != nil {
		// 	s.db.syncronizer.WaitForCodeReady(s.db.txIndex, s.Address())
		// }
		s.origin_codehash, s.code = s.baseObj.getCodeHashAndCode()
	}
	return s.code
}

func (s *MVCCStateObject) setCode(codeHash common.Hash, code []byte) {
	s.code = code
	s.dirty_codehash = codeHash[:]
}

func (s *MVCCStateObject) SetCode(codeHash common.Hash, code []byte) {
	prevcode := s.Code()
	s.db.journal.append(mvccCodeChange{
		account:  &s.baseObj.address,
		prevhash: s.CodeHash(),
		prevcode: prevcode,
	})
	s.setCode(codeHash, code)
}

type ReadPrecommitResult struct {
	Storage common.Hash
	Balance *uint256.Int
	Ready   bool
}

func (s *MVCCStateObject) GetCommittedState(key common.Hash) common.Hash {
	if s.created {
		return common.Hash{}
	}
	if value, exist := s.originStorage[key]; exist {
		return value
	}
	var precommitresult ReadPrecommitResult
	if s.db.enableSyncronizer {
		s.db.syncronizer.WaitForStorageReady(s.db.txIndex, s.Address(), key, &precommitresult)
	}
	if s.db.enable2PL {
		s.db.baseStateDB.storageLock.AcquireReadLock(toStorageKey(s.Address(), key[:]), s.db.txIndex, s.db.cancelled)
	}
	if precommitresult.Ready {
		s.originStorage[key] = precommitresult.Storage
		return precommitresult.Storage
	}
	value := s.baseObj.getCommittedState(key)
	s.originStorage[key] = value
	return value
}

func (s *MVCCStateObject) GetState(key common.Hash) common.Hash {
	if value, dirty := s.dirtyStorage[key]; dirty {
		return value
	}
	return s.GetCommittedState(key)
}

func (s *MVCCStateObject) setState(key, value common.Hash) {
	s.dirtyStorage[key] = value
}

func (s *MVCCStateObject) SetState(key, value common.Hash) {
	if s.db.enable2PL {
		s.db.baseStateDB.storageLock.AcquireWriteLock(toStorageKey(s.Address(), key[:]), s.db.txIndex, s.db.cancelled)
	}
	prev := s.GetState(key)
	if prev == value {
		return
	}
	addr := s.Address()
	s.db.journal.append(mvccStorageChange{
		account:  &addr,
		key:      key,
		prevalue: prev,
	})
	s.setState(key, value)
}
