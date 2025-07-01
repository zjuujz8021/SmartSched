package state

import (
	"bytes"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

const (
	WaitTime = 50 * time.Microsecond
)

const (
	accountType byte = 'A'
	codeType    byte = 'C'
	emptyType   byte = 'E'
	stateType   byte = 'S'
	globalType  byte = 'G'
)

var _ Trie = (*SlimArchiveTrie)(nil)
var _ Database = (*SlimArchiveTrie)(nil)

type StateReader interface {
	Get(key []byte) ([]byte, error)
	Scan(start, end []byte, limit int, reverse bool) (keys [][]byte, values [][]byte, err error)
}

type SlimArchiveTrie struct {
	blockNumber uint64
	txnIndex    int
	suffix      []byte

	db StateReader

	latestSuicide     []byte
	latestSuicideOnce sync.Once

	codeCache        sync.Map
	accountCache     sync.Map
	storageTrieCache sync.Map
	storageCache     sync.Map

	Hit          *int64
	Miss         *int64
	SimulateWait *bool
}

func NewSlimArchiveTrie(blockNumber uint64, txnIndex int, db StateReader) *SlimArchiveTrie {
	readTrie := &SlimArchiveTrie{
		blockNumber: blockNumber,
		txnIndex:    txnIndex,
		suffix:      make([]byte, 8),
		db:          db,

		Hit:          new(int64),
		Miss:         new(int64),
		SimulateWait: new(bool),
	}

	binary.BigEndian.PutUint64(readTrie.suffix, blockNumber<<16|uint64(txnIndex))

	return readTrie
}

func (t *SlimArchiveTrie) ResetHitMiss() {
	*t.Hit = 0
	*t.Miss = 0
}

func (t *SlimArchiveTrie) EnableSimulateWait() {
	*t.SimulateWait = true
}

func (t *SlimArchiveTrie) DisableSimulateWait() {
	*t.SimulateWait = false
}

func (t *SlimArchiveTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	if *t.SimulateWait {
		time.Sleep(WaitTime)
	}
	storageKey := common.BytesToHash(key)
	val, ok := t.storageCache.Load(storageKey)
	if ok {
		atomic.AddInt64(t.Hit, 1)
		return val.([]byte), nil
	}
	atomic.AddInt64(t.Miss, 1)

	prefix := t.jointPrefix([]byte{stateType}, addr.Bytes(), key)
	keys, vals, err := t.db.Scan(prefix, t.jointKey(prefix, t.suffix), 1, true)
	if err != nil {
		return nil, err
	}
	if len(keys) > 0 {
		if writeAt, suicideAt := t.extractPostfix(keys[0]), t.tryGetEmpty(addr); bytes.Compare(writeAt, suicideAt) > 0 {
			val, err := t.decodeValue(vals[0])
			if err == nil {
				t.storageCache.LoadOrStore(storageKey, val)
			}
			return val, err
		} else {
			t.storageCache.LoadOrStore(storageKey, []byte{})
			return common.Hash{}.Bytes(), nil
		}
	}
	t.storageCache.LoadOrStore(storageKey, []byte{})

	return nil, nil
}

func (t *SlimArchiveTrie) GetAccount(address common.Address) (*types.StateAccount, error) {
	if *t.SimulateWait {
		time.Sleep(WaitTime)
	}
	val, ok := t.accountCache.Load(address)
	if ok {
		atomic.AddInt64(t.Hit, 1)
		if val == nil {
			return nil, nil
		}
		return val.(*types.StateAccount), nil
	}

	atomic.AddInt64(t.Miss, 1)

	prefix := t.jointPrefix([]byte{accountType}, address.Bytes())

	keys, vals, err := t.db.Scan(prefix, t.jointKey(prefix, t.suffix), 1, true)
	if err != nil {
		return nil, err
	}
	if len(keys) > 0 {
		if len(vals[0]) == 0 {
			// return nil, ErrAccountDeleted
			t.accountCache.LoadOrStore(address, nil)
			return nil, nil
		}
		acct, err := t.decodeAccount(vals[0])
		if err != nil {
			return nil, err
		}
		old, loaded := t.accountCache.LoadOrStore(address, acct)
		if loaded {
			return old.(*types.StateAccount), nil
		}
		return acct, nil
	}
	t.accountCache.LoadOrStore(address, nil)
	return nil, nil
}

func (t *SlimArchiveTrie) tryGetEmpty(addr common.Address) []byte {
	t.latestSuicideOnce.Do(func() {
		prefix := t.jointPrefix([]byte{emptyType}, addr[:])
		keys, _, err := t.db.Scan(prefix, t.jointKey(prefix, t.suffix), 1, true)
		if err != nil {
			log.Error("try get empty failed", "err", err)
			panic("db err when get empty")
		}
		if len(keys) > 0 {
			t.latestSuicide = t.extractPostfix(keys[0])
		} else {
			t.latestSuicide = make([]byte, 8)
		}
	})

	return t.latestSuicide
}

func (t *SlimArchiveTrie) jointPrefix(prefixes ...[]byte) []byte {
	return bytes.Join(prefixes, []byte{})
}

func (t *SlimArchiveTrie) jointKey(keys ...[]byte) []byte {
	return bytes.Join(keys, []byte{})
}

func (t *SlimArchiveTrie) extractPostfix(key []byte) []byte {
	return key[len(key)-8:]
}

func (t *SlimArchiveTrie) decodeAccount(data []byte) (*types.StateAccount, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var account = new(types.StateAccount)
	err := rlp.DecodeBytes(data, account)
	return account, err
}

func (t *SlimArchiveTrie) decodeValue(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	_, content, _, err := rlp.Split(data)
	return content, err
}

func (t *SlimArchiveTrie) OpenTrie(root common.Hash) (Trie, error) {
	return t, nil
}

func (t *SlimArchiveTrie) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash, trie Trie) (Trie, error) {
	val, ok := t.storageTrieCache.Load(address)
	if ok {
		return val.(Trie), nil
	}

	newtrie := &SlimArchiveTrie{
		blockNumber: t.blockNumber,
		txnIndex:    t.txnIndex,
		// addrHash:    addrHash,
		suffix: t.suffix,
		db:     t.db,

		Hit:          t.Hit,
		Miss:         t.Miss,
		SimulateWait: t.SimulateWait,
	}
	oldtrie, loaded := t.storageTrieCache.LoadOrStore(address, newtrie)
	if loaded {
		return oldtrie.(Trie), nil
	}
	return newtrie, nil
}

func (t *SlimArchiveTrie) ContractCode(addr common.Address, codeHash common.Hash) ([]byte, error) {
	if *t.SimulateWait {
		time.Sleep(WaitTime)
	}
	val, ok := t.codeCache.Load(codeHash)
	if ok {
		atomic.AddInt64(t.Hit, 1)
		return val.([]byte), nil
	}
	atomic.AddInt64(t.Miss, 1)
	code, err := t.db.Get(t.jointPrefix([]byte{codeType}, codeHash.Bytes()))
	if err != nil {
		return nil, err
	}
	old, loaded := t.codeCache.LoadOrStore(codeHash, code)
	if loaded {
		return old.([]byte), nil
	}
	return code, nil
}

func (t *SlimArchiveTrie) ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	code, err := t.ContractCode(addr, codeHash)
	return len(code), err
}

func (t *SlimArchiveTrie) GetKey(shaKey []byte) []byte {
	return nil
}

func (t *SlimArchiveTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	return nil
}

func (t *SlimArchiveTrie) UpdateAccount(address common.Address, account *types.StateAccount) error {
	return nil
}

func (t *SlimArchiveTrie) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	return nil
}

func (t *SlimArchiveTrie) DeleteStorage(addr common.Address, key []byte) error {
	return nil
}

func (t *SlimArchiveTrie) DeleteAccount(address common.Address) error {
	return nil
}

func (t *SlimArchiveTrie) Hash() common.Hash {
	return common.Hash{}
}

func (t *SlimArchiveTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet, error) {
	return common.Hash{}, nil, nil
}

func (t *SlimArchiveTrie) NodeIterator(startKey []byte) (trie.NodeIterator, error) {
	return nil, nil
}

func (t *SlimArchiveTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return nil
}

func (t *SlimArchiveTrie) CopyTrie(Trie) Trie {
	return nil
}

func (t *SlimArchiveTrie) DiskDB() ethdb.KeyValueStore {
	return nil
}

func (t *SlimArchiveTrie) TrieDB() *triedb.Database {
	return nil
}
