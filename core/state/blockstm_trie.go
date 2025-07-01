package state

import (
	"bytes"

	storetypes "cosmossdk.io/store/types"
	blockstm "github.com/crypto-org-chain/go-block-stm"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

var (
	StoreKeyAccount = storetypes.NewKVStoreKey("account")
	StoreKeyStorage = storetypes.NewKVStoreKey("storage")
	StoreKeyCode    = storetypes.NewKVStoreKey("code")
)

var STMStores = map[storetypes.StoreKey]int{
	StoreKeyAccount: 0,
	StoreKeyStorage: 1,
	StoreKeyCode:    2,
}

type StorageKey [common.AddressLength + common.HashLength]byte

func toStorageKey(addr common.Address, slot []byte) (key StorageKey) {
	copy(key[:common.AddressLength], addr.Bytes())
	copy(key[common.AddressLength:], slot)
	return key
}

type BlockSTMTrie struct {
	view blockstm.MultiStore
}

func NewBlockSTMTrie(view blockstm.MultiStore) *BlockSTMTrie {
	return &BlockSTMTrie{view: view}
}

func (t *BlockSTMTrie) GetAccount(address common.Address) (*types.StateAccount, error) {
	store := t.view.GetKVStore(StoreKeyAccount)
	data := store.Get(address.Bytes())
	if len(data) == 0 {
		return nil, nil
	}
	acct := new(types.StateAccount)
	if err := rlp.DecodeBytes(data, acct); err != nil {
		return nil, err
	}
	return acct, nil
}

func (t *BlockSTMTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	storageKey := toStorageKey(addr, key)
	store := t.view.GetKVStore(StoreKeyStorage)
	return store.Get(storageKey[:]), nil
}

func (t *BlockSTMTrie) OpenTrie(root common.Hash) (Trie, error) {
	return t, nil
}

func (t *BlockSTMTrie) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash, trie Trie) (Trie, error) {
	return t, nil
}

func (t *BlockSTMTrie) ContractCode(addr common.Address, codeHash common.Hash) ([]byte, error) {
	store := t.view.GetKVStore(StoreKeyCode)
	return store.Get(codeHash[:]), nil
}

func (t *BlockSTMTrie) ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	code, err := t.ContractCode(addr, codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (t *BlockSTMTrie) GetKey(shaKey []byte) []byte {
	return nil
}

func (t *BlockSTMTrie) UpdateAccountWithOrigin(address common.Address, account, origin *types.StateAccount) error {
	if origin != nil {
		if origin.Nonce == account.Nonce && origin.Balance.Cmp(account.Balance) == 0 && bytes.Equal(origin.CodeHash, account.CodeHash) {
			return nil
		}
	}
	store := t.view.GetKVStore(StoreKeyAccount)
	data, err := rlp.EncodeToBytes(account)
	if err != nil {
		return err
	}
	store.Set(address.Bytes(), data)
	return nil
}

func (t *BlockSTMTrie) UpdateAccount(address common.Address, account *types.StateAccount) error {
	store := t.view.GetKVStore(StoreKeyAccount)
	data, err := rlp.EncodeToBytes(account)
	if err != nil {
		return err
	}
	store.Set(address.Bytes(), data)
	return nil
}

func (t *BlockSTMTrie) DeleteAccount(address common.Address) error {
	store := t.view.GetKVStore(StoreKeyAccount)
	store.Set(address.Bytes(), []byte{})
	return nil
}

func (t *BlockSTMTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	store := t.view.GetKVStore(StoreKeyStorage)
	storageKey := toStorageKey(addr, key)
	store.Set(storageKey[:], value)
	return nil
}

func (t *BlockSTMTrie) DeleteStorage(addr common.Address, key []byte) error {
	store := t.view.GetKVStore(StoreKeyStorage)
	storageKey := toStorageKey(addr, key)
	store.Set(storageKey[:], []byte{})
	return nil
}

func (t *BlockSTMTrie) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	if len(code) == 0 || codeHash == types.EmptyCodeHash {
		return nil
	}
	store := t.view.GetKVStore(StoreKeyCode)
	store.Set(codeHash[:], code)
	return nil
}

func (t *BlockSTMTrie) Hash() common.Hash {
	return common.Hash{}
}

func (t *BlockSTMTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet, error) {
	return common.Hash{}, nil, nil
}

func (t *BlockSTMTrie) NodeIterator(startKey []byte) (trie.NodeIterator, error) {
	return nil, nil
}

func (t *BlockSTMTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return nil
}

func (t *BlockSTMTrie) CopyTrie(Trie) Trie {
	return nil
}

func (t *BlockSTMTrie) DiskDB() ethdb.KeyValueStore {
	return nil
}

func (t *BlockSTMTrie) TrieDB() *triedb.Database {
	return nil
}

func SlimArchiveTrieToSTMStorage(tr *SlimArchiveTrie) *blockstm.MultiMemDB {
	mmdb := blockstm.NewMultiMemDB(STMStores)
	acctStore := mmdb.GetKVStore(StoreKeyAccount)
	storageStore := mmdb.GetKVStore(StoreKeyStorage)
	codeStore := mmdb.GetKVStore(StoreKeyCode)

	tr.accountCache.Range(func(key, value any) bool {
		if value == nil {
			return true
		}
		acct := value.(*types.StateAccount)
		data, err := rlp.EncodeToBytes(acct)
		if err != nil {
			panic(err)
		}
		acctStore.Set(key.(common.Address).Bytes(), data)
		return true
	})

	tr.storageTrieCache.Range(func(address, value any) bool {
		storageTrie := value.(*SlimArchiveTrie)
		storageTrie.storageCache.Range(func(key, value any) bool {
			storageKey := toStorageKey(address.(common.Address), key.(common.Hash).Bytes())
			storageStore.Set(storageKey[:], value.([]byte))
			return true
		})
		return true
	})

	tr.codeCache.Range(func(key, value any) bool {
		codeStore.Set(key.(common.Hash).Bytes(), value.([]byte))
		return true
	})

	return mmdb
}

func CopySTMStorage(old *blockstm.MultiMemDB) *blockstm.MultiMemDB {
	newMmdb := blockstm.NewMultiMemDB(STMStores)
	newAcctStore := newMmdb.GetKVStore(StoreKeyAccount)
	newStorageStore := newMmdb.GetKVStore(StoreKeyStorage)
	newCodeStore := newMmdb.GetKVStore(StoreKeyCode)

	acctStore := old.GetKVStore(StoreKeyAccount)
	storageStore := old.GetKVStore(StoreKeyStorage)
	codeStore := old.GetKVStore(StoreKeyCode)

	var it storetypes.GIterator[[]byte]
	for it = acctStore.Iterator(nil, nil); it.Valid(); it.Next() {
		newAcctStore.Set(it.Key(), it.Value())
	}
	it.Close()

	for it = storageStore.Iterator(nil, nil); it.Valid(); it.Next() {
		newStorageStore.Set(it.Key(), it.Value())
	}
	it.Close()

	for it = codeStore.Iterator(nil, nil); it.Valid(); it.Next() {
		newCodeStore.Set(it.Key(), it.Value())
	}
	it.Close()

	return newMmdb
}
