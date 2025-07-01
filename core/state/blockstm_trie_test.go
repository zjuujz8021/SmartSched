package state

import (
	"fmt"
	"testing"

	block_stm "github.com/crypto-org-chain/go-block-stm"
	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

func TestBlockSTMTrie(t *testing.T) {
	storage := block_stm.NewMultiMemDB(STMStores)
	scheduler := block_stm.NewScheduler(16)
	mv := block_stm.NewMVMemory(16, STMStores, storage, scheduler)

	addr1 := common.HexToAddress("0x1")

	view0 := mv.View(0)
	stmtrie0 := NewBlockSTMTrie(view0)
	sdb0, _ := New(common.Hash{}, stmtrie0, nil)
	fmt.Println(sdb0.GetBalance(addr1))
	sdb0.SetBalance(addr1, uint256.MustFromDecimal("1000000000000000000"))
	sdb0.Finalise(true)

	view1 := mv.View(1)
	stmtrie1 := NewBlockSTMTrie(view1)
	sdb1, _ := New(common.Hash{}, stmtrie1, nil)
	fmt.Println(sdb1.GetBalance(addr1))
	fmt.Println(mv.Record(block_stm.TxnVersion{0, 0}, view0))
	fmt.Println(sdb1.GetBalance(addr1))

	mv2 := block_stm.NewMVMemory(16, STMStores, storage, scheduler)
	view2 := mv2.View(1)
	stmtrie2 := NewBlockSTMTrie(view2)
	sdb2, _ := New(common.Hash{}, stmtrie2, nil)
	fmt.Println(sdb2.GetBalance(addr1))

	mv.WriteSnapshot(storage)
	fmt.Println(sdb2.GetBalance(addr1))
}

func TestSTMIterator(t *testing.T) {
	storage := block_stm.NewMultiMemDB(STMStores)
	kv := storage.GetKVStore(StoreKeyStorage)
	kv.Set([]byte{0}, []byte{0})
	kv.Set([]byte{1}, []byte{1})
	kv.Set([]byte{2}, []byte{10})
	kv.Set([]byte{3}, []byte{3})

	// fmt.Println(storage.GetKVStore(StoreKeyStorage).Get([]byte{1}))

	for it := storage.GetKVStore(StoreKeyStorage).Iterator(nil, nil); it.Valid(); it.Next() {
		fmt.Println(it.Key(), it.Value())
	}

}
