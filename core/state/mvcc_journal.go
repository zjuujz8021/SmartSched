// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

// journalEntry is a modification entry in the state change journal that can be
// reverted on demand.
type mvccJournalEntry interface {
	// revert undoes the changes introduced by this journal entry.
	revert(*MVCCStateDB)

	// dirtied returns the Ethereum address modified by this journal entry.
	dirtied() *common.Address
}

// journal contains the list of state modifications applied since the last state
// commit. These are tracked to be able to be reverted in the case of an execution
// exception or request for reversal.
type mvccJournal struct {
	entries []mvccJournalEntry     // Current changes tracked by the journal
	dirties map[common.Address]int // Dirty accounts and the number of changes
}

// newJournal creates a new initialized journal.
func newMvccJournal() *mvccJournal {
	return &mvccJournal{
		dirties: make(map[common.Address]int),
	}
}

// append inserts a new modification entry to the end of the change journal.
func (j *mvccJournal) append(entry mvccJournalEntry) {
	j.entries = append(j.entries, entry)
	if addr := entry.dirtied(); addr != nil {
		j.dirties[*addr]++
	}
}

// revert undoes a batch of journalled modifications along with any reverted
// dirty handling too.
func (j *mvccJournal) revert(statedb *MVCCStateDB, snapshot int) {
	for i := len(j.entries) - 1; i >= snapshot; i-- {
		// Undo the changes made by the operation
		j.entries[i].revert(statedb)

		// Drop any dirty tracking induced by the change
		if addr := j.entries[i].dirtied(); addr != nil {
			if j.dirties[*addr]--; j.dirties[*addr] == 0 {
				delete(j.dirties, *addr)
			}
		}
	}
	j.entries = j.entries[:snapshot]
}

// dirty explicitly sets an address to dirty, even if the change entries would
// otherwise suggest it as clean. This method is an ugly hack to handle the RIPEMD
// precompile consensus exception.
func (j *mvccJournal) dirty(addr common.Address) {
	j.dirties[addr]++
}

// length returns the current number of entries in the journal.
func (j *mvccJournal) length() int {
	return len(j.entries)
}

type (
	// Changes to the account trie.
	mvccCreateObjectChange struct {
		account *common.Address
	}
	mvccResetObjectChange struct {
		account *common.Address
		prev    *MVCCStateObject
		// prevdestruct bool
		// prevAccount  []byte
		// prevStorage  map[common.Hash][]byte

		// prevAccountOriginExist bool
		// prevAccountOrigin      []byte
		// prevStorageOrigin      map[common.Hash][]byte
	}
	mvccSelfDestructChange struct {
		account     *common.Address
		prev        bool // whether account had already self-destructed
		prevbalance *uint256.Int
	}

	mvccCommutativeSelfDestructChange struct {
		account          *common.Address
		prev             bool // whether account had already self-destructed
		prevbalancedelta *big.Int
	}

	// Changes to individual accounts.
	mvccBalanceChange struct {
		account *common.Address
		prev    *uint256.Int
	}
	mvccCommutativeBalanceChange struct {
		account *common.Address
		prev    *big.Int
	}
	mvccNonceChange struct {
		account *common.Address
		prev    uint64
	}
	mvccStorageChange struct {
		account       *common.Address
		key, prevalue common.Hash
	}
	mvccCodeChange struct {
		account            *common.Address
		prevcode, prevhash []byte
	}

	// Changes to other state values.
	mvccRefundChange struct {
		prev uint64
	}
	mvccAddLogChange struct {
		txhash common.Hash
	}
	// mvccAddPreimageChange struct {
	// 	hash common.Hash
	// }
	mvccTouchChange struct {
		account *common.Address
	}
	// Changes to the access list
	mvccAccessListAddAccountChange struct {
		address *common.Address
	}
	mvccAccessListAddSlotChange struct {
		address *common.Address
		slot    *common.Hash
	}

	mvccTransientStorageChange struct {
		account       *common.Address
		key, prevalue common.Hash
	}
)

func (ch mvccCreateObjectChange) revert(s *MVCCStateDB) {
	s.danglingObjects = append(s.danglingObjects, s.mvccStateObjects[*ch.account])
	delete(s.mvccStateObjects, *ch.account)
	// delete(s.stateObjectsDirty, *ch.account)
}

func (ch mvccCreateObjectChange) dirtied() *common.Address {
	return ch.account
}

func (ch mvccResetObjectChange) revert(s *MVCCStateDB) {
	s.danglingObjects = append(s.danglingObjects, s.mvccStateObjects[*ch.account])
	s.mvccStateObjects[ch.prev.baseObj.address] = ch.prev

	// TODO: is this necessary?
	// if !ch.prev.created {
	// delete(s.replacedStateObjects, ch.prev.address)
	// }

	// s.setStateObject(ch.prev)
	// if !ch.prevdestruct {
	// 	delete(s.stateObjectsDestruct, ch.prev.address)
	// }
	// if ch.prevAccount != nil {
	// 	s.accounts[ch.prev.addrHash] = ch.prevAccount
	// }
	// if ch.prevStorage != nil {
	// 	s.storages[ch.prev.addrHash] = ch.prevStorage
	// }
	// if ch.prevAccountOriginExist {
	// 	s.accountsOrigin[ch.prev.address] = ch.prevAccountOrigin
	// }
	// if ch.prevStorageOrigin != nil {
	// 	s.storagesOrigin[ch.prev.address] = ch.prevStorageOrigin
	// }
}

func (ch mvccResetObjectChange) dirtied() *common.Address {
	return ch.account
}

func (ch mvccSelfDestructChange) revert(s *MVCCStateDB) {
	obj := s.getStateObject(*ch.account)
	if obj != nil {
		obj.selfDestructed = ch.prev
		obj.setBalance(ch.prevbalance)
	}
}

func (ch mvccSelfDestructChange) dirtied() *common.Address {
	return ch.account
}

func (ch mvccCommutativeSelfDestructChange) dirtied() *common.Address {
	return ch.account
}

func (ch mvccCommutativeSelfDestructChange) revert(s *MVCCStateDB) {
	obj := s.getStateObject(*ch.account)
	if obj != nil {
		obj.selfDestructed = ch.prev
		obj.setBalanceDelta(ch.prevbalancedelta)
	}
}

// var ripemd = common.HexToAddress("0000000000000000000000000000000000000003")

func (ch mvccTouchChange) revert(s *MVCCStateDB) {
}

func (ch mvccTouchChange) dirtied() *common.Address {
	return ch.account
}

func (ch mvccBalanceChange) revert(s *MVCCStateDB) {
	s.getStateObject(*ch.account).setBalance(ch.prev)
}

func (ch mvccBalanceChange) dirtied() *common.Address {
	return ch.account
}

func (ch mvccCommutativeBalanceChange) revert(s *MVCCStateDB) {
	s.getStateObject(*ch.account).setBalanceDelta(ch.prev)
}

func (ch mvccCommutativeBalanceChange) dirtied() *common.Address {
	return ch.account
}

func (ch mvccNonceChange) revert(s *MVCCStateDB) {
	s.getStateObject(*ch.account).setNonce(ch.prev)
}

func (ch mvccNonceChange) dirtied() *common.Address {
	return ch.account
}

func (ch mvccCodeChange) revert(s *MVCCStateDB) {
	s.getStateObject(*ch.account).setCode(common.BytesToHash(ch.prevhash), ch.prevcode)
}

func (ch mvccCodeChange) dirtied() *common.Address {
	return ch.account
}

func (ch mvccStorageChange) revert(s *MVCCStateDB) {
	s.getStateObject(*ch.account).setState(ch.key, ch.prevalue)
}

func (ch mvccStorageChange) dirtied() *common.Address {
	return ch.account
}

func (ch mvccTransientStorageChange) revert(s *MVCCStateDB) {
	s.setTransientState(*ch.account, ch.key, ch.prevalue)
}

func (ch mvccTransientStorageChange) dirtied() *common.Address {
	return nil
}

func (ch mvccRefundChange) revert(s *MVCCStateDB) {
	s.refund = ch.prev
}

func (ch mvccRefundChange) dirtied() *common.Address {
	return nil
}

func (ch mvccAddLogChange) revert(s *MVCCStateDB) {
	s.logs = s.logs[:len(s.logs)-1]
}

func (ch mvccAddLogChange) dirtied() *common.Address {
	return nil
}

// func (ch mvccAddPreimageChange) revert(s *MVCCStateDB) {
// 	// delete(s.preimages, ch.hash)
// }

// func (ch mvccAddPreimageChange) dirtied() *common.Address {
// 	return nil
// }

func (ch mvccAccessListAddAccountChange) revert(s *MVCCStateDB) {
	/*
		One important invariant here, is that whenever a (addr, slot) is added, if the
		addr is not already present, the add causes two journal entries:
		- one for the address,
		- one for the (address,slot)
		Therefore, when unrolling the change, we can always blindly delete the
		(addr) at this point, since no storage adds can remain when come upon
		a single (addr) change.
	*/
	s.accessList.DeleteAddress(*ch.address)
}

func (ch mvccAccessListAddAccountChange) dirtied() *common.Address {
	return nil
}

func (ch mvccAccessListAddSlotChange) revert(s *MVCCStateDB) {
	s.accessList.DeleteSlot(*ch.address, *ch.slot)
}

func (ch mvccAccessListAddSlotChange) dirtied() *common.Address {
	return nil
}
