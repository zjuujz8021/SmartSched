package state

import (
	"fmt"
	"sync"
)

type LockType int

const (
	LockRead LockType = 1 << iota
	LockWrite
)

func (l LockType) AddRead() LockType {
	return l | LockRead
}

func (l LockType) AddWrite() LockType {
	return l | LockWrite
}

func (l LockType) HasRead() bool {
	return l&LockRead != 0
}

func (l LockType) HasWrite() bool {
	return l&LockWrite != 0
}

type Waiting struct {
	Index int
	Cond  *Condvar
}

type TwoPhaseLock[T comparable] struct {
	mu         sync.Mutex
	lockedTxs  map[T]map[int]LockType
	lockedKeys [][]T
	waiting    [][]Waiting
	onConflict func(key T, txIndex int)
}

func NewTwoPhaseLock[T comparable](txs int, onConflict func(key T, txIndex int)) *TwoPhaseLock[T] {
	return &TwoPhaseLock[T]{
		lockedTxs:  make(map[T]map[int]LockType),
		lockedKeys: make([][]T, txs),
		waiting:    make([][]Waiting, txs),
		onConflict: onConflict,
	}
}

// func newBTree() *btree.BTreeG[LockEntry] {
// 	return btree.NewBTreeGOptions(func(a, b LockEntry) bool {
// 		return a.Index < b.Index
// 	}, btree.Options{
// 		NoLocks: true,
// 		Degree:  4,
// 	})
// }

func (l *TwoPhaseLock[T]) releaseLocks(txIndex int) {
	for _, key := range l.lockedKeys[txIndex] {
		delete(l.lockedTxs[key], txIndex)
	}
	for _, dep := range l.waiting[txIndex] {
		dep.Cond.Signal()
	}
	l.lockedKeys[txIndex] = l.lockedKeys[txIndex][:0]
	l.waiting[txIndex] = l.waiting[txIndex][:0]
}

func (l *TwoPhaseLock[T]) ReleaseLocks(txIndex int) {
	l.mu.Lock()
	l.releaseLocks(txIndex)
	l.mu.Unlock()
}

func (l *TwoPhaseLock[T]) getLockSet(key T) map[int]LockType {
	var lockSet map[int]LockType
	var ok bool
	lockSet, ok = l.lockedTxs[key]
	if !ok {
		lockSet = make(map[int]LockType)
		l.lockedTxs[key] = lockSet
	}
	return lockSet
}

func (l *TwoPhaseLock[T]) acquireReadLock(key T, txIndex int, cancelled func() bool) *Condvar {
	l.mu.Lock()
	defer l.mu.Unlock()
	if cancelled() {
		return nil
	}
	lockSet := l.getLockSet(key)
	old, exist := lockSet[txIndex]
	if exist && old.HasRead() {
		return nil
	}
	waitFor := -1
	for tx, lock := range lockSet {
		if tx >= txIndex {
			continue
		}
		if lock.HasWrite() {
			// wr is not allowed, wait for write lock to be released
			waitFor = tx
		}
	}
	if waitFor >= 0 {
		cond := NewCondvar()
		l.waiting[waitFor] = append(l.waiting[waitFor], Waiting{Index: txIndex, Cond: cond})
		return cond
	}
	lockSet[txIndex] = old.AddRead()
	l.lockedKeys[txIndex] = append(l.lockedKeys[txIndex], key)
	return nil
}

func (l *TwoPhaseLock[T]) AcquireReadLock(key T, txIndex int, cancelled func() bool) {
	cond := l.acquireReadLock(key, txIndex, cancelled)
	for cond != nil {
		cond.Wait()
		cond = l.acquireReadLock(key, txIndex, cancelled)
	}
}

func (l *TwoPhaseLock[T]) AcquireWriteLock(key T, txIndex int, cancelled func() bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if cancelled() {
		return
	}
	lockSet := l.getLockSet(key)
	old, exist := lockSet[txIndex]
	if exist && old.HasWrite() {
		return
	}
	conflicts := make([]int, 0)
	for tx, lock := range lockSet {
		if tx <= txIndex {
			// write does not conflict with prev tx
			continue
		}
		if lock.HasRead() {
			// wr is not detected, abort the succeeding tx
			conflicts = append(conflicts, tx)
		}
	}
	for _, conflict := range conflicts {
		// notify the conflict tx to abort & release the lock
		l.onConflict(key, conflict)
		l.releaseLocks(conflict)
	}
	lockSet[txIndex] = old.AddWrite()
	l.lockedKeys[txIndex] = append(l.lockedKeys[txIndex], key)
}

func (l *TwoPhaseLock[T]) Stats() string {
	return fmt.Sprintf("lockedTxs: %v, lockedKeys: %v, waiting: %+v", l.lockedTxs, l.lockedKeys, l.waiting)
}
