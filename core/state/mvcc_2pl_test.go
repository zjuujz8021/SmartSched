package state

import (
	"fmt"
	"testing"
	"time"
)

func TestTwoPhaseLock(t *testing.T) {
	lock := NewTwoPhaseLock(10, func(key int, txIndex int) {
		fmt.Println("tx abort", txIndex, "for key", key)
	})
	cancelled := func() bool {
		return false
	}
	lock.AcquireReadLock(1, 0, cancelled)
	lock.AcquireReadLock(2, 0, cancelled)
	lock.AcquireReadLock(4, 5, cancelled)
	fmt.Println(lock.Stats())
	lock.AcquireWriteLock(4, 3, cancelled)
	fmt.Println(lock.Stats())
	go func() {
		lock.AcquireReadLock(4, 6, cancelled)
	}()
	time.Sleep(1 * time.Second)
	fmt.Println(lock.Stats())
	fmt.Println("release 3")
	lock.ReleaseLocks(3)
	time.Sleep(1 * time.Second)
	fmt.Println(lock.Stats())
}
