package main

import (
	"sync/atomic"
	"testing"
	_ "unsafe"
)

func TestERC20(t *testing.T) {
	BenchmarkERC20(&Config{
		Accounts:    20,
		TxsPerBlock: 100,
		Threads:     8,
		Rounds:      100,
	})
}

//go:linkname procyield runtime.procyield
func procyield(cycles uint32)

func BenchmarkPause(b *testing.B) {
	for i := 0; i < b.N; i++ {
		procyield(10)
	}
}

func BenchmarkAtomicAdd(b *testing.B) {
	var a atomic.Int32
	for i := 0; i < b.N; i++ {
		a.Load()
	}
}

func BenchmarkChan(b *testing.B) {
	ch := make(chan struct{}, 1000)
	go func() {
		for range ch {
		}
	}()
	for i := 0; i < b.N; i++ {
		ch <- struct{}{}
	}
}

func TestNop(t *testing.T) {
	BenchmarkNop(&Config{
		Accounts:    20,
		TxsPerBlock: 100,
		Threads:     16,
		Rounds:      10,
	})
}
