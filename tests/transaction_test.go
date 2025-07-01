// Copyright 2015 The go-ethereum Authors
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

package tests

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	_ "unsafe"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

func TestTransaction(t *testing.T) {
	t.Parallel()

	txt := new(testMatcher)
	// These can't be parsed, invalid hex in RLP
	txt.skipLoad("^ttWrongRLP/.*")
	// We don't allow more than uint64 in gas amount
	// This is a pseudo-consensus vulnerability, but not in practice
	// because of the gas limit
	txt.skipLoad("^ttGasLimit/TransactionWithGasLimitxPriceOverflow.json")
	// We _do_ allow more than uint64 in gas price, as opposed to the tests
	// This is also not a concern, as long as tx.Cost() uses big.Int for
	// calculating the final cozt
	txt.skipLoad(".*TransactionWithGasPriceOverflow.*")

	// The nonce is too large for uint64. Not a concern, it means geth won't
	// accept transactions at a certain point in the distant future
	txt.skipLoad("^ttNonce/TransactionWithHighNonce256.json")

	// The value is larger than uint64, which according to the test is invalid.
	// Geth accepts it, which is not a consensus issue since we use big.Int's
	// internally to calculate the cost
	txt.skipLoad("^ttValue/TransactionWithHighValueOverflow.json")
	txt.walk(t, transactionTestDir, func(t *testing.T, name string, test *TransactionTest) {
		cfg := params.MainnetChainConfig
		if err := txt.checkFailure(t, test.Run(cfg)); err != nil {
			t.Error(err)
		}
	})
}

func TestWg(t *testing.T) {
	cnt := 1000
	var diff time.Duration
	for i := 0; i < cnt; i++ {
		var signal time.Time
		var wake time.Time
		wg := new(sync.WaitGroup)
		wg.Add(1)

		go func() {
			time.Sleep(1 * time.Millisecond)
			signal = time.Now()
			wg.Done()
		}()
		wg.Wait()
		wake = time.Now()
		diff += wake.Sub(signal)
	}
	fmt.Println("Average time to wake up:", diff/time.Duration(cnt))
}
func TestCond(t *testing.T) {
	cnt := 1000
	var diff time.Duration
	for i := 0; i < cnt; i++ {
		var signal time.Time
		var wake time.Time
		cond := sync.NewCond(new(sync.Mutex))
		go func() {
			time.Sleep(1 * time.Millisecond)
			signal = time.Now()
			cond.Signal()
		}()
		cond.L.Lock()
		cond.Wait()
		cond.L.Unlock()
		wake = time.Now()
		diff += wake.Sub(signal)
	}
	fmt.Println("Average time to wake up:", diff/time.Duration(cnt))
}

func BenchmarkWgAdd(b *testing.B) {
	wg := new(sync.WaitGroup)
	for i := 0; i < b.N; i++ {
		wg.Add(1)
	}
}

func BenchmarkWgDone(b *testing.B) {
	wg := new(sync.WaitGroup)
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		wg.Done()
		wg.Wait()
	}
}

func BenchmarkCondSignal(b *testing.B) {
	cond := sync.NewCond(new(sync.Mutex))
	for i := 0; i < b.N; i++ {
		cond.Signal()
	}
}

func BenchmarkCondWait(b *testing.B) {
	cond := sync.NewCond(new(sync.Mutex))
	go func() {
		for {
			cond.Signal()
		}
	}()
	for i := 0; i < b.N; i++ {
		cond.L.Lock()
		cond.Wait()
		cond.L.Unlock()
	}
}

func BenchmarkAddrMap(b *testing.B) {
	m := make(map[common.Address]int)
	for i := 0; i < b.N; i++ {
		m[common.Address{byte(i)}] = i
	}
}

func BenchmarkHashMap(b *testing.B) {
	m := make(map[common.Hash]int)
	for i := 0; i < b.N; i++ {
		m[common.Hash{byte(i)}] = i
	}
}

func lockfn(mu *sync.Mutex) {
	mu.Lock()
	mu.Unlock()
}

func BenchmarkDeferMu(b *testing.B) {
	mu := new(sync.Mutex)
	for i := 0; i < b.N; i++ {
		// lockfn(mu)
		mu.Lock()
		mu.Unlock()
	}
}

func BenchmarkSlice(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = make([]int, 0, 1000)
	}
}

//go:linkname procyield runtime.procyield
func procyield(cycles uint32)

func TestCursorCreate(t *testing.T) {
	var cursor atomic.Uint32
	size := 500
	emitInterval := 1 * time.Millisecond
	var latency time.Duration
	var createlatency time.Duration
	var setTime time.Time
	var cnt int
	go func() {
		// runtime.LockOSThread()
		var curCursor uint32
		var lastCursor uint32
		for {
			curCursor = cursor.Load()
			if curCursor != lastCursor {
				cnt += 1
				latency += time.Since(setTime)
				lastCursor = curCursor
				createTime := time.Now()
				go func() {
					createlatency += time.Since(createTime)
				}()
			}
			procyield(1)
		}
	}()
	for i := 0; i < size; i++ {
		time.Sleep(emitInterval)
		go func() {
			setTime = time.Now()
			cursor.Add(1)
		}()
	}
	time.Sleep(emitInterval)
	t.Error(latency, cnt, createlatency)
}

func TestCursorSignal(t *testing.T) {
	var cursor atomic.Uint32
	size := 500
	emitInterval := 1 * time.Millisecond
	var triggerLatency time.Duration
	var signalLatency time.Duration
	var triggerTime time.Time
	var signalTime time.Time
	var cnt int
	wgs := make([]*sync.WaitGroup, size)
	for i := 0; i < size; i++ {
		wgs[i] = new(sync.WaitGroup)
		wgs[i].Add(1)
		go func(j int) {
			wgs[j].Wait()
			signalLatency += time.Since(signalTime)
		}(i)
	}
	time.Sleep(1 * time.Second)
	go func() {
		// runtime.LockOSThread()
		var curCursor uint32
		var lastCursor uint32
		for {
			curCursor = cursor.Load()
			if curCursor != lastCursor {
				triggerLatency += time.Since(triggerTime)
				lastCursor = curCursor
				signalTime = time.Now()
				wgs[cnt].Done()
				cnt += 1
			}
			procyield(1)
		}
	}()
	for i := 0; i < size; i++ {
		time.Sleep(emitInterval)
		triggerTime = time.Now()
		cursor.Add(1)
	}
	time.Sleep(emitInterval)
	t.Error(triggerLatency, cnt, signalLatency)
}

func TestChCreate(t *testing.T) {
	ch := make(chan struct{}, 1024)
	size := 500
	emitInterval := 1 * time.Millisecond
	var latency time.Duration
	var setTime time.Time
	var cnt int
	var createlatency time.Duration
	go func() {
		for range ch {
			cnt += 1
			latency += time.Since(setTime)
			createTime := time.Now()
			go func() {
				createlatency += time.Since(createTime)
			}()
		}
	}()
	for i := 0; i < size; i++ {
		time.Sleep(emitInterval)
		setTime = time.Now()
		ch <- struct{}{}
	}
	time.Sleep(emitInterval)
	t.Error(latency, cnt, createlatency)
}

func TestWgSinal(t *testing.T) {
	size := 500
	emitInterval := 1 * time.Millisecond
	var latency time.Duration
	var signalTime time.Time
	wgs := make([]*sync.WaitGroup, size)
	for i := 0; i < size; i++ {
		wgs[i] = new(sync.WaitGroup)
		wgs[i].Add(1)
		go func(j int) {
			wgs[j].Wait()
			latency += time.Since(signalTime)
		}(i)
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < size; i++ {
		time.Sleep(emitInterval)
		signalTime = time.Now()
		wgs[i].Done()
	}
	time.Sleep(emitInterval)
	t.Error(latency)
}

func execKeccak256(size int) {
	for i := 0; i < size; i++ {
		crypto.Keccak256([]byte{byte(i)})
	}
}

func TestKeccak256(t *testing.T) {
	size := 10
	start := time.Now()
	execKeccak256(size)
	t.Error(time.Since(start))
}

func TestCursorExec(t *testing.T) {
	var cursor atomic.Uint32
	size := 500
	emitInterval := 1 * time.Millisecond
	var triggerLatency time.Duration
	var triggerTime time.Time
	var cnt int
	time.Sleep(1 * time.Second)
	go func() {
		// runtime.LockOSThread()
		var curCursor uint32
		var lastCursor uint32
		for {
			curCursor = cursor.Load()
			if curCursor != lastCursor {
				triggerLatency += time.Since(triggerTime)
				lastCursor = curCursor
				execKeccak256(10)
				cnt += 1
			}
			procyield(1)
		}
	}()
	for i := 0; i < size; i++ {
		time.Sleep(emitInterval)
		go func() {
			triggerTime = time.Now()
			cursor.Add(1)
		}()
	}
	time.Sleep(emitInterval)
	t.Error(triggerLatency, cnt)
}

func TestChClose(t *testing.T) {
	ch := make(chan struct{}, 10)
	close(ch)
	select {
	case ch <- struct{}{}:
		t.Error("ch is closed")
	default:
		t.Error("ch is not closed")
	}
}
