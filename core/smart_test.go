package core

import "testing"

func BenchmarkTxQueue(b *testing.B) {
	size := 500
	q := TxQueue{}
	for i := 0; i < size; i++ {
		q.Push(i)
	}
	b.ResetTimer()
	sizePerOp := 1
	txs := make([]int, sizePerOp)
	for i := 0; i < b.N; i++ {
		q.PushPop(txs, sizePerOp)
		// q.Push(1)
		// q.PopN(1)
	}
}
