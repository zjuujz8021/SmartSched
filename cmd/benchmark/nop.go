package main

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/cmd/benchmark/nop"
	"github.com/ethereum/go-ethereum/core"
	"github.com/schollz/progressbar/v3"
)

func BenchmarkNop(config *Config) error {
	fmt.Println("init env")
	env := nop.CreateNopEnv(config.Accounts)
	fmt.Println("init msgs")
	msgs := make([]*core.Message, config.TxsPerBlock)
	for j := 0; j < config.TxsPerBlock; j++ {
		msgs[j] = nop.CreateNopMsg(env.ContractAddr, env.Accounts[j%config.Accounts])
	}
	nopResults := [6]Result{}
	for i := range nopResults {
		nopResults[i].Stats = &core.ProcessStats{}
	}
	fmt.Println("run benchmark")
	bar := progressbar.Default(int64(config.Rounds))
	for i := 0; i < config.Rounds; i++ {
		bar.Add(1)
		results := run(env.StateEnv, msgs, core.ParallelProcessors,
			core.ParallelConfig{WorkerNum: config.Threads, UseCommutative: true, DisableNonceValidate: true})
		for j := 0; j < 6; j++ {
			nopResults[j].GasUsed += results[j].GasUsed
			nopResults[j].Stats.Add(results[j].Stats)
		}
	}

	fmt.Println("nop results")
	for _, r := range nopResults {
		fmt.Println("process", r.Stats.Process/time.Duration(config.Rounds),
			"aborted", r.Stats.Aborted/int(config.Rounds),
			"validate", r.Stats.Validate/time.Duration(config.Rounds),
			"finalize", r.Stats.Finalize/time.Duration(config.Rounds),
			"workers", r.Stats.AvgRunningWorker/float64(config.Rounds),
			"critical path", r.Stats.StaticCriticalPathLen/int(config.Rounds),
			r.Stats.DynamicCriticalPathLen/int(config.Rounds),
			"gas/tx", r.GasUsed/uint64(config.TxsPerBlock)/uint64(config.Rounds),
			"tx exec time", r.Stats.TxProcessTime/time.Duration(config.Rounds),
			"tx exec cnt", r.Stats.TxProcessCnt/int(config.Rounds),
		)
	}
	return nil
}
