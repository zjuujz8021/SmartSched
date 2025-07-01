package main

import (
	"fmt"
	"math/big"

	// _ "net/http/pprof"
	"runtime"
	"time"

	"github.com/ethereum/go-ethereum/cmd/benchmark/stateinit"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"github.com/urfave/cli/v2"
)

var (
	AccountsFlag = &cli.IntFlag{
		Name:  "accounts",
		Value: 100,
	}
	TxsPerBlockFlag = &cli.IntFlag{
		Name:  "txs.per.block",
		Value: 100,
	}
	RoundsFlag = &cli.IntFlag{
		Name:  "rounds",
		Value: 100,
	}
	TypeFlag = &cli.StringFlag{
		Name: "type",
	}
)

type Config struct {
	Accounts        int
	TxsPerBlock     int
	Threads         int
	Rounds          int
	Type            string
	ExecStatsFile   string
	OnlySmartScheds bool
	AllSchemes      bool
}

func Benchmark(ctx *cli.Context) error {
	core.SenderCacher.Stop()
	// go func() {
	// 	http.ListenAndServe("0.0.0.0:16060", nil)
	// }()
	// runtime.SetBlockProfileRate(1)     // 开启对阻塞操作的跟踪，block
	// runtime.SetMutexProfileFraction(1) // 开启对锁调用的跟踪，mutex
	// fmt.Printf("Current Num of CPUs: %d\n", runtime.NumCPU())
	config := &Config{
		Accounts:        ctx.Int(AccountsFlag.Name),
		TxsPerBlock:     ctx.Int(TxsPerBlockFlag.Name),
		Threads:         ctx.Int(utils.ThreadsFlag.Name),
		Rounds:          ctx.Int(RoundsFlag.Name),
		Type:            ctx.String(TypeFlag.Name),
		ExecStatsFile:   ctx.String(utils.ExecStatsFileFlag.Name),
		OnlySmartScheds: ctx.Bool(utils.OnlySmartSchedsFlag.Name),
		AllSchemes:      ctx.Bool(utils.AllSchemesFlag.Name),
	}

	// fmt.Printf("%+v\n", config)

	// log.Info("benchmark", "type", config.Type)

	switch config.Type {
	case "erc20":
		return BenchmarkERC20(config)
	case "nop":
		return BenchmarkNop(config)
	default:
		return fmt.Errorf("unknown type: %s", config.Type)
	}
}

type Result struct {
	Name    string
	GasUsed uint64
	Result  []*core.ExecutionResult
	Stats   *core.ProcessStats
}

const (
	defaultGasLimit      = 1000000
	defaultBlockGasLimit = 1000000000000
)

func newMockHeader() *types.Header {
	header := &types.Header{
		Number:     big.NewInt(20000000),
		Difficulty: big.NewInt(0),
		GasLimit:   defaultBlockGasLimit,
		BaseFee:    big.NewInt(0),
		Time:       uint64(time.Now().Unix()),
	}
	return header
}

func run(env *stateinit.StateEnv, msgs []*core.Message, processSchemes []string, parallelConfig core.ParallelConfig) []Result {
	header := newMockHeader()
	_, _, dbg, _, err := core.NewOccProcessor(params.MainnetChainConfig, nil,
		core.ParallelConfig{WorkerNum: 1, UseCommutative: true, TrackRWSet: true, DisableNonceValidate: true}).ExecuteMsgs(
		header, vm.Config{NoBaseFee: true}, &core.ParallelInput{Msgs: msgs, OCCStateDB: env.OCCStateDB.DeepCopy()},
	)
	if err != nil {
		panic(fmt.Errorf("failed to load rwsets: %v", err))
	}

	ret := make([]Result, 0)

	runtime.GC()
	results, stats, gasUsed, err := core.NewReplayProcessor(params.MainnetChainConfig, nil, false).ExecuteMsgs(msgs, header, vm.Config{NoBaseFee: true}, env.SerialStateDB)
	if err != nil {
		panic(err)
	}
	for i := 0; i < len(msgs); i++ {
		if results[i].Err != nil {
			panic(results[i].Err)
		}
	}
	ret = append(ret, Result{
		Name:    "serial",
		GasUsed: gasUsed,
		Result:  results,
		Stats:   stats,
	})

	input := &core.ParallelInput{
		Msgs:                           msgs,
		RWSets:                         dbg.RWSets,
		OCCStateDB:                     env.OCCStateDB,
		TwoPLStateDB:                   env.TwoPLStateDB,
		STMStateDB:                     env.STMStateDB,
		SpectrumStateDB:                env.SpectrumStateDB,
		DmvccStateDB:                   env.DmvccStateDB,
		ParEVMStateDB:                  env.ParEVMStateDB,
		SmartSchedStateDB:              env.SmartSchedStateDB,
		SmartSchedOnlyPreCommitStateDB: env.SmartSchedOnlyPreCommitStateDB,
		SmartSchedOnlySchedStateDB:     env.SmartSchedOnlySchedStateDB,
	}

	processors := core.CreateParallelProcessors(params.MainnetChainConfig, nil, processSchemes, parallelConfig)
	// parallel
	for _, p := range processors {
		runtime.GC()
		results, stats, _, gasUsed, err := p.ExecuteMsgs(header, vm.Config{}, input)
		if err != nil {
			panic(err)
		}
		for i := 0; i < len(msgs); i++ {
			if results[i].Err != nil {
				panic(results[i].Err)
			}
		}
		ret = append(ret, Result{
			Name:    p.Name(),
			GasUsed: gasUsed,
			Result:  results,
			Stats:   stats,
		})
	}
	return ret
}
