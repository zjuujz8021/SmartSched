package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	// _ "net/http/pprof"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/tracers/logger"
	"github.com/ethereum/go-ethereum/log"
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli/v2"
)

var (
	ReplayStartFlag = &cli.IntFlag{
		Name: "replay.start",
	}
	ReplayEndFlag = &cli.IntFlag{
		Name: "replay.end",
	}
	IntervalFlag = &cli.IntFlag{
		Name:  "interval",
		Value: 100,
	}
	ConcurrencyFlag = &cli.IntFlag{
		Name:  "concurrency",
		Value: 1,
	}

	UseTxLevelStateFlag = &cli.BoolFlag{
		Name: "use.tx.level.state",
	}
	UseOCCFlag = &cli.BoolFlag{
		Name: "use.occ",
	}
	UseSmartSchedFlag = &cli.BoolFlag{
		Name: "use.smart.sched",
	}
	EnableTraceFlag = &cli.BoolFlag{
		Name: "enable.trace",
	}
	OmitFailureFlag = &cli.BoolFlag{
		Name: "omit.failure",
	}
	EnableCommutativeFlag = &cli.BoolFlag{
		Name: "enable.commutative",
	}
	UseGTRWSetsFlag = &cli.BoolFlag{
		Name: "use.gt.rwsets",
	}
	CompareFlag = &cli.BoolFlag{
		Name: "compare",
	}

	// flags for trace tx
	UseMVCCFlag = &cli.BoolFlag{
		Name: "use.mvcc",
	}
	TxHashFlag = &cli.StringFlag{
		Name: "tx.hash",
	}
)

type ReplayConfig struct {
	Start, End, Interval int
	Concurrency          int
	ThreadCount          int
	UseTxLevelState      bool
	UseOCC               bool
	UseSmartSched        bool
	EnableTrace          bool
	OmitFailure          bool
	EnableCommutative    bool
	UseGTRWSets          bool
	Compare              bool
	ExecStatsFile        string
}

type BlockTaskResult struct {
	Err          error
	Cost         time.Duration
	Block        *types.Block
	UsedGas      uint64
	ProcessStats *core.ProcessStats
	TxResults    []*core.ExecutionResult

	// Used for OCC
	DebugInfo *core.DebugInfo
}

type CompBlockTaskResultSingle struct {
	Name         string
	UsedGas      uint64
	ProcessStats *core.ProcessStats
	TxResults    []*core.ExecutionResult
}

type CompBlockTaskResult struct {
	Err     error
	Cost    time.Duration
	Block   *types.Block
	Results []*CompBlockTaskResultSingle
}

func ReplayBlocks(ctx *cli.Context) error {
	// go func() {
	// 	http.ListenAndServe("0.0.0.0:16060", nil)
	// }()
	// runtime.SetBlockProfileRate(1)     // 开启对阻塞操作的跟踪，block
	// runtime.SetMutexProfileFraction(1) // 开启对锁调用的跟踪，mutex
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	chain, db := utils.MakeChain(ctx, stack, false)
	defer db.Close()
	defer chain.Stop()

	config := &ReplayConfig{
		Start:             ctx.Int(ReplayStartFlag.Name),
		End:               ctx.Int(ReplayEndFlag.Name),
		Interval:          ctx.Int(IntervalFlag.Name),
		Concurrency:       ctx.Int(ConcurrencyFlag.Name),
		ThreadCount:       ctx.Int(utils.ThreadsFlag.Name),
		UseTxLevelState:   ctx.Bool(UseTxLevelStateFlag.Name),
		UseOCC:            ctx.Bool(UseOCCFlag.Name),
		UseSmartSched:     ctx.Bool(UseSmartSchedFlag.Name),
		EnableTrace:       ctx.Bool(EnableTraceFlag.Name),
		OmitFailure:       ctx.Bool(OmitFailureFlag.Name),
		UseGTRWSets:       ctx.Bool(UseGTRWSetsFlag.Name),
		EnableCommutative: ctx.Bool(EnableCommutativeFlag.Name),
		Compare:           ctx.Bool(CompareFlag.Name),
		ExecStatsFile:     ctx.String(utils.ExecStatsFileFlag.Name),
	}

	confStr, _ := json.Marshal(config)
	fmt.Println("replay config: ", string(confStr))
	var err error
	if config.Compare {
		err = compareReplayBlocks(chain, config)
	} else {
		err = replayBlocks(chain, config)
	}
	if err != nil {
		log.Error("failed to replay blocks", "err", err)
	}
	return err
}

func compareReplayBlocks(bc *core.BlockChain, config *ReplayConfig) error {
	var csvWriter *csv.Writer
	if config.ExecStatsFile != "" {
		csvFile, err := os.OpenFile(config.ExecStatsFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		csvWriter = csv.NewWriter(csvFile)
		defer csvFile.Close()
		defer csvWriter.Flush()
	}

	tasks, err := bc.SampleBlockByMaxTxCount(uint64(config.Start), uint64(config.End), uint64(config.Interval))
	if err != nil {
		return err
	}
	taskCnt := len(tasks)
	taskCh := make(chan *types.Block, len(tasks))
	go func() {
		for _, block := range tasks {
			taskCh <- block
		}
		close(taskCh)
	}()

	// taskCh := make(chan *types.Block, 100000)
	// go bc.SampleBlockByMaxTxCountCh(uint64(config.Start), uint64(config.End), uint64(config.Interval), taskCh)
	// time.Sleep(1 * time.Second)
	// taskCnt := (config.End - config.Start) / config.Interval
	// if (config.End-config.Start)%config.Interval != 0 {
	// 	taskCnt++
	// }
	parallelProcessors := core.ParallelProcessors
	schemes := append([]string{"serial"}, parallelProcessors...)
	resCh := make(chan *CompBlockTaskResult, 100000)

	for i := 0; i < config.Concurrency; i++ {
		go func() {
			for block := range taskCh {
				resCh <- compareReplayBlock(bc, block, config.ThreadCount, parallelProcessors)
			}
		}()
	}

	log.Info("start replay blocks", "task count", taskCnt)
	execStats := make([]core.ProcessStats, len(parallelProcessors)+1)
	failures := make(map[string]int)
	cost := time.Duration(0)
	bar := progressbar.Default(int64(taskCnt))
	for i := 0; i < int(taskCnt); i++ {
		ret := <-resCh
		bar.Add(1)
		if ret.Err != nil {
			return ret.Err
		}
		if len(ret.Results) != len(schemes) {
			return errors.New("unmatched result count")
		}
		for j := 0; j < len(ret.Results); j++ {
			if ret.Results[j].UsedGas != ret.Block.GasUsed() {
				// log.Error("unmatched gas used - compare replay blocks!", "block", ret.Block.NumberU64(), "method", ret.Results[j].Name, "origin", ret.Block.GasUsed(), "replayed", ret.Results[j].UsedGas)
				failures[ret.Results[j].Name]++
			}
			if ret.Results[j].Name != schemes[j] {
				panic("unmatched scheme")
			}
			execStats[j].Add(ret.Results[j].ProcessStats)
		}
		cost += ret.Cost
	}

	log.Info("compare replay blocks done", "avg cost per block", cost/time.Duration(taskCnt))
	for i := 0; i < len(execStats); i++ {
		log.Info("Exec stats", "scheme", schemes[i],
			// "txs", execStats[i].TxCount,
			"block size", float64(execStats[i].TxCount)/float64(taskCnt),
			"aborted", execStats[i].Aborted,
			// "avg static critical path", float64(execStats[i].StaticCriticalPathLen)/float64(taskCnt),
			// "avg dynamic critical path", float64(execStats[i].DynamicCriticalPathLen)/float64(taskCnt),
			// "avg critical path exec", execStats[i].CriticalPathExec/time.Duration(taskCnt),
			"init", execStats[i].Init/time.Duration(taskCnt),
			"process", execStats[i].Process/time.Duration(taskCnt),
			"validate", execStats[i].Validate/time.Duration(taskCnt),
			"finalize", execStats[i].Finalize/time.Duration(taskCnt),
			"failures", failures[schemes[i]],
			// "avg running workers", float64(execStats[i].AvgRunningWorker)/float64(taskCnt),
		)
		if csvWriter != nil {
			// csv line: scheme, threads, accounts, txs, process time (ms).
			csvWriter.Write([]string{
				schemes[i],
				fmt.Sprintf("%d", config.ThreadCount),
				fmt.Sprintf("%d", 0),
				fmt.Sprintf("%d", 0),
				fmt.Sprintf("%.3f", (execStats[i].Process/time.Duration(taskCnt)).Seconds()*1e3),
				fmt.Sprintf("%.3f", float64(execStats[0].Process)/float64(execStats[i].Process)),
			})
		}
	}
	return nil
}

func replayBlocks(bc *core.BlockChain, config *ReplayConfig) error {
	tasks, err := bc.SampleBlockByMaxTxCount(uint64(config.Start), uint64(config.End), uint64(config.Interval))
	if err != nil {
		return err
	}
	taskCnt := len(tasks)
	taskCh := make(chan *types.Block, len(tasks))
	go func() {
		for _, block := range tasks {
			taskCh <- block
		}
		close(taskCh)
	}()

	// taskCh := make(chan *types.Block, 100000)
	// go bc.SampleBlockByMaxTxCountCh(uint64(config.Start), uint64(config.End), uint64(config.Interval), taskCh)
	// time.Sleep(1 * time.Second)
	// taskCnt := (config.End - config.Start) / config.Interval
	// if (config.End-config.Start)%config.Interval != 0 {
	// 	taskCnt++
	// }

	resCh := make(chan *BlockTaskResult, 100000)

	for i := 0; i < config.Concurrency; i++ {
		go func() {
			for block := range taskCh {
				if config.UseOCC {
					resCh <- occReplayBlock(bc, block, config.ThreadCount, config.EnableTrace, config.EnableCommutative)
				} else if config.UseSmartSched {
					resCh <- smartSchedReplayBlock(bc, block, config.ThreadCount, config.EnableTrace, config.EnableCommutative, config.UseGTRWSets, false)
				} else {
					resCh <- serialReplayBlock(bc, block, config.UseTxLevelState)
				}
			}
		}()
	}

	log.Info("start replay blocks", "task count", taskCnt)
	procStats := &core.ProcessStats{}
	failedTxs := 0
	cost := time.Duration(0)
	// totalExecTime := time.Duration(0)
	// totalExecCount := 0
	bar := progressbar.Default(int64(taskCnt))
	for i := 0; i < int(taskCnt); i++ {
		ret := <-resCh
		bar.Add(1)
		if ret.Err != nil {
			return ret.Err
		} else if ret.UsedGas != ret.Block.GasUsed() {
			receipts := bc.GetReceiptsByHash(ret.Block.Hash())
			if len(receipts) != len(ret.TxResults) {
				log.Error("invalid receipts", "recorded", len(ret.TxResults))
			}
			for i := 0; i < min(len(receipts), len(ret.TxResults)); i++ {
				if receipts[i].GasUsed != ret.TxResults[i].UsedGas {
					failedTxs++
					if config.OmitFailure {
						continue
					}
					log.Error("unmatched gas used - replay blocks!", "block", ret.Block.NumberU64(), "index", i,
						"origin", receipts[i].GasUsed, "replayed", ret.TxResults[i].UsedGas,
						"err", ret.TxResults[i].Err, "ret data", hexutil.Encode(ret.TxResults[i].ReturnData))
					if ret.DebugInfo != nil && len(ret.DebugInfo.TraceResults) > 0 {
						if debugFile, err := os.Create(fmt.Sprintf("./tmp/%d-%d-false.json", ret.Block.NumberU64(), i)); err != nil {
							log.Error("create debug file failed")
						} else {
							if _, err = debugFile.Write(ret.DebugInfo.TraceResults[i]); err != nil {
								log.Error("failed to write debug file", "err", err)
							}
							debugFile.Close()
						}
						validExec, err := traceTx(bc, ret.Block.Transactions()[i].Hash(), false)
						if err != nil {
							log.Error("failed to trace tx", "err", err)
						} else {
							if debugFile, err := os.Create(fmt.Sprintf("./tmp/%d-%d-true.json", ret.Block.NumberU64(), i)); err != nil {
								log.Error("create debug file failed")
							} else {
								if _, err = debugFile.Write(validExec); err != nil {
									log.Error("failed to write debug file", "err", err)
								}
								debugFile.Close()
							}
						}
					}
					if ret.DebugInfo != nil && len(ret.DebugInfo.MVCCStateDBs) > 0 {
						for j := 0; j <= i; j++ {
							readinfo := ret.DebugInfo.MVCCStateDBs[j].Report()
							rawreadinfo, _ := json.MarshalIndent(readinfo, "", "  ")
							if debugFile, err := os.Create(fmt.Sprintf("./tmp/%d-%d-report-false.json", ret.Block.NumberU64(), j)); err != nil {
								log.Error("create debug file failed")
							} else {
								if _, err = debugFile.Write(rawreadinfo); err != nil {
									log.Error("failed to write debug file", "err", err)
								}
								debugFile.Close()
							}
						}
						trueRes := occReplayBlock(bc, ret.Block, 1, false, false)
						// trueRes := serialReplayBlock(bc, ret.Block, false)
						if trueRes.Err != nil {
							log.Error("failed to get true result", "err", trueRes.Err)
						} else if trueRes.UsedGas != ret.Block.GasUsed() {
							log.Error("unmatched gas used - true result!", "origin", ret.Block.GasUsed(), "replayed", trueRes.UsedGas)
						}
						for j := 0; j <= i; j++ {
							truereadinfo := trueRes.DebugInfo.MVCCStateDBs[j].Report()
							rawtruereadinfo, _ := json.MarshalIndent(truereadinfo, "", "  ")
							if debugFile, err := os.Create(fmt.Sprintf("./tmp/%d-%d-report-true.json", ret.Block.NumberU64(), j)); err != nil {
								log.Error("create debug file failed")
							} else {
								if _, err = debugFile.Write(rawtruereadinfo); err != nil {
									log.Error("failed to write debug file", "err", err)
								}
								debugFile.Close()
							}
						}
					}
					break
				}
			}
			if !config.OmitFailure {
				return fmt.Errorf("unmatched gasUsed, block: %v, tx count: %v, origin gas: %v, result gas: %v",
					ret.Block.NumberU64(), ret.Block.Transactions().Len(), ret.Block.GasUsed(), ret.UsedGas)
			}
		}
		procStats.Add(ret.ProcessStats)
		// for _, txStats := range ret.ProcessStats.TxStats {
		// 	totalExecTime += txStats.ExecTime
		// 	totalExecCount += txStats.ExecCount
		// }
		cost += ret.Cost
	}

	log.Info("replay blocks done", "cost", cost,
		"avg cost per block", cost/time.Duration(taskCnt),
		"txs", procStats.TxCount,
		"avg block size", float64(procStats.TxCount)/float64(taskCnt),
		"aborted", procStats.Aborted,
		"avg critical path", float64(procStats.DynamicCriticalPathLen)/float64(taskCnt),
		"avg init time", procStats.Init/time.Duration(taskCnt),
		"avg process time", procStats.Process/time.Duration(taskCnt),
		"avg validate time", procStats.Validate/time.Duration(taskCnt),
		"avg finalize time", procStats.Finalize/time.Duration(taskCnt),
		"tx exec time", procStats.TxProcessTime/time.Duration(taskCnt),
		"tx exec cnt", procStats.TxProcessCnt/int(taskCnt),
	)
	// if totalExecCount > 0 {
	// 	log.Info("tx stats", "total exec count", totalExecCount, "avg exec time", totalExecTime/time.Duration(totalExecCount))
	// }
	if config.OmitFailure {
		log.Info("omit failure", "tx count", failedTxs)
	}

	return nil
}

// this method recreate statedb every tx
func serialReplayBlock(bc *core.BlockChain, block *types.Block, useTxLevelState bool) *BlockTaskResult {
	start := time.Now()
	if block == nil {
		return &BlockTaskResult{
			Err: errors.New("nil block"),
		}
	}
	p := core.NewReplayProcessor(bc.Config(), bc, useTxLevelState)
	statedb, _ := bc.GetStateDB(block.NumberU64(), 0)
	results, stats, gasUsed, err := p.ProcessBlock(block, vm.Config{}, statedb)
	if err != nil {
		return &BlockTaskResult{
			Err: err,
		}
	}
	return &BlockTaskResult{
		Cost:         time.Since(start),
		Block:        block,
		UsedGas:      gasUsed,
		TxResults:    results,
		ProcessStats: stats,
	}
}

func occReplayBlock(bc *core.BlockChain, block *types.Block, threadNum int, enableTrace, useCommutative bool) *BlockTaskResult {
	if block == nil {
		return &BlockTaskResult{
			Err: errors.New("nil block"),
		}
	}

	msgs := make([]*core.Message, block.Transactions().Len())
	var err error
	for i, tx := range block.Transactions() {
		msgs[i], err = core.TransactionToMessage(tx, types.MakeSigner(bc.Config(), block.Number(), block.Time()), block.BaseFee())
		if err != nil {
			return &BlockTaskResult{
				Err: fmt.Errorf("failed to convert tx to message: %w", err),
			}
		}
	}

	p := core.NewOccProcessor(bc.Config(), bc, core.ParallelConfig{
		WorkerNum:            threadNum,
		EnableTrace:          enableTrace,
		UseCommutative:       useCommutative,
		TrackRWSet:           false,
		DisableNonceValidate: false,
	})
	// p := core.NewOldOccProcessor(bc.Config(), bc, nil, threadNum)
	trie := bc.GetSlimArchiveTrie(block.NumberU64(), 0)
	results, stats, dbg, gasUsed, err := p.ExecuteMsgs(block.Header(), vm.Config{}, &core.ParallelInput{
		Msgs:       msgs,
		OCCStateDB: state.NewMVCCBaseStateDB(trie),
	})
	if err != nil {
		return &BlockTaskResult{
			Err: err,
		}
	}
	return &BlockTaskResult{
		Cost:         stats.Process,
		Block:        block,
		UsedGas:      gasUsed,
		TxResults:    results,
		ProcessStats: stats,
		DebugInfo:    dbg,
	}
}

func smartSchedReplayBlock(bc *core.BlockChain, block *types.Block, threadNum int, enableTrace, useCommutative, useGTRWSets, trackDependency bool) *BlockTaskResult {
	start := time.Now()
	if block == nil {
		return &BlockTaskResult{
			Err: errors.New("nil block"),
		}
	}

	msgs := make([]*core.Message, block.Transactions().Len())
	var err error
	for i, tx := range block.Transactions() {
		msgs[i], err = core.TransactionToMessage(tx, types.MakeSigner(bc.Config(), block.Number(), block.Time()), block.BaseFee())
		if err != nil {
			return &BlockTaskResult{
				Err: fmt.Errorf("failed to convert tx to message: %w", err),
			}
		}
	}

	trie := bc.GetSlimArchiveTrie(block.NumberU64(), 0)

	parallelConfig := core.ParallelConfig{
		WorkerNum:      threadNum,
		EnableTrace:    enableTrace,
		UseCommutative: useCommutative,
		TrackRWSet:     true,
	}
	input := &core.ParallelInput{
		Msgs:              msgs,
		OCCStateDB:        state.NewMVCCBaseStateDB(trie),
		SmartSchedStateDB: state.NewMVCCBaseStateDB(trie),
	}
	if useGTRWSets {
		tmpCfg := parallelConfig
		tmpCfg.WorkerNum = 1
		p := core.NewOccProcessor(bc.Config(), bc, tmpCfg)
		_, _, dbg, _, err := p.ExecuteMsgs(block.Header(), vm.Config{}, input)
		if err != nil {
			return &BlockTaskResult{
				Err: fmt.Errorf("failed to get rwsets: %w", err),
			}
		}
		input.RWSets = dbg.RWSets
	}
	p := core.NewSmartProcessor(bc.Config(), bc, parallelConfig)
	// results, stats, dbg, gasUsed, err := p.Process(block, vm.Config{}, statedb, rwSets)
	results, stats, dbg, gasUsed, err := p.ExecuteMsgs(block.Header(), vm.Config{}, input)
	if err != nil {
		return &BlockTaskResult{
			Err: err,
		}
	}
	return &BlockTaskResult{
		Cost:         time.Since(start),
		Block:        block,
		UsedGas:      gasUsed,
		TxResults:    results,
		ProcessStats: stats,
		DebugInfo:    dbg,
	}
}

func compareReplayBlock(bc *core.BlockChain, block *types.Block, threadNum int, processSchemes []string) *CompBlockTaskResult {
	start := time.Now()
	if block == nil {
		return &CompBlockTaskResult{
			Err: errors.New("nil block"),
		}
	}
	ret := &CompBlockTaskResult{
		Block: block,
	}

	msgs := make([]*core.Message, block.Transactions().Len())
	var err error
	for i, tx := range block.Transactions() {
		msgs[i], err = core.TransactionToMessage(tx, types.MakeSigner(bc.Config(), block.Number(), block.Time()), block.BaseFee())
		if err != nil {
			return &CompBlockTaskResult{
				Err: fmt.Errorf("failed to convert tx to message: %w", err),
			}
		}
	}
	header := block.Header()

	trie := bc.GetSlimArchiveTrie(block.NumberU64(), 0)
	// firstly, warm data and collect rwsets
	occ := core.NewOccProcessor(bc.Config(), bc, core.ParallelConfig{WorkerNum: 1, UseCommutative: true, TrackRWSet: true})
	_, _, dbg, _, err := occ.ExecuteMsgs(header, vm.Config{}, &core.ParallelInput{Msgs: msgs, OCCStateDB: state.NewMVCCBaseStateDB(trie)})
	if err != nil {
		return &CompBlockTaskResult{
			Err: fmt.Errorf("SmartSched failed to get rwsets: %w", err),
		}
	}

	// serial
	statedb, _ := state.New(common.Hash{}, trie, nil)
	serial := core.NewReplayProcessor(bc.Config(), bc, false)
	runtime.GC()
	results, stats, gasUsed, err := serial.ExecuteMsgs(msgs, header, vm.Config{}, statedb)
	if err != nil {
		return &CompBlockTaskResult{
			Err: fmt.Errorf("serial failed to replay block: %w", err),
		}
	}
	ret.Results = append(ret.Results, &CompBlockTaskResultSingle{
		Name:         "serial",
		UsedGas:      gasUsed,
		ProcessStats: stats,
		TxResults:    results,
	})

	for _, msg := range msgs {
		msg.SkipAccountChecks = true
	}

	parallelConfig := core.ParallelConfig{WorkerNum: threadNum, UseCommutative: true}
	input := &core.ParallelInput{
		Msgs:              msgs,
		RWSets:            dbg.RWSets,
		OCCStateDB:        state.NewMVCCBaseStateDB(trie),
		TwoPLStateDB:      state.NewMVCCBaseStateDB(trie),
		STMStateDB:        state.SlimArchiveTrieToSTMStorage(trie),
		SpectrumStateDB:   state.NewMVCCBaseStateDB(trie),
		DmvccStateDB:      state.NewMVCCBaseStateDB(trie),
		ParEVMStateDB:     state.NewMVCCBaseStateDB(trie),
		SmartSchedStateDB: state.NewMVCCBaseStateDB(trie),
	}

	processors := core.CreateParallelProcessors(bc.Config(), bc, processSchemes, parallelConfig)

	// parallel
	for _, p := range processors {
		runtime.GC()
		results, stats, _, gasUsed, err := p.ExecuteMsgs(header, vm.Config{}, input)
		if err != nil {
			return &CompBlockTaskResult{
				Err: fmt.Errorf("%s parallel failed to replay block: %w", p.Name(), err),
			}
		}
		ret.Results = append(ret.Results, &CompBlockTaskResultSingle{
			Name:         p.Name(),
			UsedGas:      gasUsed,
			ProcessStats: stats,
			TxResults:    results,
		})
	}

	ret.Cost = time.Since(start)
	return ret
}

func TraceTx(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	chain, db := utils.MakeChain(ctx, stack, false)
	defer db.Close()
	defer chain.Stop()

	txhash := ctx.String(TxHashFlag.Name)
	if txhash == "" {
		return errors.New("invalid txhash")
	}

	res, err := traceTx(chain, common.HexToHash(txhash), ctx.Bool(UseMVCCFlag.Name))
	if err != nil {
		log.Error(err.Error())
		return err
	}
	if debugFile, err := os.Create(fmt.Sprintf("./tmp/%s.json", txhash)); err != nil {
		log.Error("create debug file failed")
	} else {
		if _, err = debugFile.Write(res); err != nil {
			log.Error("failed to write debug file", "err", err)
		}
		debugFile.Close()
	}
	return nil
}

func traceTx(bc *core.BlockChain, hash common.Hash, useMvccDb bool) (json.RawMessage, error) {
	tx, _, number, index := rawdb.ReadTransaction(bc.ChainDB(), hash)
	log.Info("trace tx", "hash", hash, "block", number, "index", index, "use mvcc", useMvccDb)
	if tx == nil {
		return nil, errors.New("not found")
	}
	block := bc.GetBlockByNumber(number)
	if block == nil {
		return nil, errors.New("not found")
	}
	var (
		gp     = new(core.GasPool).AddGas(block.GasLimit())
		signer = types.MakeSigner(bc.Config(), block.Number(), block.Time())
	)
	trie := bc.GetSlimArchiveTrie(number, int(index))
	var vmstate vm.StateDB
	if useMvccDb {
		mvccBaseStateDb := state.NewMVCCBaseStateDB(trie)
		vmstate = state.NewMVCCStateDB(mvccBaseStateDb, int(index), tx.Hash(), nil)
	} else {
		serialStateDb, _ := state.New(common.Hash{}, trie, nil)
		serialStateDb.SetTxContext(tx.Hash(), int(index))
		vmstate = serialStateDb
	}
	msg, err := core.TransactionToMessage(tx, signer, block.BaseFee())
	if err != nil {
		return nil, err
	}
	tracer := logger.NewStructLogger(&logger.Config{})
	vmenv := vm.NewEVM(core.NewEVMBlockContext(block.Header(), bc, nil), core.NewEVMTxContext(msg), vmstate, bc.Config(), vm.Config{
		Tracer: tracer,
	})
	_, err = core.ApplyMessage(vmenv, msg, gp)
	if err != nil {
		return nil, err
	} else {
		switch s := vmstate.(type) {
		case *state.MVCCStateDB:
			err = s.Error()
		case *state.StateDB:
			err = s.Error()
		}
		if err != nil {
			return nil, err
		}
	}
	traceResult, _ := tracer.GetResult()
	return traceResult, nil
}
