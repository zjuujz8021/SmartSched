package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli/v2"
)

var (
	ThreadsSliceFlag = &cli.IntSliceFlag{
		Name:  "threads.slice",
		Value: cli.NewIntSlice(8, 16),
	}
	LogFilePrefixFlag = &cli.StringFlag{
		Name:  "log.file.prefix",
		Value: "./results/logs/history",
	}
)

type ExecConfig struct {
	Start, End, Interval int
	Threads              []int
	LogFilePrefix        string
	OnlySmartScheds      bool
	AllSchemes           bool
}

type ExecResult struct {
	Name         string
	UsedGas      uint64
	ProcessStats *core.ProcessStats
}

func ExecuteBlocks(ctx *cli.Context) error {
	config := &ExecConfig{
		Start:           ctx.Int(ReplayStartFlag.Name),
		End:             ctx.Int(ReplayEndFlag.Name),
		Interval:        ctx.Int(IntervalFlag.Name),
		Threads:         ctx.IntSlice(ThreadsSliceFlag.Name),
		LogFilePrefix:   ctx.String(LogFilePrefixFlag.Name),
		OnlySmartScheds: ctx.Bool(utils.OnlySmartSchedsFlag.Name),
		AllSchemes:      ctx.Bool(utils.AllSchemesFlag.Name),
	}

	confStr, _ := json.Marshal(config)
	fmt.Println("execute config: ", string(confStr))

	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	chain, db := utils.MakeChain(ctx, stack, false)
	defer db.Close()
	defer chain.Stop()

	return executeBlocks(chain, config)
}

func executeBlocks(bc *core.BlockChain, config *ExecConfig) error {
	csvWriters := make([]*csv.Writer, len(config.Threads))
	for i := range csvWriters {
		csvFile, err := os.OpenFile(fmt.Sprintf("%s_%d.csv", config.LogFilePrefix, config.Threads[i]), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		defer csvFile.Close()
		csvWriters[i] = csv.NewWriter(csvFile)
	}

	tasks, err := bc.SampleBlocksByPercentiles(uint64(config.Start), uint64(config.End), uint64(config.Interval), []int{90, 100})
	if err != nil {
		return err
	}
	taskCnt := len(tasks)
	msgs := make([]*core.Message, 0, 5000)
	parallelSchemes := core.ParallelProcessors
	if config.OnlySmartScheds {
		parallelSchemes = core.ParallelProcessorsSmartScheds
	} else if config.AllSchemes {
		parallelSchemes = core.AllParallelProcessors
	}
	schemes := append([]string{"serial"}, parallelSchemes...)
	failures := make([]int, len(schemes))

	bar := progressbar.Default(int64(taskCnt))
	for _, block := range tasks {
		if block.Transactions().Len() == 0 {
			continue
		}
		header := block.Header()
		msgs = msgs[:0]
		for _, tx := range block.Transactions() {
			msg, err := core.TransactionToMessage(tx, types.MakeSigner(bc.Config(), block.Number(), block.Time()), block.BaseFee())
			if err != nil {
				return fmt.Errorf("failed to convert tx to message: %w", err)
			}
			msgs = append(msgs, msg)
		}
		trie, rws, err := getSlimArchiveTrieAndRWSets(bc, block.Header(), msgs)
		if err != nil {
			return fmt.Errorf("failed to get trie and rws: %w", err)
		}
		for threadIdx, thread := range config.Threads {
			results, err := executeBlock(bc, header, msgs, trie, rws, parallelSchemes, thread)
			if err != nil {
				return fmt.Errorf("failed to execute block: %w", err)
			}
			if len(results) != len(schemes) {
				return errors.New("unmatched result count")
			}
			for resultIdx, result := range results {
				if result.Name != schemes[resultIdx] {
					return errors.New("unmatched result name")
				}
				if result.UsedGas != block.GasUsed() {
					failures[resultIdx]++
				}
			}
			if err = csvWriters[threadIdx].Write(execResultsToCSVRow(block, results)); err != nil {
				log.Error("write csv failed", "err", err)
			}
			csvWriters[threadIdx].Flush()
			if err = csvWriters[threadIdx].Error(); err != nil {
				log.Error("flush csv failed", "err", err)
			}
		}
		bar.Add(1)
	}

	for i := range schemes {
		log.Info("Exec stats", "scheme", schemes[i],
			"total", taskCnt*len(config.Threads),
			"failures", failures[i],
		)
	}
	return nil
}

func execResultsToCSVRow(block *types.Block, result []*ExecResult) []string {
	// row: block number, tx count, process cost for each scheme...
	row := make([]string, 0, len(result)+2)
	row = append(row, strconv.FormatUint(block.Number().Uint64(), 10))
	row = append(row, strconv.Itoa(len(block.Transactions())))
	for _, res := range result {
		row = append(row, fmt.Sprintf("%.3f", res.ProcessStats.Process.Seconds()*1e3))
	}
	return row
}

func getSlimArchiveTrieAndRWSets(bc *core.BlockChain, header *types.Header, msgs []*core.Message) (*state.SlimArchiveTrie, []*state.RWSet, error) {
	trie := bc.GetSlimArchiveTrie(header.Number.Uint64(), 0)
	// warm data and collect rwsets
	occ := core.NewOccProcessor(bc.Config(), bc, core.ParallelConfig{WorkerNum: 16, UseCommutative: true, TrackRWSet: true})
	_, _, dbg, _, err := occ.ExecuteMsgs(header, vm.Config{}, &core.ParallelInput{Msgs: msgs, OCCStateDB: state.NewMVCCBaseStateDB(trie)})
	if err != nil {
		return nil, nil, err
	}
	return trie, dbg.RWSets, nil
}

func executeBlock(bc *core.BlockChain, header *types.Header, msgs []*core.Message,
	trie *state.SlimArchiveTrie, rws []*state.RWSet, schemes []string, thread int) ([]*ExecResult, error) {
	ret := make([]*ExecResult, 0)
	// serial
	statedb, _ := state.New(common.Hash{}, trie, nil)
	serial := core.NewReplayProcessor(bc.Config(), bc, false)
	runtime.GC()
	_, stats, gasUsed, err := serial.ExecuteMsgs(msgs, header, vm.Config{}, statedb)
	if err != nil {
		return nil, fmt.Errorf("serial failed to replay block: %w", err)
	}
	ret = append(ret, &ExecResult{
		Name:         "serial",
		UsedGas:      gasUsed,
		ProcessStats: stats,
	})

	for _, msg := range msgs {
		msg.SkipAccountChecks = true
	}

	parallelConfig := core.ParallelConfig{WorkerNum: thread, UseCommutative: true}
	input := &core.ParallelInput{
		Msgs:                           msgs,
		RWSets:                         rws,
		OCCStateDB:                     state.NewMVCCBaseStateDB(trie),
		TwoPLStateDB:                   state.NewMVCCBaseStateDB(trie),
		STMStateDB:                     state.SlimArchiveTrieToSTMStorage(trie),
		SpectrumStateDB:                state.NewMVCCBaseStateDB(trie),
		DmvccStateDB:                   state.NewMVCCBaseStateDB(trie),
		ParEVMStateDB:                  state.NewMVCCBaseStateDB(trie),
		SmartSchedStateDB:              state.NewMVCCBaseStateDB(trie),
		SmartSchedOnlySchedStateDB:     state.NewMVCCBaseStateDB(trie),
		SmartSchedOnlyPreCommitStateDB: state.NewMVCCBaseStateDB(trie),
	}

	processors := core.CreateParallelProcessors(bc.Config(), bc, schemes, parallelConfig)

	// parallel
	for _, p := range processors {
		runtime.GC()
		_, stats, _, gasUsed, err := p.ExecuteMsgs(header, vm.Config{}, input)
		if err != nil {
			return nil, fmt.Errorf("%s parallel failed to replay block: %w", p.Name(), err)
		}
		ret = append(ret, &ExecResult{
			Name:         p.Name(),
			UsedGas:      gasUsed,
			ProcessStats: stats,
		})
	}

	for _, msg := range msgs {
		msg.SkipAccountChecks = false
	}

	return ret, nil
}
