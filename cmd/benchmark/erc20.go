package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/cmd/benchmark/erc20"
	"github.com/ethereum/go-ethereum/cmd/benchmark/stateinit"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/log"
	"github.com/schollz/progressbar/v3"
)

func loadOrCreateERC20TransferPairs(accounts, blocks, txsPerBlock int) ([][][2]int, error) {
	filePath := fmt.Sprintf("./datasets/erc20_%d_%d_%d_transfer_pairs.json", accounts, blocks, txsPerBlock)
	var pairs [][][2]int
	_, err := os.Stat(filePath)
	if err == nil {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read file: %v", err)
		}
		if err := json.Unmarshal(data, &pairs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal json: %v", err)
		}
		return pairs, nil
	} else if os.IsNotExist(err) {
		log.Info("Creating new dataset", "filepath", filePath)
		pairs = erc20.CreateUniformTransferPairs(accounts, blocks, txsPerBlock)
		data, err := json.Marshal(pairs)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal json: %v", err)
		}
		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory: %v", err)
		}
		if err := os.WriteFile(filePath, data, 0644); err != nil {
			return nil, fmt.Errorf("failed to write file: %v", err)
		}
		return pairs, nil
	}
	return nil, err
}

func BenchmarkERC20(config *Config) error {
	// fmt.Println("init env")
	log.Info("Run ERC-20 benchmark", "threads", config.Threads, "accounts", config.Accounts, "txs", config.TxsPerBlock, "rounds", config.Rounds)

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

	env := erc20.CreateERC20Env(config.Accounts)
	processors := core.ParallelProcessors
	if config.OnlySmartScheds {
		processors = core.ParallelProcessorsSmartScheds
	} else if config.AllSchemes {
		processors = core.AllParallelProcessors
	}
	transferResults := make([]Result, len(processors)+1)
	for i := range transferResults {
		transferResults[i].Stats = &core.ProcessStats{}
	}

	// pairs := erc20.CreateUniformTransferPairs(config.Accounts, config.Rounds, config.TxsPerBlock)
	pairs, err := loadOrCreateERC20TransferPairs(config.Accounts, config.Rounds, config.TxsPerBlock)
	if err != nil {
		return err
	}
	blocks := erc20.CreateERC20TransferUniformDataSet(env, pairs, big.NewInt(5))

	bar := progressbar.Default(int64(config.Rounds))
	for i := 0; i < config.Rounds; i++ {
		results := run(env.StateEnv, blocks[i], processors,
			core.ParallelConfig{WorkerNum: config.Threads, UseCommutative: true, DisableNonceValidate: true})
		for j := range transferResults {
			transferResults[j].GasUsed += results[j].GasUsed
			transferResults[j].Stats.Add(results[j].Stats)
			if transferResults[j].Name == "" {
				transferResults[j].Name = results[j].Name
			}
			if transferResults[j].Name != results[j].Name {
				panic("mismatch processor names")
			}
		}
		bar.Add(1)
	}

	// fmt.Println("verify balance")
	balanceOfMsgs := erc20.CreateERC20BalanceOfDataSet(env)
	balanceResults := run(env.StateEnv, balanceOfMsgs, processors, core.ParallelConfig{WorkerNum: 1})
	finalDelta := new(big.Int)
	absDelta := new(big.Int)
	for i := 0; i < config.Accounts; i++ {
		for j := range processors {
			if !bytes.Equal(balanceResults[0].Result[i].ReturnData, balanceResults[j+1].Result[i].ReturnData) {
				// return fmt.Errorf("balance of mismatch, processors %s account %d", processros[j+1], i)
				fmt.Printf("balance of mismatch, processors %s account %d want %v got %v\n", balanceResults[j+1].Name, i,
					new(big.Int).SetBytes(balanceResults[0].Result[i].ReturnData),
					new(big.Int).SetBytes(balanceResults[j+1].Result[i].ReturnData),
				)
			}
		}
		curBalance := new(big.Int).SetBytes(balanceResults[0].Result[i].ReturnData)
		delta := new(big.Int).Sub(stateinit.InitBalance.ToBig(), curBalance)
		absDelta.Add(absDelta, new(big.Int).Abs(delta))
		finalDelta.Add(finalDelta, delta)
		// fmt.Println(env.Accounts[i], curBalance)
	}
	// fmt.Println("final delta", finalDelta, "abs delta", absDelta)

	for _, r := range transferResults {
		log.Info("Exec stats",
			"scheme", r.Name,
			"process", r.Stats.Process/time.Duration(config.Rounds),
			// "aborted", r.Stats.Aborted/int(config.Rounds),
			// "validate", r.Stats.Validate/time.Duration(config.Rounds),
			// "finalize", r.Stats.Finalize/time.Duration(config.Rounds),
			// "workers", r.Stats.AvgRunningWorker/float64(config.Rounds),
			// "critical path", r.Stats.StaticCriticalPathLen/int(config.Rounds), r.Stats.DynamicCriticalPathLen/int(config.Rounds),
			"gas/tx", r.GasUsed/uint64(config.TxsPerBlock)/uint64(config.Rounds),
			// "tx exec time", r.Stats.TxExecTime/time.Duration(config.Rounds),
			"tx exec cnt", r.Stats.TxProcessCnt/int(config.Rounds),
			"speedup", float64(transferResults[0].Stats.Process)/float64(r.Stats.Process),
		)
		if csvWriter != nil {
			// csv line: scheme, threads, accounts, txs, process time (ms).
			csvWriter.Write([]string{
				r.Name,
				fmt.Sprintf("%d", config.Threads),
				fmt.Sprintf("%d", config.Accounts),
				fmt.Sprintf("%d", config.TxsPerBlock),
				fmt.Sprintf("%.3f", (r.Stats.Process/time.Duration(config.Rounds)).Seconds()*1e3),
				fmt.Sprintf("%.3f", float64(transferResults[0].Stats.Process)/float64(r.Stats.Process)),
			})
		}
	}
	return nil
}
