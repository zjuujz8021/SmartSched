package core

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/schollz/progressbar/v3"
)

func (bc *BlockChain) BlocksByRange(start, end uint64) (blocks types.Blocks, err error) {
	t0 := time.Now()
	blocks, err = bc.blocksByRange(start, end, 20)
	log.Info("fetch finished", "cost", time.Since(t0), "total", len(blocks))
	return
}

func (bc *BlockChain) blocksByRange(start, end uint64, maxWorkers int) (blocks types.Blocks, err error) {
	if start == 0 {
		start = 1
	}
	totalTasks := int(end - start)

	taskCh := make(chan uint64, totalTasks)
	go func() {
		for i := start; i < end; i++ {
			taskCh <- i
		}
		close(taskCh)
	}()

	resCh := make(chan *types.Block, totalTasks)

	fetchWorker := func() {
		for task := range taskCh {
			blk := bc.GetBlockByNumber(task)
			if blk == nil {
				panic(fmt.Errorf("block %v not found", start))
			}
			resCh <- blk
		}
	}

	for i := 0; i < maxWorkers; i++ {
		go fetchWorker()
	}
	blocks = make(types.Blocks, 0, totalTasks)

	for i := 0; i < totalTasks; i++ {
		blocks = append(blocks, <-resCh)
	}
	return
}

// 10, 50, 90 percentiles
func (bc *BlockChain) SampleBlocksByPercentiles(start, end, interval uint64, percentiles []int) (types.Blocks, error) {
	log.Info("sample trisector blocks", "start", start, "end", end, "interval", interval)
	for _, p := range percentiles {
		if p < 0 || p > 100 {
			return nil, errors.New("percentile must be between 0 and 100")
		}
	}
	if interval < 1 {
		return nil, errors.New("interval must > 0")
	} else if interval == 1 {
		return bc.blocksByRange(start, end, 100)
	}
	bar := progressbar.Default(int64((end - start) / interval))
	sampled := make(types.Blocks, 0)
	t0 := time.Now()
	for i := start; i < end; i += interval {
		bar.Add(1)
		blocks, err := bc.blocksByRange(i, i+interval, 10)
		if err != nil {
			return nil, err
		}
		sort.Slice(blocks, func(i, j int) bool {
			if len(blocks[i].Transactions()) != len(blocks[j].Transactions()) {
				return blocks[i].Transactions().Len() < blocks[j].Transactions().Len()
			}
			return blocks[i].NumberU64() < blocks[j].NumberU64()
		})
		for _, p := range percentiles {
			idx := len(blocks) * p / 100
			if idx == len(blocks) {
				idx--
			}
			targetBlock := blocks[idx]
			signer := types.MakeSigner(bc.Config(), targetBlock.Number(), targetBlock.Time())
			SenderCacher.Recover(signer, targetBlock.Transactions())
			sampled = append(sampled, targetBlock)
		}
	}
	log.Info("sampling finished", "cost", time.Since(t0), "total", len(sampled))
	return sampled, nil
}

func (bc *BlockChain) RandomlySampleBlocks(start, end, interval uint64) (types.Blocks, error) {
	log.Info("randomly sample blocks", "start", start, "end", end, "interval", interval)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	if interval < 1 {
		return nil, errors.New("interval must > 0")
	} else if interval == 1 {
		return bc.blocksByRange(start, end, 100)
	}
	bar := progressbar.Default(int64((end - start) / interval))
	sampled := make(types.Blocks, 0)
	t0 := time.Now()
	for i := start; i < end; i += interval {
		bar.Add(1)
		blocks, err := bc.blocksByRange(i, i+interval, 10)
		if err != nil {
			return nil, err
		}
		targetBlock := blocks[r.Intn(len(blocks))]
		signer := types.MakeSigner(bc.Config(), targetBlock.Number(), targetBlock.Time())
		SenderCacher.Recover(signer, targetBlock.Transactions())
		sampled = append(sampled, targetBlock)
		// ch <- &BlockTask{Number: blocks[interval/2].NumberU64(), Block: blocks[interval/2]}
	}
	log.Info("sampling finished", "cost", time.Since(t0), "total", len(sampled))
	return sampled, nil
}

func (bc *BlockChain) SampleBlockByMaxTxCount(start, end, interval uint64) (types.Blocks, error) {
	log.Info("sample blocks by max tx count", "start", start, "end", end, "interval", interval)
	if interval < 1 {
		return nil, errors.New("interval must > 0")
	} else if interval == 1 {
		return bc.blocksByRange(start, end, 100)
	}
	bar := progressbar.Default(int64((end - start) / interval))
	sampled := make(types.Blocks, 0)
	t0 := time.Now()
	for i := start; i < end; i += interval {
		bar.Add(1)
		blocks, err := bc.blocksByRange(i, i+interval, 10)
		if err != nil {
			return nil, err
		}
		sort.Slice(blocks, func(i, j int) bool {
			if len(blocks[i].Transactions()) != len(blocks[j].Transactions()) {
				return blocks[i].Transactions().Len() < blocks[j].Transactions().Len()
			}
			return blocks[i].NumberU64() < blocks[j].NumberU64()
		})
		targetBlock := blocks[len(blocks)-1]
		signer := types.MakeSigner(bc.Config(), targetBlock.Number(), targetBlock.Time())
		SenderCacher.Recover(signer, targetBlock.Transactions())
		sampled = append(sampled, targetBlock)
		// ch <- &BlockTask{Number: blocks[interval/2].NumberU64(), Block: blocks[interval/2]}
	}
	log.Info("sampling finished", "cost", time.Since(t0), "total", len(sampled))
	return sampled, nil
}

func (bc *BlockChain) SampleBlockByMaxTxCountCh(start, end, interval uint64, ch chan<- *types.Block) {
	log.Info("sample blocks by max tx count", "start", start, "end", end, "interval", interval)
	if interval < 1 {
		panic("interval must > 0")
	}
	cnt := 0
	t0 := time.Now()
	for i := start; i < end; i += interval {
		cnt++
		blocks, err := bc.blocksByRange(i, i+interval, 10)
		if err != nil {
			ch <- nil
		}
		sort.SliceStable(blocks, func(i, j int) bool {
			if blocks[i].Transactions().Len() != blocks[j].Transactions().Len() {
				return blocks[i].Transactions().Len() < blocks[j].Transactions().Len()
			}
			return blocks[i].NumberU64() < blocks[j].NumberU64()
		})
		targetBlock := blocks[len(blocks)-1]
		signer := types.MakeSigner(bc.Config(), targetBlock.Number(), targetBlock.Time())
		SenderCacher.Recover(signer, targetBlock.Transactions())
		ch <- targetBlock
	}
	close(ch)
	log.Info("sampling finished", "cost", time.Since(t0), "total", cnt)
}
