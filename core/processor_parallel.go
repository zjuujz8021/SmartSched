package core

import (
	"encoding/json"

	blockstm "github.com/crypto-org-chain/go-block-stm"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

type DebugInfo struct {
	TraceResults []json.RawMessage
	MVCCStateDBs []*state.MVCCStateDB
	RWSets       []*state.RWSet
}

type ParallelConfig struct {
	// Number of worker threads for parallel execution
	WorkerNum int

	// Whether to use commutative operations optimization
	UseCommutative bool

	// Whether to disable nonce validation
	DisableNonceValidate bool

	// Whether to enable tracing for debugging
	EnableTrace bool

	// Whether to track RWSet
	TrackRWSet bool

	// Whether to track dependency
	TrackDependency bool
}

type ParallelInput struct {
	Msgs   []*Message
	RWSets []*state.RWSet

	OCCStateDB        *state.MVCCBaseStateDB
	TwoPLStateDB      *state.MVCCBaseStateDB
	STMStateDB        blockstm.MultiStore
	SpectrumStateDB   *state.MVCCBaseStateDB
	DmvccStateDB      *state.MVCCBaseStateDB
	ParEVMStateDB     *state.MVCCBaseStateDB
	SmartSchedStateDB *state.MVCCBaseStateDB

	SmartSchedOnlySchedStateDB     *state.MVCCBaseStateDB
	SmartSchedOnlyPreCommitStateDB *state.MVCCBaseStateDB
}

type ParallelProcessor interface {
	Name() string
	// ExecuteMsgs processes a batch of messages in parallel
	// Returns execution results, processing stats, debug info (if any), gas used, and any error
	ExecuteMsgs(header *types.Header, cfg vm.Config, input *ParallelInput) ([]*ExecutionResult, *ProcessStats, *DebugInfo, uint64, error)
}

const (
	OCC                      = "occ"
	TWOPL                    = "2pl"
	STM                      = "stm"
	SPECTRUM                 = "spectrum"
	DMVCC                    = "dmvcc"
	PAREVM                   = "parevm"
	SMARTSCHED_ONLYPRECOMMIT = "smartsched_onlyPreCommit"
	SMARTSCHED_ONLYSCHED     = "smartsched_onlySched"
	SMARTSCHED               = "smartsched"
)

var AllParallelProcessors = []string{OCC, TWOPL, STM, SPECTRUM, DMVCC, PAREVM, SMARTSCHED_ONLYPRECOMMIT, SMARTSCHED_ONLYSCHED, SMARTSCHED}
var ParallelProcessors = []string{OCC, TWOPL, STM, SPECTRUM, DMVCC, PAREVM, SMARTSCHED}
var ParallelProcessorsSmartScheds = []string{SPECTRUM, SMARTSCHED_ONLYPRECOMMIT, SMARTSCHED_ONLYSCHED, SMARTSCHED}

func CreateParallelProcessors(chainConfig *params.ChainConfig, bc *BlockChain, names []string, parallelConfig ParallelConfig) []ParallelProcessor {
	ps := make([]ParallelProcessor, len(names))
	for i, name := range names {
		switch name {
		case OCC:
			ps[i] = NewOccProcessor(chainConfig, bc, parallelConfig)
		case TWOPL:
			ps[i] = NewTwoPLProcessor(chainConfig, bc, parallelConfig)
		case STM:
			ps[i] = NewSTMProcessor(chainConfig, bc, parallelConfig)
		case SPECTRUM:
			ps[i] = NewSpectrumProcessor(chainConfig, bc, parallelConfig)
		case DMVCC:
			ps[i] = NewDmvccProcessor(chainConfig, bc, parallelConfig)
		case PAREVM:
			ps[i] = NewParEVMProcessor(chainConfig, bc, parallelConfig)
		case SMARTSCHED_ONLYPRECOMMIT:
			ps[i] = NewSmartProcessorOnlyPrecommit(chainConfig, bc, parallelConfig)
		case SMARTSCHED_ONLYSCHED:
			ps[i] = NewSmartProcessorOnlyScheduling(chainConfig, bc, parallelConfig)
		case SMARTSCHED:
			ps[i] = NewSmartProcessor(chainConfig, bc, parallelConfig)
		default:
			panic("unknown processor name")
		}
	}
	return ps
}
