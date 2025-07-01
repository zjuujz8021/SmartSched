package state

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

type OperationCode uint8

const (
	OpEmpty OperationCode = iota
	OpAdd
	OpSub
	OpMul
	OpDiv
	OpSdiv
	OpMod
	OpSmod
	OpAddmod
	OpMulmod
	OpExp
	OpSignExtend
	OpLT
	OpGT
	OpSLT
	OpSGT
	OpEQ
	OpIsZero
	OpAnd
	OpOr
	OpXor
	OpNot
	OpByte
	OpShl
	OpShr
	OpSar

	OpSload
	OpSstore
	OpSstoreGasV1
	OpSstoreGasV2
	OpSstoreGasV3
	OpSstoreGasV4

	OpAssert
)

type OperationOperands struct {
	lsn   []int
	value []uint256.Int
}

type OperationLogEntry struct {
	opcode   OperationCode
	operands OperationOperands
	result   uint256.Int
	redo     bool
}

type OperationLog struct {
	entries []OperationLogEntry

	mostRecentWrites map[common.Address]map[common.Hash]int   // the most recent writes to (addr, key)
	readCommittedLSN map[common.Address]map[common.Hash][]int // all sloads that read committed (addr, key) directly
	createdAccount   map[common.Address]struct{}
}

func newOperationLog() *OperationLog {
	logEntries := make([]OperationLogEntry, 0, 100)
	logEntries = append(logEntries, OperationLogEntry{opcode: OpEmpty, redo: false})
	return &OperationLog{
		entries: logEntries,

		mostRecentWrites: make(map[common.Address]map[common.Hash]int),
		readCommittedLSN: make(map[common.Address]map[common.Hash][]int),
		createdAccount:   make(map[common.Address]struct{}),
	}
}

func (oplog *OperationLog) Stat() (oplen, constraint_len, redo_len int) {
	oplen = len(oplog.entries) - 1
	for _, ent := range oplog.entries {
		if ent.opcode == OpAssert {
			constraint_len++
		}
		if ent.redo {
			redo_len++
		}
	}
	return
}

func (oplog *OperationLog) CreateAccount(addr common.Address) {
	oplog.createdAccount[addr] = struct{}{}
	oplog.mostRecentWrites[addr] = nil
}

func (oplog *OperationLog) updateMostRecentWriteLSN(addr common.Address, key common.Hash, lsn int) {
	accountMap := oplog.mostRecentWrites[addr]
	if accountMap == nil {
		accountMap = make(map[common.Hash]int)
		oplog.mostRecentWrites[addr] = accountMap
	}
	accountMap[key] = lsn
}

func (oplog *OperationLog) getMostRecentWriteLSN(addr common.Address, key common.Hash) int {
	if accountMap := oplog.mostRecentWrites[addr]; accountMap != nil {
		if lsn, keyExist := accountMap[key]; keyExist {
			return lsn
		}
	}
	return 0
}

func (oplog *OperationLog) insertReadCommittedStateLSN(addr common.Address, key common.Hash, lsn int) {
	accountMap := oplog.readCommittedLSN[addr]
	if accountMap == nil {
		accountMap = make(map[common.Hash][]int)
		oplog.readCommittedLSN[addr] = accountMap
	}
	accountMap[key] = append(accountMap[key], lsn)
}

func (oplog *OperationLog) AppendSStoreGasV1Log(addr common.Address, key, current common.Hash,
	val uint256.Int, valTag int, gas, refund uint64) {
	currentLSN := len(oplog.entries)
	prev_write := oplog.getMostRecentWriteLSN(addr, key)
	if prev_write == 0 {
		if _, created := oplog.createdAccount[addr]; !created {
			oplog.insertReadCommittedStateLSN(addr, key, currentLSN)
		}
	}
	oplog.entries = append(oplog.entries, OperationLogEntry{
		opcode: OpSstoreGasV1,
		operands: OperationOperands{lsn: []int{prev_write, valTag},
			value: []uint256.Int{*uint256.NewInt(0).SetBytes(current.Bytes()), val}},
		result: uint256.Int{gas, refund},
		redo:   false,
	})
}

func (oplog *OperationLog) AppendSStoreGasV2Log(addr common.Address, key, original, current, value common.Hash,
	valTag int, gas, refund uint64) {
	currentLSN := len(oplog.entries)
	prev_write := oplog.getMostRecentWriteLSN(addr, key)
	oplog.insertReadCommittedStateLSN(addr, key, currentLSN)

	createdCurrent := uint64(0)
	if _, created := oplog.createdAccount[addr]; created {
		createdCurrent = 1
	}

	oplog.entries = append(oplog.entries, OperationLogEntry{
		opcode: OpSstoreGasV2,
		operands: OperationOperands{lsn: []int{0, prev_write, valTag},
			value: []uint256.Int{*uint256.NewInt(0).SetBytes(original.Bytes()),
				*uint256.NewInt(0).SetBytes(current.Bytes()),
				*uint256.NewInt(0).SetBytes(value.Bytes())}},
		result: uint256.Int{gas, refund, 0, createdCurrent},
		redo:   false,
	})
}

func (oplog *OperationLog) AppendSStoreGasV3Log(addr common.Address, key, original, current, value common.Hash,
	valTag int, gas, refund, clearingRefund uint64) {
	currentLSN := len(oplog.entries)
	prev_write := oplog.getMostRecentWriteLSN(addr, key)
	oplog.insertReadCommittedStateLSN(addr, key, currentLSN)

	createdCurrent := uint64(0)
	if _, created := oplog.createdAccount[addr]; created {
		createdCurrent = 1
	}

	oplog.entries = append(oplog.entries, OperationLogEntry{
		opcode: OpSstoreGasV3,
		operands: OperationOperands{lsn: []int{0, prev_write, valTag},
			value: []uint256.Int{*uint256.NewInt(0).SetBytes(original.Bytes()),
				*uint256.NewInt(0).SetBytes(current.Bytes()),
				*uint256.NewInt(0).SetBytes(value.Bytes())}},
		result: uint256.Int{gas, refund, clearingRefund, createdCurrent},
		redo:   false,
	})
}

func (oplog *OperationLog) AppendSStoreGasV4Log(addr common.Address, key, original, current, value common.Hash,
	valTag int, gas, refund uint64) {
	currentLSN := len(oplog.entries)
	prev_write := oplog.getMostRecentWriteLSN(addr, key)
	oplog.insertReadCommittedStateLSN(addr, key, currentLSN)

	createdCurrent := uint64(0)
	if _, created := oplog.createdAccount[addr]; created {
		createdCurrent = 1
	}

	oplog.entries = append(oplog.entries, OperationLogEntry{
		opcode: OpSstoreGasV4,
		operands: OperationOperands{lsn: []int{0, prev_write, valTag},
			value: []uint256.Int{*uint256.NewInt(0).SetBytes(original.Bytes()),
				*uint256.NewInt(0).SetBytes(current.Bytes()),
				*uint256.NewInt(0).SetBytes(value.Bytes())}},
		result: uint256.Int{gas, refund, 0, createdCurrent},
		redo:   false,
	})
}

func (oplog *OperationLog) AppendSStoreLog(addr common.Address, key common.Hash, val uint256.Int, valTag int) {
	currentLSN := len(oplog.entries)
	oplog.entries = append(oplog.entries, OperationLogEntry{
		opcode: OpSstore,
		operands: OperationOperands{lsn: []int{valTag},
			value: []uint256.Int{
				*uint256.NewInt(0).SetBytes(val.Bytes()),
				*uint256.NewInt(0).SetBytes(addr.Bytes()),
				*uint256.NewInt(0).SetBytes(key.Bytes())}},
		result: val,
		redo:   false,
	})
	oplog.updateMostRecentWriteLSN(addr, key, currentLSN)
}

func (oplog *OperationLog) AppendSloadLog(addr common.Address, key common.Hash, val common.Hash) int {
	prev_write := oplog.getMostRecentWriteLSN(addr, key)
	if prev_write == 0 {
		if _, created := oplog.createdAccount[addr]; !created {
			currentLSN := len(oplog.entries)
			oplog.entries = append(oplog.entries, OperationLogEntry{
				opcode: OpSload,
				result: *uint256.NewInt(0).SetBytes(val.Bytes()),
				redo:   false,
			})
			oplog.insertReadCommittedStateLSN(addr, key, currentLSN)
			return currentLSN
		}
	}
	return prev_write
}

func (oplog *OperationLog) AppendAssertion(tag int, value uint256.Int) {
	if tag != 0 {
		oplog.entries = append(oplog.entries, OperationLogEntry{
			opcode:   OpAssert,
			operands: OperationOperands{lsn: []int{tag}, value: []uint256.Int{value}},
			result:   value,
			redo:     false,
		})
	}
}

func (oplog *OperationLog) AppendUnaryArithmeticLog(opcode OperationCode, x, y uint256.Int, xtag int) int {
	if xtag == 0 {
		return 0
	}
	currentLSN := len(oplog.entries)
	entry := OperationLogEntry{
		opcode:   opcode,
		operands: OperationOperands{lsn: []int{xtag}, value: []uint256.Int{x}},
		result:   y,
		redo:     false,
	}
	oplog.entries = append(oplog.entries, entry)
	return currentLSN
}

func (oplog *OperationLog) AppendBinaryArithmeticLog(opcode OperationCode, x, y, z uint256.Int, xtag, ytag int) int {
	if xtag == 0 && ytag == 0 {
		return 0
	}
	currentLSN := len(oplog.entries)
	entry := OperationLogEntry{
		opcode:   opcode,
		operands: OperationOperands{lsn: []int{xtag, ytag}, value: []uint256.Int{x, y}},
		result:   z,
		redo:     false,
	}
	oplog.entries = append(oplog.entries, entry)
	return currentLSN
}

func (oplog *OperationLog) AppendTripleArithmeticLog(opcode OperationCode, x, y, z, result uint256.Int, xtag, ytag, ztag int) int {
	if xtag == 0 && ytag == 0 && ztag == 0 {
		return 0
	}
	currentLSN := len(oplog.entries)
	entry := OperationLogEntry{
		opcode:   opcode,
		operands: OperationOperands{lsn: []int{xtag, ytag, ztag}, value: []uint256.Int{x, y, z}},
		result:   result,
		redo:     false,
	}
	oplog.entries = append(oplog.entries, entry)
	return currentLSN
}

func (oplog *OperationLog) AppendAddLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpAdd, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendSubLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpSub, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendMulLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpMul, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendDivLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpDiv, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendSdivLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpSdiv, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendModLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpMod, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendSmodLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpSmod, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendExpLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpExp, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendSignExtendLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpSignExtend, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendLTLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpLT, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendGTLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpGT, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendSLTLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpSLT, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendSGTLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpSGT, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendEQLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpEQ, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendAndLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpAnd, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendOrLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpOr, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendXorLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpXor, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendShlLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpShl, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendShrLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpShr, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendSarLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpSar, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendByteLog(x, y, z uint256.Int, xtag, ytag int) int {
	return oplog.AppendBinaryArithmeticLog(OpByte, x, y, z, xtag, ytag)
}
func (oplog *OperationLog) AppendIsZeroLog(x, y uint256.Int, xtag int) int {
	return oplog.AppendUnaryArithmeticLog(OpIsZero, x, y, xtag)
}
func (oplog *OperationLog) AppendNotLog(x, y uint256.Int, xtag int) int {
	return oplog.AppendUnaryArithmeticLog(OpNot, x, y, xtag)
}
func (oplog *OperationLog) AppendAddmodLog(x, y, z, result uint256.Int, xtag, ytag, ztag int) int {
	return oplog.AppendTripleArithmeticLog(OpAddmod, x, y, z, result, xtag, ytag, ztag)
}
func (oplog *OperationLog) AppendMulmodLog(x, y, z, result uint256.Int, xtag, ytag, ztag int) int {
	return oplog.AppendTripleArithmeticLog(OpMulmod, x, y, z, result, xtag, ytag, ztag)
}

func (oplog *OperationLog) checkRedo(index int) bool {
	if oplog.entries[index].redo {
		return true
	}
	for _, parent := range oplog.entries[index].operands.lsn {
		if parent != 0 && oplog.entries[parent].redo {
			return true
		}
	}
	return false
}

func (oplog *OperationLog) updateOperand(index int) {
	for i := 0; i < len(oplog.entries[index].operands.lsn); i++ {
		if parent := oplog.entries[index].operands.lsn[i]; parent != 0 && oplog.entries[parent].redo {
			oplog.entries[index].operands.value[i] = oplog.entries[parent].result
		}
	}
}

func (oplog *OperationLog) redoOperation(index int, redoStorage map[common.Address]Storage) bool {
	entry := &oplog.entries[index]
	entry.redo = true
	oplog.updateOperand(index)
	switch entry.opcode {
	case OpAdd:
		entry.result.Add(&entry.operands.value[0], &entry.operands.value[1])
	case OpSub:
		entry.result.Sub(&entry.operands.value[0], &entry.operands.value[1])
	case OpMul:
		entry.result.Mul(&entry.operands.value[0], &entry.operands.value[1])
	case OpDiv:
		entry.result.Div(&entry.operands.value[0], &entry.operands.value[1])
	case OpSdiv:
		entry.result.SDiv(&entry.operands.value[0], &entry.operands.value[1])
	case OpMod:
		entry.result.Mod(&entry.operands.value[0], &entry.operands.value[1])
	case OpSmod:
		entry.result.SMod(&entry.operands.value[0], &entry.operands.value[1])
	case OpAddmod:
		entry.result.AddMod(&entry.operands.value[0], &entry.operands.value[1], &entry.operands.value[2])
	case OpMulmod:
		entry.result.MulMod(&entry.operands.value[0], &entry.operands.value[1], &entry.operands.value[2])
	case OpExp:
		entry.result.Exp(&entry.operands.value[0], &entry.operands.value[1])
	case OpSignExtend:
		entry.result.ExtendSign(&entry.operands.value[1], &entry.operands.value[0])
	case OpLT:
		prev_result := entry.result
		if entry.operands.value[0].Lt(&entry.operands.value[1]) {
			entry.result.SetOne()
		} else {
			entry.result.Clear()
		}
		if prev_result.Eq(&entry.result) {
			entry.redo = false
		}
	case OpGT:
		prev_result := entry.result
		if entry.operands.value[0].Gt(&entry.operands.value[1]) {
			entry.result.SetOne()
		} else {
			entry.result.Clear()
		}
		if prev_result.Eq(&entry.result) {
			entry.redo = false
		}
	case OpSLT:
		prev_result := entry.result
		if entry.operands.value[0].Slt(&entry.operands.value[1]) {
			entry.result.SetOne()
		} else {
			entry.result.Clear()
		}
		if prev_result.Eq(&entry.result) {
			entry.redo = false
		}
	case OpSGT:
		prev_result := entry.result
		if entry.operands.value[0].Sgt(&entry.operands.value[1]) {
			entry.result.SetOne()
		} else {
			entry.result.Clear()
		}
		if prev_result.Eq(&entry.result) {
			entry.redo = false
		}
	case OpEQ:
		prev_result := entry.result
		if entry.operands.value[0].Eq(&entry.operands.value[1]) {
			entry.result.SetOne()
		} else {
			entry.result.Clear()
		}
		if prev_result.Eq(&entry.result) {
			entry.redo = false
		}
	case OpIsZero:
		prev_result := entry.result
		if entry.operands.value[0].IsZero() {
			entry.result.SetOne()
		} else {
			entry.result.Clear()
		}
		if prev_result.Eq(&entry.result) {
			entry.redo = false
		}
	case OpAnd:
		entry.result.And(&entry.operands.value[0], &entry.operands.value[1])
	case OpOr:
		entry.result.Or(&entry.operands.value[0], &entry.operands.value[1])
	case OpXor:
		entry.result.Xor(&entry.operands.value[0], &entry.operands.value[1])
	case OpNot:
		entry.result.Not(&entry.operands.value[0])
	case OpByte:
		entry.result.Set(&entry.operands.value[1]).Byte(&entry.operands.value[0])
	case OpShl:
		shift := &entry.operands.value[0]
		value := &entry.operands.value[1]
		if shift.LtUint64(256) {
			entry.result.Lsh(value, uint(shift.Uint64()))
		} else {
			entry.result.Clear()
		}
	case OpShr:
		shift := &entry.operands.value[0]
		value := &entry.operands.value[1]
		if shift.LtUint64(256) {
			entry.result.Rsh(value, uint(shift.Uint64()))
		} else {
			entry.result.Clear()
		}
	case OpSar:
		shift := &entry.operands.value[0]
		value := &entry.operands.value[1]
		if shift.GtUint64(256) {
			if value.Sign() >= 0 {
				entry.result.Clear()
			} else {
				entry.result.SetAllOne()
			}
		} else {
			entry.result.SRsh(value, uint(shift.Uint64()))
		}
	case OpAssert:
		return entry.result.Eq(&entry.operands.value[0])
	case OpSload:
		// No Action
	case OpSstore:
		entry.result.Set(&entry.operands.value[0])
		addr := common.Address(entry.operands.value[1].Bytes20())
		key := common.Hash(entry.operands.value[2].Bytes32())
		accountMap := redoStorage[addr]
		if accountMap == nil {
			accountMap = make(map[common.Hash]common.Hash)
			redoStorage[addr] = accountMap
		}
		accountMap[key] = entry.result.Bytes32()
	case OpSstoreGasV1:
		newgas, newrefund := gasSStoreV1(entry.operands.value[0].Bytes32(),
			&entry.operands.value[1])
		if newgas != entry.result[0] || newrefund != entry.result[1] {
			return false
		}
	case OpSstoreGasV2:
		newgas, newrefund := gasSStoreV2(entry.operands.value[0].Bytes32(),
			entry.operands.value[1].Bytes32(), entry.operands.value[2].Bytes32())
		if newgas != entry.result[0] || newrefund != entry.result[1] {
			return false
		}
	case OpSstoreGasV3:
		newgas, newrefund := gasSStoreV3(entry.operands.value[0].Bytes32(),
			entry.operands.value[1].Bytes32(), entry.operands.value[2].Bytes32(), entry.result[2])
		if newgas != entry.result[0] || newrefund != entry.result[1] {
			return false
		}
	case OpSstoreGasV4:
		newgas, newrefund := gasSStoreV4(entry.operands.value[0].Bytes32(),
			entry.operands.value[1].Bytes32(), entry.operands.value[2].Bytes32())
		if newgas != entry.result[0] || newrefund != entry.result[1] {
			return false
		}
	default:
		panic("unreachable")
	}
	return true
}

func (oplog *OperationLog) updateCommittedState(
	conflictStorage map[common.Address]map[common.Hash]common.Hash) {
	for addr, conflictKeyMap := range conflictStorage {
		lsnMap := oplog.readCommittedLSN[addr]
		for key, value := range conflictKeyMap {
			for _, lsn := range lsnMap[key] {
				entry := &oplog.entries[lsn]
				entry.redo = true
				switch entry.opcode {
				case OpSload:
					entry.result.SetBytes(value.Bytes())
				case OpSstoreGasV1:
					entry.operands.value[0].SetBytes(value.Bytes())
				case OpSstoreGasV2, OpSstoreGasV3, OpSstoreGasV4:
					entry.operands.value[0].SetBytes(value.Bytes())
					if entry.operands.lsn[1] == 0 && entry.result[3] == 0 {
						entry.operands.value[1].SetBytes(value.Bytes())
					}
				default:
					panic("unreachable")
				}
			}
		}
	}
}

func (oplog *OperationLog) Redo(conflictStorage map[common.Address]map[common.Hash]common.Hash) (bool, map[common.Address]Storage) {
	oplog.updateCommittedState(conflictStorage)
	redoStorage := make(map[common.Address]Storage)
	for i := 1; i < len(oplog.entries); i++ {
		if !oplog.checkRedo(i) {
			continue
		}
		if !oplog.redoOperation(i, redoStorage) {
			return false, nil
		}
	}
	return true, redoStorage
}

func gasSStoreV1(current common.Hash, y *uint256.Int) (uint64, uint64) {
	switch {
	case current == (common.Hash{}) && y.Sign() != 0: // 0 => non 0
		return params.SstoreSetGas, 0
	case current != (common.Hash{}) && y.Sign() == 0: // non 0 => 0
		return params.SstoreClearGas, params.SstoreRefundGas
	default: // non 0 => non 0 (or 0 => 0)
		return params.SstoreResetGas, 0
	}
}

func gasSStoreV2(original, current, value common.Hash) (uint64, uint64) {
	refund := uint64(0)
	if current == value {
		return params.NetSstoreNoopGas, 0
	}
	if original == current {
		if original == (common.Hash{}) {
			return params.NetSstoreInitGas, 0
		}
		if value == (common.Hash{}) {
			refund = params.NetSstoreClearRefund
		}
		return params.NetSstoreCleanGas, refund
	}
	if original != (common.Hash{}) {
		if current == (common.Hash{}) {
			refund += ^params.NetSstoreClearRefund + 1
		} else if value == (common.Hash{}) {
			refund += params.NetSstoreClearRefund
		}
	}
	if original == value {
		if original == (common.Hash{}) {
			refund += params.NetSstoreResetClearRefund
		} else {
			refund += params.NetSstoreResetRefund
		}
	}
	return params.NetSstoreDirtyGas, refund
}

func gasSStoreV3(original, current, value common.Hash, clearingRefund uint64) (uint64, uint64) {
	refund := uint64(0)
	if current == value {
		return params.WarmStorageReadCostEIP2929, 0
	}
	if original == current {
		if original == (common.Hash{}) {
			return params.SstoreSetGasEIP2200, 0
		}
		if value == (common.Hash{}) {
			refund = clearingRefund
		}
		return (params.SstoreResetGasEIP2200 - params.ColdSloadCostEIP2929), refund
	}
	if original != (common.Hash{}) {
		if current == (common.Hash{}) {
			refund += ^clearingRefund + 1
		} else if value == (common.Hash{}) {
			refund += clearingRefund
		}
	}
	if original == value {
		if original == (common.Hash{}) {
			refund += params.SstoreSetGasEIP2200 - params.WarmStorageReadCostEIP2929
		} else {
			refund += (params.SstoreResetGasEIP2200 - params.ColdSloadCostEIP2929) - params.WarmStorageReadCostEIP2929
		}
	}
	return params.WarmStorageReadCostEIP2929, refund
}

func gasSStoreV4(original, current, value common.Hash) (uint64, uint64) {
	refund := uint64(0)
	if current == value {
		return params.SloadGasEIP2200, 0
	}
	if original == current {
		if original == (common.Hash{}) {
			return params.SstoreSetGasEIP2200, 0
		}
		if value == (common.Hash{}) {
			refund = params.SstoreClearsScheduleRefundEIP2200
		}
		return params.SstoreResetGasEIP2200, refund
	}
	if original != (common.Hash{}) {
		if current == (common.Hash{}) {
			refund = ^params.SstoreClearsScheduleRefundEIP2200 + 1
		} else if value == (common.Hash{}) {
			refund = params.SstoreClearsScheduleRefundEIP2200
		}
	}
	if original == value {
		if original == (common.Hash{}) {
			refund += params.SstoreSetGasEIP2200 - params.SloadGasEIP2200
		} else {
			refund += params.SstoreResetGasEIP2200 - params.SloadGasEIP2200
		}
	}
	return params.SloadGasEIP2200, refund
}

var OpName = [...]string{
	"OpEmpty",
	"OpAdd",
	"OpSub",
	"OpMul",
	"OpDiv",
	"OpSdiv",
	"OpMod",
	"OpSmod",
	"OpAddmod",
	"OpMulmod",
	"OpExp",
	"OpSignExtend",
	"OpLT",
	"OpGT",
	"OpSLT",
	"OpSGT",
	"OpEQ",
	"OpIsZero",
	"OpAnd",
	"OpOr",
	"OpXor",
	"OpNot",
	"OpByte",
	"OpShl",
	"OpShr",
	"OpSar",
	"OpSload",
	"OpSstore",
	"OpSstoreGasV1",
	"OpSstoreGasV2",
	"OpSstoreGasV3",
	"OpSstoreGasV4",
	"OpAssert",
}
