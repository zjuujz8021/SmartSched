package state

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

type RedoContext struct {
	state                          *MVCCStateDB
	IsBalanceConflict              bool
	IsNonceConflict                bool
	IsStorageConflict              bool
	IsUnrecoverableStorageConflict bool
	IsCodeConflict                 bool

	BalanceLogs     []BalanceLog
	DirtyBalance    map[common.Address]struct{}
	NonceLogs       []NonceLog
	DirtyNonce      map[common.Address]struct{}
	OperationLogs   *OperationLog
	ConflictStorage map[common.Address]map[common.Hash]common.Hash

	IsRevert bool
	// validRevision []redoCtxRev
}

// type redoCtxRev struct {
// 	id           int
// 	balanceIndex int
// 	nonceIndex   int
// }

type RedoDebugInfo struct {
	TxnIndex    int
	BalanceLogs []BalanceDebugInfo
}

type ValidateResult int

const (
	Pass ValidateResult = iota
	ConflictNonce
	ConflictBalance
	ConflictStorage
	ConflictUnrecoverableStorage
	ConflictCode
)

func NewRedoContext(state *MVCCStateDB) *RedoContext {
	return &RedoContext{
		state:         state,
		DirtyBalance:  make(map[common.Address]struct{}),
		DirtyNonce:    make(map[common.Address]struct{}),
		OperationLogs: newOperationLog(),
	}
}

func (ctx *RedoContext) Snapshot(id int) {
	// ctx.validRevision = append(ctx.validRevision, redoCtxRev{
	// 	id:           id,
	// 	balanceIndex: len(ctx.BalanceLogs),
	// 	nonceIndex:   len(ctx.NonceLogs),
	// })
}

func (ctx *RedoContext) RevertToSnapshot(revid int) {
	ctx.IsRevert = true
	// idx := sort.Search(len(ctx.validRevision), func(i int) bool {
	// 	return ctx.validRevision[i].id >= revid
	// })
	// ctx.BalanceLogs = ctx.BalanceLogs[:ctx.validRevision[idx].balanceIndex]
	// ctx.NonceLogs = ctx.NonceLogs[:ctx.validRevision[idx].nonceIndex]
}

func (ctx *RedoContext) IsSimplePass() bool {
	return !ctx.IsBalanceConflict && !ctx.IsCodeConflict && !ctx.IsNonceConflict &&
		!ctx.IsStorageConflict && !ctx.IsUnrecoverableStorageConflict
}

func (ctx *RedoContext) Redo() ValidateResult {
	if ctx.IsCodeConflict {
		return ConflictCode
	} else if ctx.IsUnrecoverableStorageConflict {
		return ConflictUnrecoverableStorage
	} else if !ctx.RedoOperationLog() {
		return ConflictStorage
	} else if !ctx.RedoBalanceLog() {
		return ConflictBalance
	} else if !ctx.RedoNonceLog() {
		return ConflictNonce
	}
	return Pass
}

func (ctx *RedoContext) Report() *RedoDebugInfo {
	debugInfo := &RedoDebugInfo{TxnIndex: ctx.state.txIndex}
	for _, entry := range ctx.BalanceLogs {
		debugInfo.BalanceLogs = append(debugInfo.BalanceLogs, entry.report())
	}
	return debugInfo
}

// Balance Log
// LHR-TODO: consider how to deal with revert
type (
	GetBalanceFunc func(addr common.Address) *uint256.Int
	SetBalanceFunc func(addr common.Address, value *uint256.Int)
	BalanceLog     interface {
		redo(GetBalance GetBalanceFunc, SetBalance SetBalanceFunc) bool
		report() BalanceDebugInfo
	}
	BalanceConstraint interface {
		redo(GetBalance GetBalanceFunc) bool
		report() BalanceDebugInfo
	}
)

type TransferBalance struct {
	from, to common.Address
	value    *uint256.Int
}

type BalanceDebugInfo struct {
	Optype string
	From   string
	To     string
	Value  string
}

func (l TransferBalance) redo(GetBalance GetBalanceFunc, SetBalance SetBalanceFunc) bool {
	fromBalance := GetBalance(l.from)
	if fromBalance.Cmp(l.value) < 0 {
		return false
	}
	if l.from != l.to {
		toBalance := GetBalance(l.to)
		SetBalance(l.from, new(uint256.Int).Sub(fromBalance, l.value))
		SetBalance(l.to, new(uint256.Int).Add(toBalance, l.value))
	}
	return true
}

func (l TransferBalance) report() BalanceDebugInfo {
	return BalanceDebugInfo{Optype: "TransferBalance", From: l.from.Hex(), To: l.to.Hex(), Value: l.value.String()}
}

type AddBalance struct {
	from  common.Address
	value *uint256.Int
}

func (l AddBalance) redo(GetBalance GetBalanceFunc, SetBalance SetBalanceFunc) bool {
	fromBalance := GetBalance(l.from)
	SetBalance(l.from, new(uint256.Int).Add(fromBalance, l.value))
	return true
}

func (l AddBalance) report() BalanceDebugInfo {
	return BalanceDebugInfo{Optype: "AddBalance", From: l.from.Hex(), Value: l.value.String()}
}

type SubBalance struct {
	from  common.Address
	value *uint256.Int
}

func (l SubBalance) redo(GetBalance GetBalanceFunc, SetBalance SetBalanceFunc) bool {
	fromBalance := GetBalance(l.from)
	if fromBalance.Cmp(l.value) < 0 {
		return false
	}
	SetBalance(l.from, new(uint256.Int).Sub(fromBalance, l.value))
	return true
}

func (l SubBalance) report() BalanceDebugInfo {
	return BalanceDebugInfo{Optype: "SubBalance", From: l.from.Hex(), Value: l.value.String()}
}

type AssertEnoughBalance struct {
	from  common.Address
	value *uint256.Int
}

func (l AssertEnoughBalance) redo(GetBalance GetBalanceFunc, SetBalance SetBalanceFunc) bool {
	return GetBalance(l.from).Cmp(l.value) >= 0
}

func (l AssertEnoughBalance) report() BalanceDebugInfo {
	return BalanceDebugInfo{Optype: "AssertEnoughBalance", From: l.from.Hex(), Value: l.value.String()}
}

type AssertBalanceLess struct {
	from  common.Address
	value *uint256.Int
}

func (l AssertBalanceLess) redo(GetBalance GetBalanceFunc, SetBalance SetBalanceFunc) bool {
	return GetBalance(l.from).Cmp(l.value) < 0
}

func (l AssertBalanceLess) report() BalanceDebugInfo {
	return BalanceDebugInfo{Optype: "AssertBalanceLess", From: l.from.Hex(), Value: l.value.String()}
}

type AssertBalanceEqual struct {
	from  common.Address
	value *uint256.Int
}

func (l AssertBalanceEqual) redo(GetBalance GetBalanceFunc, SetBalance SetBalanceFunc) bool {
	return GetBalance(l.from).Cmp(l.value) == 0
}

func (l AssertBalanceEqual) report() BalanceDebugInfo {
	return BalanceDebugInfo{Optype: "AssertBalanceEqual", From: l.from.Hex(), Value: l.value.String()}
}

type AssertBalanceZero struct {
	from common.Address
}

func (l AssertBalanceZero) redo(GetBalance GetBalanceFunc, SetBalance SetBalanceFunc) bool {
	return GetBalance(l.from).Sign() == 0
}

func (l AssertBalanceZero) report() BalanceDebugInfo {
	return BalanceDebugInfo{Optype: "AssertBalanceZero", From: l.from.Hex()}
}

type AssertBalanceNonZero struct {
	from common.Address
}

func (l AssertBalanceNonZero) redo(GetBalance GetBalanceFunc, SetBalance SetBalanceFunc) bool {
	return GetBalance(l.from).Sign() != 0
}

func (l AssertBalanceNonZero) report() BalanceDebugInfo {
	return BalanceDebugInfo{Optype: "AssertBalanceNonZero", From: l.from.Hex()}
}

func (ctx *RedoContext) AppendTransferBalanceLog(from, to common.Address, value *uint256.Int) {
	ctx.BalanceLogs = append(ctx.BalanceLogs, TransferBalance{from: from, to: to, value: new(uint256.Int).Set(value)})
	ctx.DirtyBalance[from] = struct{}{}
	ctx.DirtyBalance[to] = struct{}{}
}

func (ctx *RedoContext) AppendAddBalanceLog(from common.Address, value *uint256.Int) {
	ctx.BalanceLogs = append(ctx.BalanceLogs, AddBalance{from: from, value: new(uint256.Int).Set(value)})
	ctx.DirtyBalance[from] = struct{}{}
}

func (ctx *RedoContext) AppendSubBalanceLog(from common.Address, value *uint256.Int) {
	ctx.BalanceLogs = append(ctx.BalanceLogs, SubBalance{from: from, value: new(uint256.Int).Set(value)})
	ctx.DirtyBalance[from] = struct{}{}
}

func (ctx *RedoContext) AppendAssertBalanceEqualLog(from common.Address, value *uint256.Int) {
	ctx.BalanceLogs = append(ctx.BalanceLogs, AssertBalanceEqual{from: from, value: new(uint256.Int).Set(value)})
}

func (ctx *RedoContext) AppendAssertEnoughBalanceLog(from common.Address, value *uint256.Int) {
	ctx.BalanceLogs = append(ctx.BalanceLogs, AssertEnoughBalance{from: from, value: new(uint256.Int).Set(value)})
}

func (ctx *RedoContext) AppendAssertBalanceLessLog(from common.Address, value *uint256.Int) {
	ctx.BalanceLogs = append(ctx.BalanceLogs, AssertBalanceLess{from: from, value: new(uint256.Int).Set(value)})
}

func (ctx *RedoContext) AppendAssertBalanceZeroLog(from common.Address) {
	ctx.BalanceLogs = append(ctx.BalanceLogs, AssertBalanceZero{from: from})
}

func (ctx *RedoContext) AppendAssertBalanceNonZeroLog(from common.Address) {
	ctx.BalanceLogs = append(ctx.BalanceLogs, AssertBalanceNonZero{from: from})
}

func (ctx *RedoContext) RedoBalanceLog() bool {
	if ctx.IsRevert {
		return false
	}
	if !ctx.IsBalanceConflict {
		return true
	}
	balanceMap := make(map[common.Address]*uint256.Int)
	getBalance := func(addr common.Address) *uint256.Int {
		if balance, exist := balanceMap[addr]; exist {
			return balance
		} else if obj := ctx.state.baseStateDB.getStateObject(addr); obj != nil {
			// it is safe to call statedb.getStateObject without holding the lock becuase
			// only current worker is in validation phase and no other worker can write to the statedb
			balanceMap[addr] = obj.getBalance()
			return obj.getBalance()
		}
		return uint256.NewInt(0)
	}
	setBalance := func(addr common.Address, value *uint256.Int) {
		balanceMap[addr] = value
	}

	for _, entry := range ctx.BalanceLogs {
		if !entry.redo(getBalance, setBalance) {
			return false
		}
	}
	for addr, balance := range balanceMap {
		ctx.state.SetBalance(addr, balance)
	}
	// corner case: incorrect balance origin but logs are reverted
	if len(balanceMap) != len(ctx.DirtyBalance) {
		for addr := range ctx.DirtyBalance {
			if _, exist := balanceMap[addr]; !exist {
				ctx.state.revertDirtyBalance(addr)
			}
		}
	}
	return true
}

// Nonce Log
type (
	GetNonceFunc func(addr common.Address) uint64
	SetNonceFunc func(addr common.Address, value uint64)
	NonceLog     interface {
		redo(GetNonce GetNonceFunc, SetNonce SetNonceFunc) bool
	}
)

type NonceInc common.Address

func (l NonceInc) redo(GetNonce GetNonceFunc, SetNonce SetNonceFunc) bool {
	addr := common.Address(l)
	nonce := GetNonce(addr)
	if nonce > nonce+1 {
		return false
	}
	SetNonce(addr, nonce+1)
	return true
}

type NonceEqual struct {
	addr   common.Address
	expect uint64
}

func (l NonceEqual) redo(GetNonce GetNonceFunc, SetNonce SetNonceFunc) bool {
	return GetNonce(l.addr) == l.expect
}

type NonceNotEqual struct {
	addr   common.Address
	expect uint64
}

func (l NonceNotEqual) redo(GetNonce GetNonceFunc, SetNonce SetNonceFunc) bool {
	return GetNonce(l.addr) != l.expect
}

func (ctx *RedoContext) AppendNonceIncLog(addr common.Address) {
	ctx.NonceLogs = append(ctx.NonceLogs, NonceInc(addr))
	ctx.DirtyNonce[addr] = struct{}{}
}

func (ctx *RedoContext) AppendNonceEqualLog(addr common.Address, expect uint64) {
	ctx.NonceLogs = append(ctx.NonceLogs, NonceEqual{addr: addr, expect: expect})
}

func (ctx *RedoContext) AppendNonceNotEqualLog(addr common.Address, expect uint64) {
	ctx.NonceLogs = append(ctx.NonceLogs, NonceNotEqual{addr: addr, expect: expect})
}

func (ctx *RedoContext) RedoNonceLog() bool {
	if ctx.IsRevert {
		return false
	}
	if !ctx.IsNonceConflict {
		return true
	}
	nonceMap := make(map[common.Address]uint64)
	getNonce := func(addr common.Address) uint64 {
		if nonce, exist := nonceMap[addr]; exist {
			return nonce
		} else if obj := ctx.state.baseStateDB.getStateObject(addr); obj != nil {
			nonceMap[addr] = obj.getNonce()
			return obj.getNonce()
		}
		return 0
	}
	setNonce := func(addr common.Address, value uint64) {
		nonceMap[addr] = value
	}
	for _, entry := range ctx.NonceLogs {
		if !entry.redo(getNonce, setNonce) {
			return false
		}
	}
	for addr, nonce := range nonceMap {
		ctx.state.SetNonce(addr, nonce)
	}
	if len(nonceMap) != len(ctx.DirtyNonce) {
		for addr := range ctx.DirtyNonce {
			if _, exist := nonceMap[addr]; !exist {
				ctx.state.revertDirtyNonce(addr)
			}
		}
	}
	return true
}

func (ctx *RedoContext) RedoOperationLog() bool {
	if ctx.IsRevert {
		return false
	}
	if !ctx.IsStorageConflict {
		return true
	}
	if ok, redoStorage := ctx.OperationLogs.Redo(ctx.ConflictStorage); ok {
		for addr, storage := range redoStorage {
			for key, value := range storage {
				ctx.state.SetState(addr, key, value)
			}
		}
		return true
	}
	return false
}
