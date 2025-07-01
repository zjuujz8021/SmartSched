// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"math"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

func opAdd(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	y.Add(&x, y)
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendAddLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opSub(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	y.Sub(&x, y)
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendSubLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opMul(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	y.Mul(&x, y)
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendMulLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opDiv(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	y.Div(&x, y)
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendDivLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opSdiv(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	y.SDiv(&x, y)
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendSdivLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opMod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	y.Mod(&x, y)
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendModLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opSmod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	y.SMod(&x, y)
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendSmodLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opExp(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	base, exponent := scope.Stack.pop(), scope.Stack.peek()
	base_tag, exp_tag := scope.Stack.popTag(), scope.Stack.peekTag()
	exp_copy, exp_tag_copy := *exponent, *exp_tag
	exponent.Exp(&base, exponent)
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(exp_tag_copy, exp_copy)
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendExpLog(base, exp_copy, *exponent, base_tag, exp_tag_copy)
		*exp_tag = new_tag
	}
	return nil, nil
}

func opSignExtend(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	back, num := scope.Stack.pop(), scope.Stack.peek()
	back_tag, num_tag := scope.Stack.popTag(), scope.Stack.peekTag()
	num_copy, num_tag_copy := *num, *num_tag
	num.ExtendSign(num, &back)
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendSignExtendLog(back, num_copy, *num, back_tag, num_tag_copy)
		*num_tag = new_tag
	}
	return nil, nil
}

func opNot(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x := scope.Stack.peek()
	xtag := scope.Stack.peekTag()
	x_copy, xtag_copy := *x, *xtag
	x.Not(x)
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendNotLog(x_copy, *x, xtag_copy)
		*xtag = new_tag
	}
	return nil, nil
}

func opLt(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	if x.Lt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendLTLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opGt(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	if x.Gt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendGTLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opSlt(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	if x.Slt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendSLTLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opSgt(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	if x.Sgt(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendSGTLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opEq(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	if x.Eq(y) {
		y.SetOne()
	} else {
		y.Clear()
	}
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendEQLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opIszero(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x := scope.Stack.peek()
	xtag := scope.Stack.peekTag()
	x_copy, xtag_copy := *x, *xtag
	if x.IsZero() {
		x.SetOne()
	} else {
		x.Clear()
	}
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendIsZeroLog(x_copy, *x, xtag_copy)
		*xtag = new_tag
	}
	return nil, nil
}

func opAnd(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	y.And(&x, y)
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendAndLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opOr(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	y.Or(&x, y)
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendOrLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opXor(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y := scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag := scope.Stack.popTag(), scope.Stack.peekTag()
	y_copy, ytag_copy := *y, *ytag
	y.Xor(&x, y)
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendXorLog(x, y_copy, *y, xtag, ytag_copy)
		*ytag = new_tag
	}
	return nil, nil
}

func opByte(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	th, val := scope.Stack.pop(), scope.Stack.peek()
	th_tag, val_tag := scope.Stack.popTag(), scope.Stack.peekTag()
	val_copy, val_tag_copy := *val, *val_tag
	val.Byte(&th)
	if interpreter.evm.Config.EnableRedo {
		new_tag := interpreter.evm.RedoContext.OperationLogs.AppendByteLog(th, val_copy, *val, th_tag, val_tag_copy)
		*val_tag = new_tag
	}
	return nil, nil
}

func opAddmod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y, z := scope.Stack.pop(), scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag, ztag := scope.Stack.popTag(), scope.Stack.popTag(), scope.Stack.peekTag()
	z_copy, ztag_copy := *z, *ztag
	if z.IsZero() {
		z.Clear()
	} else {
		z.AddMod(&x, &y, z)
	}
	if interpreter.evm.Config.EnableRedo {
		newtag := interpreter.evm.RedoContext.OperationLogs.AppendAddmodLog(x, y, z_copy, *z, xtag, ytag, ztag_copy)
		*ztag = newtag
	}
	return nil, nil
}

func opMulmod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x, y, z := scope.Stack.pop(), scope.Stack.pop(), scope.Stack.peek()
	xtag, ytag, ztag := scope.Stack.popTag(), scope.Stack.popTag(), scope.Stack.peekTag()
	z_copy, ztag_copy := *z, *ztag
	z.MulMod(&x, &y, z)
	if interpreter.evm.Config.EnableRedo {
		newtag := interpreter.evm.RedoContext.OperationLogs.AppendAddmodLog(x, y, z_copy, *z, xtag, ytag, ztag_copy)
		*ztag = newtag
	}
	return nil, nil
}

// opSHL implements Shift Left
// The SHL instruction (shift left) pops 2 values from the stack, first arg1 and then arg2,
// and pushes on the stack arg2 shifted to the left by arg1 number of bits.
func opSHL(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	// Note, second operand is left in the stack; accumulate result into it, and no need to push it afterwards
	shift, value := scope.Stack.pop(), scope.Stack.peek()
	shift_tag, value_tag := scope.Stack.popTag(), scope.Stack.peekTag()
	value_copy, value_tag_copy := *value, *value_tag
	if shift.LtUint64(256) {
		value.Lsh(value, uint(shift.Uint64()))
	} else {
		value.Clear()
	}
	if interpreter.evm.Config.EnableRedo {
		newtag := interpreter.evm.RedoContext.OperationLogs.AppendShlLog(shift, value_copy, *value, shift_tag, value_tag_copy)
		*value_tag = newtag
	}
	return nil, nil
}

// opSHR implements Logical Shift Right
// The SHR instruction (logical shift right) pops 2 values from the stack, first arg1 and then arg2,
// and pushes on the stack arg2 shifted to the right by arg1 number of bits with zero fill.
func opSHR(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	// Note, second operand is left in the stack; accumulate result into it, and no need to push it afterwards
	shift, value := scope.Stack.pop(), scope.Stack.peek()
	shift_tag, value_tag := scope.Stack.popTag(), scope.Stack.peekTag()
	value_copy, value_tag_copy := *value, *value_tag
	if shift.LtUint64(256) {
		value.Rsh(value, uint(shift.Uint64()))
	} else {
		value.Clear()
	}
	if interpreter.evm.Config.EnableRedo {
		newtag := interpreter.evm.RedoContext.OperationLogs.AppendShrLog(shift, value_copy, *value, shift_tag, value_tag_copy)
		*value_tag = newtag
	}
	return nil, nil
}

// opSAR implements Arithmetic Shift Right
// The SAR instruction (arithmetic shift right) pops 2 values from the stack, first arg1 and then arg2,
// and pushes on the stack arg2 shifted to the right by arg1 number of bits with sign extension.
func opSAR(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	shift, value := scope.Stack.pop(), scope.Stack.peek()
	shift_tag, value_tag := scope.Stack.popTag(), scope.Stack.peekTag()
	value_copy, value_tag_copy := *value, *value_tag
	if shift.GtUint64(256) {
		if value.Sign() >= 0 {
			value.Clear()
		} else {
			// Max negative shift: all bits set
			value.SetAllOne()
		}
		return nil, nil
	}
	n := uint(shift.Uint64())
	value.SRsh(value, n)
	if interpreter.evm.Config.EnableRedo {
		newtag := interpreter.evm.RedoContext.OperationLogs.AppendShrLog(shift, value_copy, *value, shift_tag, value_tag_copy)
		*value_tag = newtag
	}
	return nil, nil
}

func opKeccak256(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	offset, size := scope.Stack.pop(), scope.Stack.peek()
	offset_tag, size_tag := scope.Stack.popTag(), scope.Stack.peekTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(offset_tag, offset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(*size_tag, *size)
		*size_tag = 0
	}

	data := scope.Memory.GetPtr(int64(offset.Uint64()), int64(size.Uint64()))

	if interpreter.hasher == nil {
		interpreter.hasher = crypto.NewKeccakState()
	} else {
		interpreter.hasher.Reset()
	}
	interpreter.hasher.Write(data)
	interpreter.hasher.Read(interpreter.hasherBuf[:])

	evm := interpreter.evm
	if evm.Config.EnablePreimageRecording {
		evm.StateDB.AddPreimage(interpreter.hasherBuf, data)
	}

	size.SetBytes(interpreter.hasherBuf[:])
	return nil, nil
}

func opAddress(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetBytes(scope.Contract.Address().Bytes()))
	return nil, nil
}

func opBalance(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	slot := scope.Stack.peek()
	slotTag := scope.Stack.peekTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(*slotTag, *slot)
		*slotTag = 0
	}
	address := common.Address(slot.Bytes20())
	value := interpreter.evm.StateDB.GetBalance(address)
	slot.Set(value)
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.AppendAssertBalanceEqualLog(address, value)
	}
	return nil, nil
}

func opOrigin(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetBytes(interpreter.evm.Origin.Bytes()))
	return nil, nil
}

func opCaller(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetBytes(scope.Contract.Caller().Bytes()))
	return nil, nil
}

func opCallValue(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(scope.Contract.value)
	return nil, nil
}

func opCallDataLoad(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	x := scope.Stack.peek()
	xtag := scope.Stack.peekTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(*xtag, *x)
		*xtag = 0
	}
	if offset, overflow := x.Uint64WithOverflow(); !overflow {
		data := getData(scope.Contract.Input, offset, 32)
		x.SetBytes(data)
	} else {
		x.Clear()
	}
	return nil, nil
}

func opCallDataSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetUint64(uint64(len(scope.Contract.Input))))
	return nil, nil
}

func opCallDataCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		memOffset     = scope.Stack.pop()
		dataOffset    = scope.Stack.pop()
		length        = scope.Stack.pop()
		memOffsetTag  = scope.Stack.popTag()
		dataOffsetTag = scope.Stack.popTag()
		lengthTag     = scope.Stack.popTag()
	)
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(memOffsetTag, memOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(dataOffsetTag, dataOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(lengthTag, length)
	}

	dataOffset64, overflow := dataOffset.Uint64WithOverflow()
	if overflow {
		dataOffset64 = 0xffffffffffffffff
	}
	// These values are checked for overflow during gas cost calculation
	memOffset64 := memOffset.Uint64()
	length64 := length.Uint64()
	scope.Memory.Set(memOffset64, length64, getData(scope.Contract.Input, dataOffset64, length64))

	return nil, nil
}

func opReturnDataSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetUint64(uint64(len(interpreter.returnData))))
	return nil, nil
}

func opReturnDataCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		memOffset     = scope.Stack.pop()
		dataOffset    = scope.Stack.pop()
		length        = scope.Stack.pop()
		memOffsetTag  = scope.Stack.popTag()
		dataOffsetTag = scope.Stack.popTag()
		lengthTag     = scope.Stack.popTag()
	)
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(memOffsetTag, memOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(dataOffsetTag, dataOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(lengthTag, length)
	}

	offset64, overflow := dataOffset.Uint64WithOverflow()
	if overflow {
		return nil, ErrReturnDataOutOfBounds
	}
	// we can reuse dataOffset now (aliasing it for clarity)
	var end = dataOffset
	end.Add(&dataOffset, &length)
	end64, overflow := end.Uint64WithOverflow()
	if overflow || uint64(len(interpreter.returnData)) < end64 {
		return nil, ErrReturnDataOutOfBounds
	}
	scope.Memory.Set(memOffset.Uint64(), length.Uint64(), interpreter.returnData[offset64:end64])
	return nil, nil
}

func opExtCodeSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	slot := scope.Stack.peek()
	slot_tag := scope.Stack.peekTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(*slot_tag, *slot)
		*slot_tag = 0
	}
	slot.SetUint64(uint64(interpreter.evm.StateDB.GetCodeSize(slot.Bytes20())))
	return nil, nil
}

func opCodeSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetUint64(uint64(len(scope.Contract.Code))))
	return nil, nil
}

func opCodeCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		memOffset     = scope.Stack.pop()
		codeOffset    = scope.Stack.pop()
		length        = scope.Stack.pop()
		memOffsetTag  = scope.Stack.popTag()
		codeOffsetTag = scope.Stack.popTag()
		lengthTag     = scope.Stack.popTag()
	)
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(memOffsetTag, memOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(codeOffsetTag, codeOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(lengthTag, length)
	}

	uint64CodeOffset, overflow := codeOffset.Uint64WithOverflow()
	if overflow {
		uint64CodeOffset = math.MaxUint64
	}
	codeCopy := getData(scope.Contract.Code, uint64CodeOffset, length.Uint64())
	scope.Memory.Set(memOffset.Uint64(), length.Uint64(), codeCopy)

	return nil, nil
}

func opExtCodeCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		stack         = scope.Stack
		a             = stack.pop()
		memOffset     = stack.pop()
		codeOffset    = stack.pop()
		length        = stack.pop()
		aTag          = scope.Stack.popTag()
		memOffsetTag  = scope.Stack.popTag()
		codeOffsetTag = scope.Stack.popTag()
		lengthTag     = scope.Stack.popTag()
	)
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(aTag, a)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(memOffsetTag, memOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(codeOffsetTag, codeOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(lengthTag, length)
	}

	uint64CodeOffset, overflow := codeOffset.Uint64WithOverflow()
	if overflow {
		uint64CodeOffset = math.MaxUint64
	}
	addr := common.Address(a.Bytes20())
	codeCopy := getData(interpreter.evm.StateDB.GetCode(addr), uint64CodeOffset, length.Uint64())
	scope.Memory.Set(memOffset.Uint64(), length.Uint64(), codeCopy)

	return nil, nil
}

// opExtCodeHash returns the code hash of a specified account.
// There are several cases when the function is called, while we can relay everything
// to `state.GetCodeHash` function to ensure the correctness.
//
//  1. Caller tries to get the code hash of a normal contract account, state
//     should return the relative code hash and set it as the result.
//
//  2. Caller tries to get the code hash of a non-existent account, state should
//     return common.Hash{} and zero will be set as the result.
//
//  3. Caller tries to get the code hash for an account without contract code, state
//     should return emptyCodeHash(0xc5d246...) as the result.
//
//  4. Caller tries to get the code hash of a precompiled account, the result should be
//     zero or emptyCodeHash.
//
// It is worth noting that in order to avoid unnecessary create and clean, all precompile
// accounts on mainnet have been transferred 1 wei, so the return here should be
// emptyCodeHash. If the precompile account is not transferred any amount on a private or
// customized chain, the return value will be zero.
//
//  5. Caller tries to get the code hash for an account which is marked as self-destructed
//     in the current transaction, the code hash of this account should be returned.
//
//  6. Caller tries to get the code hash for an account which is marked as deleted, this
//     account should be regarded as a non-existent account and zero should be returned.
func opExtCodeHash(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	slot := scope.Stack.peek()
	slotTag := scope.Stack.peekTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(*slotTag, *slot)
		*slotTag = 0
	}
	address := common.Address(slot.Bytes20())
	if interpreter.evm.StateDB.Empty(address) {
		slot.Clear()
	} else {
		slot.SetBytes(interpreter.evm.StateDB.GetCodeHash(address).Bytes())
	}
	return nil, nil
}

func opGasprice(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	v, _ := uint256.FromBig(interpreter.evm.GasPrice)
	scope.Stack.push(v)
	return nil, nil
}

func opBlockhash(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	num := scope.Stack.peek()
	numTag := scope.Stack.peekTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(*numTag, *num)
		*numTag = 0
	}
	num64, overflow := num.Uint64WithOverflow()
	if overflow {
		num.Clear()
		return nil, nil
	}
	var upper, lower uint64
	upper = interpreter.evm.Context.BlockNumber.Uint64()
	if upper < 257 {
		lower = 0
	} else {
		lower = upper - 256
	}
	if num64 >= lower && num64 < upper {
		num.SetBytes(interpreter.evm.Context.GetHash(num64).Bytes())
	} else {
		num.Clear()
	}
	return nil, nil
}

func opCoinbase(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetBytes(interpreter.evm.Context.Coinbase.Bytes()))
	return nil, nil
}

func opTimestamp(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetUint64(interpreter.evm.Context.Time))
	return nil, nil
}

func opNumber(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	v, _ := uint256.FromBig(interpreter.evm.Context.BlockNumber)
	scope.Stack.push(v)
	return nil, nil
}

func opDifficulty(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	v, _ := uint256.FromBig(interpreter.evm.Context.Difficulty)
	scope.Stack.push(v)
	return nil, nil
}

func opRandom(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	v := new(uint256.Int).SetBytes(interpreter.evm.Context.Random.Bytes())
	scope.Stack.push(v)
	return nil, nil
}

func opGasLimit(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetUint64(interpreter.evm.Context.GasLimit))
	return nil, nil
}

func opPop(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.pop()
	scope.Stack.popTag()
	return nil, nil
}

func opMload(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	v := scope.Stack.peek()
	vTag := scope.Stack.peekTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(*vTag, *v)
		*vTag = 0
	}
	offset := int64(v.Uint64())
	v.SetBytes(scope.Memory.GetPtr(offset, 32))
	return nil, nil
}

func opMstore(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	// pop value of the stack
	mStart, val := scope.Stack.pop(), scope.Stack.pop()
	mStartTag, valTag := scope.Stack.popTag(), scope.Stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(mStartTag, mStart)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(valTag, val)
	}
	scope.Memory.Set32(mStart.Uint64(), &val)
	return nil, nil
}

func opMstore8(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	off, val := scope.Stack.pop(), scope.Stack.pop()
	offTag, valTag := scope.Stack.popTag(), scope.Stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(offTag, off)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(valTag, val)
	}
	scope.Memory.store[off.Uint64()] = byte(val.Uint64())
	return nil, nil
}

func opSload(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	loc := scope.Stack.peek()
	locTag := scope.Stack.peekTag()
	hash := common.Hash(loc.Bytes32())
	val := interpreter.evm.StateDB.GetState(scope.Contract.Address(), hash)
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(*locTag, *loc)
		newtag := interpreter.evm.RedoContext.OperationLogs.AppendSloadLog(scope.Contract.Address(), hash, val)
		*locTag = newtag
	}
	loc.SetBytes(val.Bytes())
	return nil, nil
}

func opSstore(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	loc := scope.Stack.pop()
	val := scope.Stack.pop()
	locTag, valTag := scope.Stack.popTag(), scope.Stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		// The gas constraints are appended in the gas function
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(locTag, loc)
		interpreter.evm.RedoContext.OperationLogs.AppendSStoreLog(scope.Contract.Address(), loc.Bytes32(), val, valTag)
	}

	addr := scope.Contract.Address()
	keyHash := common.Hash(loc.Bytes32())
	valHash := common.Hash(val.Bytes32())

	if interpreter.sstoreCb != nil {
		interpreter.sstoreCb(interpreter.evm.TxIndex, *pc, addr, keyHash, valHash)
	}

	interpreter.evm.StateDB.SetState(addr, keyHash, valHash)
	return nil, nil
}

func opJump(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.evm.abort.Load() {
		return nil, errStopToken
	}
	pos := scope.Stack.pop()
	posTag := scope.Stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(posTag, pos)
	}
	if !scope.Contract.validJumpdest(&pos) {
		return nil, ErrInvalidJump
	}
	*pc = pos.Uint64() - 1 // pc will be increased by the interpreter loop
	return nil, nil
}

func opJumpi(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.evm.abort.Load() {
		return nil, errStopToken
	}
	pos, cond := scope.Stack.pop(), scope.Stack.pop()
	posTag, condTag := scope.Stack.popTag(), scope.Stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(posTag, pos)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(condTag, cond)
	}
	if !cond.IsZero() {
		if !scope.Contract.validJumpdest(&pos) {
			return nil, ErrInvalidJump
		}
		*pc = pos.Uint64() - 1 // pc will be increased by the interpreter loop
	}
	return nil, nil
}

func opJumpdest(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	return nil, nil
}

func opPc(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetUint64(*pc))
	return nil, nil
}

func opMsize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetUint64(uint64(scope.Memory.Len())))
	return nil, nil
}

func opGas(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.push(new(uint256.Int).SetUint64(scope.Contract.Gas))
	return nil, nil
}

func opCreate(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	var (
		value        = scope.Stack.pop()
		offset, size = scope.Stack.pop(), scope.Stack.pop()
		input        = scope.Memory.GetCopy(int64(offset.Uint64()), int64(size.Uint64()))
		gas          = scope.Contract.Gas

		valueTag           = scope.Stack.popTag()
		offsetTag, sizeTag = scope.Stack.popTag(), scope.Stack.popTag()
	)
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(valueTag, value)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(offsetTag, offset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(sizeTag, size)
	}

	if interpreter.evm.chainRules.IsEIP150 {
		gas -= gas / 64
	}
	// reuse size int for stackvalue
	stackvalue := size

	scope.Contract.UseGas(gas)

	res, addr, returnGas, suberr := interpreter.evm.Create(scope.Contract, input, gas, &value)
	// Push item on the stack based on the returned error. If the ruleset is
	// homestead we must check for CodeStoreOutOfGasError (homestead only
	// rule) and treat as an error, if the ruleset is frontier we must
	// ignore this error and pretend the operation was successful.
	if interpreter.evm.chainRules.IsHomestead && suberr == ErrCodeStoreOutOfGas {
		stackvalue.Clear()
	} else if suberr != nil && suberr != ErrCodeStoreOutOfGas {
		stackvalue.Clear()
	} else {
		stackvalue.SetBytes(addr.Bytes())
	}
	scope.Stack.push(&stackvalue)
	scope.Contract.Gas += returnGas

	if suberr == ErrExecutionReverted {
		interpreter.returnData = res // set REVERT data to return data buffer
		return res, nil
	}
	interpreter.returnData = nil // clear dirty return data buffer
	return nil, nil
}

func opCreate2(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	var (
		endowment    = scope.Stack.pop()
		offset, size = scope.Stack.pop(), scope.Stack.pop()
		salt         = scope.Stack.pop()
		input        = scope.Memory.GetCopy(int64(offset.Uint64()), int64(size.Uint64()))
		gas          = scope.Contract.Gas

		endowmentTag       = scope.Stack.popTag()
		offsetTag, sizeTag = scope.Stack.popTag(), scope.Stack.popTag()
		slatTag            = scope.Stack.popTag()
	)
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(endowmentTag, endowment)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(offsetTag, offset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(sizeTag, size)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(slatTag, salt)
	}

	// Apply EIP150
	gas -= gas / 64
	scope.Contract.UseGas(gas)
	// reuse size int for stackvalue
	stackvalue := size
	res, addr, returnGas, suberr := interpreter.evm.Create2(scope.Contract, input, gas,
		&endowment, &salt)
	// Push item on the stack based on the returned error.
	if suberr != nil {
		stackvalue.Clear()
	} else {
		stackvalue.SetBytes(addr.Bytes())
	}
	scope.Stack.push(&stackvalue)
	scope.Contract.Gas += returnGas

	if suberr == ErrExecutionReverted {
		interpreter.returnData = res // set REVERT data to return data buffer
		return res, nil
	}
	interpreter.returnData = nil // clear dirty return data buffer
	return nil, nil
}

func opCall(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	stack := scope.Stack
	// Pop gas. The actual gas in interpreter.evm.callGasTemp.
	// We can use this as a temporary value
	temp := stack.pop()
	tempTag := stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(tempTag, temp)
	}
	gas := interpreter.evm.callGasTemp
	// Pop other call parameters.
	addr, value, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	addrTag, valueTag, inOffsetTag, inSizeTag, retOffsetTag, retSizeTag :=
		stack.popTag(), stack.popTag(), stack.popTag(), stack.popTag(), stack.popTag(), stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(addrTag, addr)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(valueTag, value)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(inOffsetTag, inOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(inSizeTag, inSize)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(retOffsetTag, retOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(retSizeTag, retSize)
	}
	toAddr := common.Address(addr.Bytes20())
	// Get the arguments from the memory.
	args := scope.Memory.GetPtr(int64(inOffset.Uint64()), int64(inSize.Uint64()))

	if interpreter.readOnly && !value.IsZero() {
		return nil, ErrWriteProtection
	}
	if !value.IsZero() {
		gas += params.CallStipend
	}
	ret, returnGas, err := interpreter.evm.Call(scope.Contract, toAddr, args, gas, &value)

	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.push(&temp)
	if err == nil || err == ErrExecutionReverted {
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}
	scope.Contract.Gas += returnGas

	interpreter.returnData = ret
	return ret, nil
}

func opCallCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	stack := scope.Stack
	// We use it as a temporary value
	temp := stack.pop()
	tempTag := stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(tempTag, temp)
	}
	gas := interpreter.evm.callGasTemp
	// Pop other call parameters.
	addr, value, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	addrTag, valueTag, inOffsetTag, inSizeTag, retOffsetTag, retSizeTag :=
		stack.popTag(), stack.popTag(), stack.popTag(), stack.popTag(), stack.popTag(), stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(addrTag, addr)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(valueTag, value)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(inOffsetTag, inOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(inSizeTag, inSize)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(retOffsetTag, retOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(retSizeTag, retSize)
	}
	toAddr := common.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(int64(inOffset.Uint64()), int64(inSize.Uint64()))

	if !value.IsZero() {
		gas += params.CallStipend
	}

	ret, returnGas, err := interpreter.evm.CallCode(scope.Contract, toAddr, args, gas, &value)
	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.push(&temp)
	if err == nil || err == ErrExecutionReverted {
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}
	scope.Contract.Gas += returnGas

	interpreter.returnData = ret
	return ret, nil
}

func opDelegateCall(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	stack := scope.Stack
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	// We use it as a temporary value
	temp := stack.pop()
	tempTag := stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(tempTag, temp)
	}
	gas := interpreter.evm.callGasTemp
	// Pop other call parameters.
	addr, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	addrTag, inOffsetTag, inSizeTag, retOffsetTag, retSizeTag :=
		stack.popTag(), stack.popTag(), stack.popTag(), stack.popTag(), stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(addrTag, addr)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(inOffsetTag, inOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(inSizeTag, inSize)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(retOffsetTag, retOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(retSizeTag, retSize)
	}
	toAddr := common.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(int64(inOffset.Uint64()), int64(inSize.Uint64()))

	ret, returnGas, err := interpreter.evm.DelegateCall(scope.Contract, toAddr, args, gas)
	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.push(&temp)
	if err == nil || err == ErrExecutionReverted {
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}
	scope.Contract.Gas += returnGas

	interpreter.returnData = ret
	return ret, nil
}

func opStaticCall(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	stack := scope.Stack
	// We use it as a temporary value
	temp := stack.pop()
	tempTag := stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(tempTag, temp)
	}
	gas := interpreter.evm.callGasTemp
	// Pop other call parameters.
	addr, inOffset, inSize, retOffset, retSize := stack.pop(), stack.pop(), stack.pop(), stack.pop(), stack.pop()
	addrTag, inOffsetTag, inSizeTag, retOffsetTag, retSizeTag :=
		stack.popTag(), stack.popTag(), stack.popTag(), stack.popTag(), stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(addrTag, addr)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(inOffsetTag, inOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(inSizeTag, inSize)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(retOffsetTag, retOffset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(retSizeTag, retSize)
	}
	toAddr := common.Address(addr.Bytes20())
	// Get arguments from the memory.
	args := scope.Memory.GetPtr(int64(inOffset.Uint64()), int64(inSize.Uint64()))

	ret, returnGas, err := interpreter.evm.StaticCall(scope.Contract, toAddr, args, gas)
	if err != nil {
		temp.Clear()
	} else {
		temp.SetOne()
	}
	stack.push(&temp)
	if err == nil || err == ErrExecutionReverted {
		scope.Memory.Set(retOffset.Uint64(), retSize.Uint64(), ret)
	}
	scope.Contract.Gas += returnGas

	interpreter.returnData = ret
	return ret, nil
}

func opReturn(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	offset, size := scope.Stack.pop(), scope.Stack.pop()
	offsetTag, sizeTag := scope.Stack.popTag(), scope.Stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(offsetTag, offset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(sizeTag, size)
	}
	ret := scope.Memory.GetPtr(int64(offset.Uint64()), int64(size.Uint64()))

	return ret, errStopToken
}

func opRevert(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	offset, size := scope.Stack.pop(), scope.Stack.pop()
	offsetTag, sizeTag := scope.Stack.popTag(), scope.Stack.popTag()
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(offsetTag, offset)
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(sizeTag, size)
	}
	ret := scope.Memory.GetPtr(int64(offset.Uint64()), int64(size.Uint64()))

	interpreter.returnData = ret
	return ret, ErrExecutionReverted
}

func opUndefined(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	return nil, &ErrInvalidOpCode{opcode: OpCode(scope.Contract.Code[*pc])}
}

func opStop(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	return nil, errStopToken
}

func opSelfdestruct(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	beneficiary := scope.Stack.pop()
	beneficiaryTag := scope.Stack.popTag()
	balance := interpreter.evm.StateDB.GetBalance(scope.Contract.Address())
	interpreter.evm.StateDB.AddBalance(beneficiary.Bytes20(), balance)
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(beneficiaryTag, beneficiary)
		interpreter.evm.RedoContext.AppendAssertBalanceEqualLog(scope.Contract.Address(), balance)
		interpreter.evm.RedoContext.AppendAddBalanceLog(beneficiary.Bytes20(), balance)
	}
	interpreter.evm.StateDB.SelfDestruct(scope.Contract.Address())
	if tracer := interpreter.evm.Config.Tracer; tracer != nil {
		tracer.CaptureEnter(SELFDESTRUCT, scope.Contract.Address(), beneficiary.Bytes20(), []byte{}, 0, balance.ToBig())
		tracer.CaptureExit([]byte{}, 0, nil)
	}
	return nil, errStopToken
}

func opSelfdestruct6780(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	beneficiary := scope.Stack.pop()
	beneficiaryTag := scope.Stack.popTag()
	balance := interpreter.evm.StateDB.GetBalance(scope.Contract.Address())
	interpreter.evm.StateDB.SubBalance(scope.Contract.Address(), balance)
	interpreter.evm.StateDB.AddBalance(beneficiary.Bytes20(), balance)
	if interpreter.evm.Config.EnableRedo {
		interpreter.evm.RedoContext.OperationLogs.AppendAssertion(beneficiaryTag, beneficiary)
		interpreter.evm.RedoContext.AppendAssertBalanceEqualLog(scope.Contract.Address(), balance)
		interpreter.evm.RedoContext.AppendSubBalanceLog(scope.Contract.Address(), balance)
		interpreter.evm.RedoContext.AppendAddBalanceLog(beneficiary.Bytes20(), balance)
	}
	interpreter.evm.StateDB.Selfdestruct6780(scope.Contract.Address())
	if tracer := interpreter.evm.Config.Tracer; tracer != nil {
		tracer.CaptureEnter(SELFDESTRUCT, scope.Contract.Address(), beneficiary.Bytes20(), []byte{}, 0, balance.ToBig())
		tracer.CaptureExit([]byte{}, 0, nil)
	}
	return nil, errStopToken
}

// following functions are used by the instruction jump  table

// make log instruction function
func makeLog(size int) executionFunc {
	return func(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
		if interpreter.readOnly {
			return nil, ErrWriteProtection
		}
		topics := make([]common.Hash, size)
		stack := scope.Stack
		mStart, mSize := stack.pop(), stack.pop()
		mStartTag, mSizeTag := stack.popTag(), stack.popTag()
		if interpreter.evm.Config.EnableRedo {
			interpreter.evm.RedoContext.OperationLogs.AppendAssertion(mStartTag, mStart)
			interpreter.evm.RedoContext.OperationLogs.AppendAssertion(mSizeTag, mSize)
		}
		for i := 0; i < size; i++ {
			addr := stack.pop()
			addrTag := stack.popTag()
			if interpreter.evm.Config.EnableRedo {
				interpreter.evm.RedoContext.OperationLogs.AppendAssertion(addrTag, addr)
			}
			topics[i] = addr.Bytes32()
		}

		d := scope.Memory.GetCopy(int64(mStart.Uint64()), int64(mSize.Uint64()))
		interpreter.evm.StateDB.AddLog(&types.Log{
			Address: scope.Contract.Address(),
			Topics:  topics,
			Data:    d,
			// This is a non-consensus field, but assigned here because
			// core/state doesn't know the current block number.
			BlockNumber: interpreter.evm.Context.BlockNumber.Uint64(),
		})

		return nil, nil
	}
}

// opPush1 is a specialized version of pushN
func opPush1(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		codeLen = uint64(len(scope.Contract.Code))
		integer = new(uint256.Int)
	)
	*pc += 1
	if *pc < codeLen {
		scope.Stack.push(integer.SetUint64(uint64(scope.Contract.Code[*pc])))
	} else {
		scope.Stack.push(integer.Clear())
	}
	return nil, nil
}

// make push instruction function
func makePush(size uint64, pushByteSize int) executionFunc {
	return func(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
		codeLen := len(scope.Contract.Code)

		startMin := codeLen
		if int(*pc+1) < startMin {
			startMin = int(*pc + 1)
		}

		endMin := codeLen
		if startMin+pushByteSize < endMin {
			endMin = startMin + pushByteSize
		}

		integer := new(uint256.Int)
		scope.Stack.push(integer.SetBytes(common.RightPadBytes(
			scope.Contract.Code[startMin:endMin], pushByteSize)))

		*pc += size
		return nil, nil
	}
}

// make dup instruction function
func makeDup(size int64) executionFunc {
	return func(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
		scope.Stack.dup(int(size))
		return nil, nil
	}
}

// make swap instruction function
func makeSwap(size int64) executionFunc {
	// switch n + 1 otherwise n would be swapped with n
	size++
	return func(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
		scope.Stack.swap(int(size))
		return nil, nil
	}
}
