// Copyright 2014 The go-ethereum Authors
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
	"sync"

	"github.com/holiman/uint256"
)

var stackPool = sync.Pool{
	New: func() interface{} {
		return &Stack{data: make([]uint256.Int, 0, 16), tag: make([]int, 0, 16)}
	},
}

// Stack is an object for basic stack operations. Items popped to the stack are
// expected to be changed and modified. stack does not take care of adding newly
// initialised objects.
type Stack struct {
	data []uint256.Int
	tag  []int
}

func newstack() *Stack {
	return stackPool.Get().(*Stack)
}

func returnStack(s *Stack) {
	s.data = s.data[:0]
	s.tag = s.tag[:0]
	stackPool.Put(s)
}

// Data returns the underlying uint256.Int array.
func (st *Stack) Data() []uint256.Int {
	return st.data
}

func (st *Stack) TagData() []int {
	return st.tag
}

func (st *Stack) pushData(d *uint256.Int) {
	// NOTE push limit (1024) is checked in baseCheck
	st.data = append(st.data, *d)
}

func (st *Stack) pushTag(t int) {
	// NOTE push limit (1024) is checked in baseCheck
	st.tag = append(st.tag, t)
}

func (st *Stack) push(d *uint256.Int) {
	// NOTE push limit (1024) is checked in baseCheck
	st.pushData(d)
	st.pushTag(0)
}

func (st *Stack) pop() (ret uint256.Int) {
	ret = st.data[len(st.data)-1]
	st.data = st.data[:len(st.data)-1]
	return
}

func (st *Stack) popTag() (ret int) {
	ret = st.tag[len(st.tag)-1]
	st.tag = st.tag[:len(st.tag)-1]
	return
}

func (st *Stack) len() int {
	return len(st.data)
}

func (st *Stack) swap(n int) {
	st.data[st.len()-n], st.data[st.len()-1] = st.data[st.len()-1], st.data[st.len()-n]
	st.tag[st.len()-n], st.tag[st.len()-1] = st.tag[st.len()-1], st.tag[st.len()-n]
}

func (st *Stack) dup(n int) {
	len := st.len()
	st.pushData(&st.data[len-n])
	st.pushTag(st.tag[len-n])
}

func (st *Stack) peek() *uint256.Int {
	return &st.data[st.len()-1]
}

func (st *Stack) peekTag() *int {
	return &st.tag[len(st.tag)-1]
}

// Back returns the n'th item in stack
func (st *Stack) Back(n int) *uint256.Int {
	return &st.data[st.len()-n-1]
}

func (st *Stack) BackTag(n int) int {
	return st.tag[st.len()-n-1]
}
