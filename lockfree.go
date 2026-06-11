// File: 		lockfree.go
// Description: Lockfree queue implementation.
//
// Copyright 2026 vndg-rdmt
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package taskarena

import (
	"sync/atomic"
	"unsafe"
)

type directItem struct {
	next unsafe.Pointer
	v    interface{}
}

func loaditem(p *unsafe.Pointer) *directItem {
	return (*directItem)(atomic.LoadPointer(p))
}

func casitem(p *unsafe.Pointer, old, new *directItem) bool {
	return atomic.CompareAndSwapPointer(p, unsafe.Pointer(old), unsafe.Pointer(new))
}

type LockfreeQueue struct {
	head unsafe.Pointer
	tail unsafe.Pointer
	len  uint64
}

func NewQueue() *LockfreeQueue {
	head := &directItem{next: nil, v: nil}
	return &LockfreeQueue{
		tail: unsafe.Pointer(head),
		head: unsafe.Pointer(head),
	}
}

func (q *LockfreeQueue) Enqueue(v interface{}) {
	i := &directItem{next: nil, v: v}

	var last, lastnext *directItem
	for {
		last = loaditem(&q.tail)
		lastnext = loaditem(&last.next)
		if loaditem(&q.tail) == last {
			if lastnext == nil {
				if casitem(&last.next, lastnext, i) {
					casitem(&q.tail, last, i)
					atomic.AddUint64(&q.len, 1)
					return
				}
			} else {
				casitem(&q.tail, last, lastnext)
			}
		}
	}
}

func (q *LockfreeQueue) Dequeue() interface{} {
	var first, last, firstnext *directItem
	for {
		first = loaditem(&q.head)
		last = loaditem(&q.tail)
		firstnext = loaditem(&first.next)
		if first == loaditem(&q.head) {
			if first == last {
				if firstnext == nil {
					return nil
				}
				casitem(&q.tail, last, firstnext)
			} else {
				if firstnext == nil {
					return nil
				}
				v := firstnext.v
				if casitem(&q.head, first, firstnext) {
					atomic.AddUint64(&q.len, ^uint64(0))

					firstnext.v = nil

					return v
				}
			}
		}
	}
}

func (q *LockfreeQueue) Length() uint64 {
	return atomic.LoadUint64(&q.len)
}
