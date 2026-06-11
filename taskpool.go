// File: 		taskpool.go
// Description: Task pool implementation.
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
	"errors"
	"sync/atomic"
)

const (
	DEFAULT_MAX_TASKQUEUE_SIZE = 2 << (21 - 1)
)

type TaskHandlerFunc[T any] func(t T)

// Task pool provides a buffered lockfree task storage with
// signal, specifically usefull for workerpool. It does not depend
// on workerpool at all, it's used separatly to push tasks as events,
// so the module, who is sendind tasks does not depend on worker pool
// also.
//
// Used to store non-critical events for event broker in queue,
// so the client don't have to wait for all events published,
// before returning result
//
// Why task handler is stored within task pool and
// now worker pool?
//
// 1) Task pool is meant to store tasks of T type,
// which means handler must work with the same type.
// Basically "here is the task, do it like that".
//
// 2) If worker pool is shutdown and later a new one
// is created, it can be done safely without handler setup.
// Worker pool just do that it told to do.
type TaskPool[T any] struct {
	coldQueue     *LockfreeQueue
	coldQueueSize int32
	coldQueueFlag int32
	queueSig      chan struct{}
	handler       TaskHandlerFunc[T]

	ceilingSize int32
}

func NewTaskPool[T any](handler TaskHandlerFunc[T]) *TaskPool[T] {

	self := new(TaskPool[T])
	self.coldQueue = NewQueue()
	self.coldQueueSize = 0
	self.coldQueueFlag = 0
	self.queueSig = make(chan struct{}, 1)
	self.handler = handler
	self.ceilingSize = DEFAULT_MAX_TASKQUEUE_SIZE

	return self
}

// Set ceiling size of task pool. It's an unsafe limit of tasks,
// which task pool will store.
//
// It's not recommended to use after initial init due to
// possible dirty reads within `Send`, which remains as it
// for performance.
func (self *TaskPool[T]) SetCeilingSize(ceilingSize int32) {
	if ceilingSize < 1 {
		panic(errors.New("ceiling size cannot be lower than 1"))
	}

	atomic.SwapInt32(&self.ceilingSize, ceilingSize)
}

// Send task to background worker
//
// Returns false only if it failed to store this task
// even in queue, which is proccessed as fast as workers reaches it,
// but it's pretty hard to achieve due to cold queue size
func (self *TaskPool[T]) Send(task T) bool {

	// unsafe check. this limits queue size in large
	// amounts of threads not surely equal to size limit,
	// but it stops it from unlimited grow
	if atomic.AddInt32(&self.coldQueueSize, 1) <= self.ceilingSize {
		self.coldQueue.Enqueue(task)

		select {
		case self.queueSig <- struct{}{}:
		default:
		}

		return true
	}

	atomic.AddInt32(&self.coldQueueSize, -1)
	return false
}
