// File: 		workerpool.go
// Description: Worker pool implementation.
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
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	DEFAULT_WORKERTHREADS = 8
)

type WorkerPool[T any] struct {
	size int

	ctx           context.Context
	ctxCancelFunc context.CancelFunc
	threadsWg     sync.WaitGroup
	initWg        sync.WaitGroup

	state int32

	taskPool *TaskPool[T]
}

type WorkerPoolOption[T any] func(self *WorkerPool[T])

func WorkerPoolWithNumCPUThreads[T any]() WorkerPoolOption[T] {
	return func(self *WorkerPool[T]) {
		self.size = runtime.NumCPU()
	}
}

func WorkerPoolWithThreadsCount[T any](c int) WorkerPoolOption[T] {
	return func(self *WorkerPool[T]) {
		self.size = c
	}
}

func NewWorkerPool[T any](taskPool *TaskPool[T], opts ...WorkerPoolOption[T]) *WorkerPool[T] {
	self := new(WorkerPool[T])
	self.size = DEFAULT_WORKERTHREADS
	self.taskPool = taskPool

	for i := range opts {
		opts[i](self)
	}

	return self
}

func (self *WorkerPool[T]) Run() {
	if !atomic.CompareAndSwapInt32(&self.state, 0, 1) {
		return
	}

	ctx, ctxCancelFunc := context.WithCancel(context.Background())

	self.ctx = ctx
	self.ctxCancelFunc = ctxCancelFunc

	for i := range self.size {
		self.initWg.Add(1)
		go self.threadWorker(ctx, i)
	}

	self.initWg.Wait()
}

func (self *WorkerPool[T]) Shutdown() {

	if !atomic.CompareAndSwapInt32(&self.state, 1, 0) {
		return
	}

	// it bascially means worker pool cannot
	// be stopped. context is the only way to
	// tell threads to stop working
	if self.ctx == nil || self.ctxCancelFunc == nil {
		return
	}

	self.ctxCancelFunc()
	self.threadsWg.Wait()
}

// Starts new worker thread, which is responsible for
// processing tasks, stored in queue
func (self *WorkerPool[T]) threadWorker(ctx context.Context, _ int) {
	self.initWg.Done()
	self.threadsWg.Add(1)

	defer func() {
		self.threadsWg.Done()
	}()

	for {
		for {
			// queue predrain
			if ptr := self.taskPool.coldQueue.Dequeue(); ptr != nil {
				atomic.AddInt32(&self.taskPool.coldQueueSize, -1)
				if task, ok := ptr.(T); ok {
					self.taskPool.handler(task)
				}
				continue
			}

			break
		}

		select {
		case <-ctx.Done():
			goto free

		case <-self.taskPool.queueSig:
		}
	}

free:
	return
}
