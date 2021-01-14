/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workqueue

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"
)

// Interface FIFO 队列接口, 先进先出队列, 并支持去重机制
type Interface interface {
	Add(item interface{})                   // 给队列添加元素(item), 可以是任意类型元素
	Len() int                               // 返回当前队列的长度
	Get() (item interface{}, shutdown bool) // 获取队列头部的一个元素
	Done(item interface{})                  // 标记队列中该元素已被处理
	ShutDown()                              // 关闭队列
	ShuttingDown() bool                     // 查询队列是否正在关闭
}

// New constructs a new work queue (see the package comment).
// New 实例化一个 FIFO 队列
func New() *Type {
	return NewNamed("")
}

func NewNamed(name string) *Type {
	rc := clock.RealClock{}
	return newQueue(
		rc,
		globalMetricsFactory.newQueueMetrics(name, rc),
		defaultUnfinishedWorkUpdatePeriod,
	)
}

func newQueue(c clock.Clock, metrics queueMetrics, updatePeriod time.Duration) *Type {
	t := &Type{
		clock:                      c,
		dirty:                      set{},
		processing:                 set{},
		cond:                       sync.NewCond(&sync.Mutex{}),
		metrics:                    metrics,
		unfinishedWorkUpdatePeriod: updatePeriod,
	}
	go t.updateUnfinishedWorkLoop()
	return t
}

const defaultUnfinishedWorkUpdatePeriod = 500 * time.Millisecond

// Type is a work queue (see the package comment).
type Type struct {
	// queue defines the order in which we will work on items. Every
	// element of queue should be in the dirty set and not in the
	// processing set.
	// queue 实际存储元素的地方, slice 结构, 用于保证元素有序
	queue []t

	// dirty defines all of the items that need to be processed.
	// 除了保证去重, 还能保证在处理一个元素之前哪怕其被添加了多次(并发情况下), 但也只会被处理一次
	dirty set

	// Things that are currently being processed are in the processing set.
	// These things may be simultaneously in the dirty set. When we finish
	// processing something and remove it from this set, we'll check if
	// it's in the dirty set, and if so, add it to the queue.
	// 用于标记机制, 标记一个元素是否正在被处理.
	processing set

	cond *sync.Cond // 条件变量, 当队列为空时, 会进入休眠, 等到有新数据添加后唤醒

	shuttingDown bool // 队列是否正在关闭

	metrics queueMetrics // WorkQueue metrics 接口

	unfinishedWorkUpdatePeriod time.Duration // 默认 500ms
	clock                      clock.Clock   // 时钟
}

type empty struct{}
type t interface{}
type set map[t]empty

// has 检测 set 这个 map 中是否已存在 item 这个元素, 若是则返回 true
func (s set) has(item t) bool {
	_, exists := s[item]
	return exists
}

// insert 向 set 这个 map 中存入 item 元素
func (s set) insert(item t) {
	s[item] = empty{}
}

// delete 从 set 这个 map 中删除 item 这个元素
func (s set) delete(item t) {
	delete(s, item)
}

// Add marks item as needing processing.
// Add 给队列添加元素(item), 可以是任意类型元素
func (q *Type) Add(item interface{}) {
	// 添加元素时需要给队列加锁
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	// 若队列正在关闭中, 则不添加, 立即返回
	if q.shuttingDown {
		return
	}
	// 若 dirty 这个 set 中已存在相同元素, 则不添加到 queue 中, 已达到去重目的
	if q.dirty.has(item) {
		return
	}

	q.metrics.add(item)

	// dirty 中不存在这个元素, 则向 dirty 中添加该 item 元素
	q.dirty.insert(item)
	// 若 processing 中已存在相同的 item 元素, 则表示这个 item 正在处理中, 则立即返回,
	// 不向 queue 中添加该元素
	if q.processing.has(item) {
		return
	}

	// 若该 item 元素没有在处理中, 则添加到 queue 中
	q.queue = append(q.queue, item)
	// 发送信号, 唤醒等待 queue 数据的 goroutine
	q.cond.Signal()
}

// Len returns the current queue length, for informational purposes only. You
// shouldn't e.g. gate a call to Add() or Get() on Len() being a particular
// value, that can't be synchronized properly.
func (q *Type) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return len(q.queue)
}

// Get blocks until it can return an item to be processed. If shutdown = true,
// the caller should end their goroutine. You must call Done with item when you
// have finished processing it.
func (q *Type) Get() (item interface{}, shutdown bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	// 当队列 queue 为空且队列未关闭时, 阻塞等待, 直到 queue 有新数据添加后唤醒
	for len(q.queue) == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	// 若队列正在关闭中, 则直接返回
	if len(q.queue) == 0 {
		// We must be shutting down.
		return nil, true
	}

	// 从队列头取出一个元素
	item, q.queue = q.queue[0], q.queue[1:]

	q.metrics.get(item)

	// 将取出的元素插入 processing 中, 标记这个元素正在处理中
	q.processing.insert(item)
	// 同时从 dirty 中删除该元素
	q.dirty.delete(item)

	return item, false
}

// Done marks item as done processing, and if it has been marked as dirty again
// while it was being processed, it will be re-added to the queue for
// re-processing.
// Done 标记这个对象 item 已经处理完毕
func (q *Type) Done(item interface{}) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.metrics.done(item)

	// 对象 item 已处理完毕, 则将其从 processing 这个 map 中移除
	q.processing.delete(item)
	// 如果 dirty 中存在该对象, 则表示在处理该对象 item 期间, 又接收到该相同元素,
	// 则将其放入 queue 尾部, 再次处理该对象 item
	if q.dirty.has(item) {
		q.queue = append(q.queue, item)
		q.cond.Signal()
	}
}

// ShutDown will cause q to ignore all new items added to it. As soon as the
// worker goroutines have drained the existing items in the queue, they will be
// instructed to exit.
// ShutDown 关闭队列. 将 shuttingDown 置为 true, 同时唤醒所有休眠等待获取 queue 中元素的 goroutine
func (q *Type) ShutDown() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.shuttingDown = true
	q.cond.Broadcast()
}

// ShuttingDown 查询队列是否正在关闭中
func (q *Type) ShuttingDown() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	return q.shuttingDown
}

// updateUnfinishedWorkLoop 队列未关闭时, 每 500ms 执行一次
func (q *Type) updateUnfinishedWorkLoop() {
	t := q.clock.NewTicker(q.unfinishedWorkUpdatePeriod)
	defer t.Stop()
	for range t.C() {
		if !func() bool {
			q.cond.L.Lock()
			defer q.cond.L.Unlock()
			// 若队列未关闭, 则更新 metrics 信息
			if !q.shuttingDown {
				q.metrics.updateUnfinishedWork()
				return true
			}
			return false

		}() {
			return
		}
	}
}
