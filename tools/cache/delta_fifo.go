/*
Copyright 2014 The Kubernetes Authors.

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

package cache

import (
	"errors"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"

	"k8s.io/klog"
)

// NewDeltaFIFO returns a Store which can be used process changes to items.
//
// keyFunc is used to figure out what key an object should have. (It's
// exposed in the returned DeltaFIFO's KeyOf() method, with bonus features.)
//
// 'keyLister' is expected to return a list of keys that the consumer of
// this queue "knows about". It is used to decide which items are missing
// when Replace() is called; 'Deleted' deltas are produced for these items.
// It may be nil if you don't need to detect all deletions.
// TODO: consider merging keyLister with this object, tracking a list of
//       "known" keys when Pop() is called. Have to think about how that
//       affects error retrying.
// NOTE: It is possible to misuse this and cause a race when using an
//       external known object source.
//       Whether there is a potential race depends on how the comsumer
//       modifies knownObjects. In Pop(), process function is called under
//       lock, so it is safe to update data structures in it that need to be
//       in sync with the queue (e.g. knownObjects).
//
//       Example:
//       In case of sharedIndexInformer being a consumer
//       (https://github.com/kubernetes/kubernetes/blob/0cdd940f/staging/
//       src/k8s.io/client-go/tools/cache/shared_informer.go#L192),
//       there is no race as knownObjects (s.indexer) is modified safely
//       under DeltaFIFO's lock. The only exceptions are GetStore() and
//       GetIndexer() methods, which expose ways to modify the underlying
//       storage. Currently these two methods are used for creating Lister
//       and internal tests.
//
// Also see the comment on DeltaFIFO.
func NewDeltaFIFO(keyFunc KeyFunc, knownObjects KeyListerGetter) *DeltaFIFO {
	f := &DeltaFIFO{
		items:        map[string]Deltas{},
		queue:        []string{},
		keyFunc:      keyFunc,
		knownObjects: knownObjects,
	}
	f.cond.L = &f.lock
	return f
}

// DeltaFIFO is like FIFO, but allows you to process deletes.
//
// DeltaFIFO is a producer-consumer queue, where a Reflector is
// intended to be the producer, and the consumer is whatever calls
// the Pop() method.
//
// DeltaFIFO solves this use case:
//  * You want to process every object change (delta) at most once.
//  * When you process an object, you want to see everything
//    that's happened to it since you last processed it.
//  * You want to process the deletion of objects.
//  * You might want to periodically reprocess objects.
//
// DeltaFIFO's Pop(), Get(), and GetByKey() methods return
// interface{} to satisfy the Store/Queue interfaces, but it
// will always return an object of type Deltas.
//
// A note on threading: If you call Pop() in parallel from multiple
// threads, you could end up with multiple threads processing slightly
// different versions of the same object.
//
// A note on the KeyLister used by the DeltaFIFO: It's main purpose is
// to list keys that are "known", for the purpose of figuring out which
// items have been deleted when Replace() or Delete() are called. The deleted
// object will be included in the DeleteFinalStateUnknown markers. These objects
// could be stale.
//
// DeltaFIFO 可分开理解, FIFO 是一个先进先出的队列, 它拥有队列操作的基本方法, 如 Add、Update、Delete、
// List、Pop、Close 等, 而 Delta 是一个资源对象存储, 它可以保存资源对象的操作类型, 如 Added(添加)操作
// 类型、Updated(更新)操作类型、Deleted(删除)操作类型、Sync(同步)操作类型等.
//
// DeltaFIFO 与其他队列最大的不同之处是, 它会保留所有关于资源对象(obj)的操作类型, 队列中会存在拥有不同
// 操作类型的同一个资源对象, 消费者在处理该资源对象时能够了解该资源对象所发生的事情.
type DeltaFIFO struct {
	// lock/cond protects access to 'items' and 'queue'.
	lock sync.RWMutex
	cond sync.Cond

	// We depend on the property that items in the set are in
	// the queue and vice versa, and that all Deltas in this
	// map have at least one Delta.
	// items 字段通过 map 数据结构的方式存储, value 存储的是对象的 Deltas 数组.
	// 一个 Delta 包含操作类型以及资源对象
	items map[string]Deltas
	// queue 字段存储资源对象的 key, 该 key 通过 KeyOf 函数计算得到
	queue []string

	// populated is true if the first batch of items inserted by Replace() has been populated
	// or Delete/Add/Update was called first.
	// 首次调用 Delete/Add/Update 或者通过 Replace() 首次批量插入 items 到 DeltaFIFO 中时, 则设置该字段为 true
	populated bool
	// initialPopulationCount is the number of items inserted by the first call of Replace()
	initialPopulationCount int

	// keyFunc is used to make the key used for queued item
	// insertion and retrieval, and should be deterministic.
	// keyFunc 用于计算资源对象的 key, key 的格式为: {namespace}/{name}, 若不存在 namespace, 则为 {name}
	keyFunc KeyFunc

	// knownObjects list keys that are "known", for the
	// purpose of figuring out which items have been deleted
	// when Replace() or Delete() is called.
	// knownObjects 实际为 Indexer 实例, 该实例是用于存储资源对象并自带索引功能的本地存储
	knownObjects KeyListerGetter

	// Indication the queue is closed.
	// Used to indicate a queue is closed so a control loop can exit when a queue is empty.
	// Currently, not used to gate any of CRED operations.
	closed     bool // 标识队列是否已关闭
	closedLock sync.Mutex
}

var (
	_ = Queue(&DeltaFIFO{}) // DeltaFIFO is a Queue
)

var (
	// ErrZeroLengthDeltasObject is returned in a KeyError if a Deltas
	// object with zero length is encountered (should be impossible,
	// but included for completeness).
	ErrZeroLengthDeltasObject = errors.New("0 length Deltas object; can't get key")
)

// Close the queue.
func (f *DeltaFIFO) Close() {
	f.closedLock.Lock()
	defer f.closedLock.Unlock()
	f.closed = true
	f.cond.Broadcast()
}

// KeyOf exposes f's keyFunc, but also detects the key of a Deltas object or
// DeletedFinalStateUnknown objects.
func (f *DeltaFIFO) KeyOf(obj interface{}) (string, error) {
	if d, ok := obj.(Deltas); ok {
		if len(d) == 0 {
			return "", KeyError{obj, ErrZeroLengthDeltasObject}
		}
		obj = d.Newest().Object
	}
	if d, ok := obj.(DeletedFinalStateUnknown); ok {
		return d.Key, nil
	}
	return f.keyFunc(obj)
}

// HasSynced returns true if an Add/Update/Delete/AddIfNotPresent are called first,
// or an Update called first but the first batch of items inserted by Replace() has been popped
func (f *DeltaFIFO) HasSynced() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.populated && f.initialPopulationCount == 0
}

// Add inserts an item, and puts it in the queue. The item is only enqueued
// if it doesn't already exist in the set.
// Add 当 ListAndWatch 函数中 Watch 到资源对象时, 会调用该函数, 将新增的资源对象添加到
// DeltaFIFO 中.
func (f *DeltaFIFO) Add(obj interface{}) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	return f.queueActionLocked(Added, obj)
}

// Update is just like Add, but makes an Updated Delta.
func (f *DeltaFIFO) Update(obj interface{}) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	return f.queueActionLocked(Updated, obj)
}

// Delete is just like Add, but makes an Deleted Delta. If the item does not
// already exist, it will be ignored. (It may have already been deleted by a
// Replace (re-list), for example.
func (f *DeltaFIFO) Delete(obj interface{}) error {
	id, err := f.KeyOf(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	if f.knownObjects == nil {
		if _, exists := f.items[id]; !exists {
			// Presumably, this was deleted when a relist happened.
			// Don't provide a second report of the same deletion.
			return nil
		}
	} else {
		// We only want to skip the "deletion" action if the object doesn't
		// exist in knownObjects and it doesn't have corresponding item in items.
		// Note that even if there is a "deletion" action in items, we can ignore it,
		// because it will be deduped automatically in "queueActionLocked"
		_, exists, err := f.knownObjects.GetByKey(id) // 尝试从本地缓存 Indexer 中获取该 key 指定的资源对象
		_, itemsExist := f.items[id]
		// 若该资源对象在本地存储 Indexer 和 DeltaFIFO 中都不存在, 则直接返回
		if err == nil && !exists && !itemsExist {
			// Presumably, this was deleted when a relist happened.
			// Don't provide a second report of the same deletion.
			return nil
		}
	}

	return f.queueActionLocked(Deleted, obj)
}

// AddIfNotPresent inserts an item, and puts it in the queue. If the item is already
// present in the set, it is neither enqueued nor added to the set.
//
// This is useful in a single producer/consumer scenario so that the consumer can
// safely retry items without contending with the producer and potentially enqueueing
// stale items.
//
// Important: obj must be a Deltas (the output of the Pop() function). Yes, this is
// different from the Add/Update/Delete functions.
func (f *DeltaFIFO) AddIfNotPresent(obj interface{}) error {
	deltas, ok := obj.(Deltas)
	if !ok {
		return fmt.Errorf("object must be of type deltas, but got: %#v", obj)
	}
	id, err := f.KeyOf(deltas.Newest().Object)
	if err != nil {
		return KeyError{obj, err}
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.addIfNotPresent(id, deltas)
	return nil
}

// addIfNotPresent inserts deltas under id if it does not exist, and assumes the caller
// already holds the fifo lock.
func (f *DeltaFIFO) addIfNotPresent(id string, deltas Deltas) {
	f.populated = true
	if _, exists := f.items[id]; exists {
		return
	}

	f.queue = append(f.queue, id)
	f.items[id] = deltas
	f.cond.Broadcast()
}

// re-listing and watching can deliver the same update multiple times in any
// order. This will combine the most recent two deltas if they are the same.
func dedupDeltas(deltas Deltas) Deltas {
	n := len(deltas)
	if n < 2 {
		return deltas
	}
	a := &deltas[n-1]
	b := &deltas[n-2]
	if out := isDup(a, b); out != nil {
		d := append(Deltas{}, deltas[:n-2]...)
		return append(d, *out)
	}
	return deltas
}

// If a & b represent the same event, returns the delta that ought to be kept.
// Otherwise, returns nil.
// TODO: is there anything other than deletions that need deduping?
func isDup(a, b *Delta) *Delta {
	if out := isDeletionDup(a, b); out != nil {
		return out
	}
	// TODO: Detect other duplicate situations? Are there any?
	return nil
}

// keep the one with the most information if both are deletions.
func isDeletionDup(a, b *Delta) *Delta {
	if b.Type != Deleted || a.Type != Deleted {
		return nil
	}
	// Do more sophisticated checks, or is this sufficient?
	if _, ok := b.Object.(DeletedFinalStateUnknown); ok {
		return a
	}
	return b
}

// queueActionLocked appends to the delta list for the object.
// Caller must lock first.
// queueActionLocked 将 actionType 和 obj 构造出的一个 Delta 对象缓存到 f.items 和 f.queue 中,
// 并唤醒休眠等待数据的 goroutine.
// TODO: 执行该函数需要加锁
func (f *DeltaFIFO) queueActionLocked(actionType DeltaType, obj interface{}) error {
	// 1. 通过 f.KeyOf 函数计算出资源对象的 key
	id, err := f.KeyOf(obj)
	if err != nil {
		return KeyError{obj, err}
	}

	// 将 actionType 和资源对象构造成 Delta, 添加到 items 中, 并通过 dedupDeltas 函数进行去重操作
	newDeltas := append(f.items[id], Delta{actionType, obj})
	newDeltas = dedupDeltas(newDeltas)

	// 更新构造后的 Delta 并通过 cond.Broadcast 通知所有消费者解除阻塞
	if len(newDeltas) > 0 {
		// 若 f.items 这个 map 中不曾缓存过该资源对象, 则将该资源对象的 key 添加到 f.queue 中(仅限首次才需添加)
		if _, exists := f.items[id]; !exists {
			f.queue = append(f.queue, id)
		}
		f.items[id] = newDeltas
		f.cond.Broadcast()
	} else {
		// We need to remove this from our map (extra items in the queue are
		// ignored if they are not in the map).
		delete(f.items, id)
	}
	return nil
}

// List returns a list of all the items; it returns the object
// from the most recent Delta.
// You should treat the items returned inside the deltas as immutable.
func (f *DeltaFIFO) List() []interface{} {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.listLocked()
}

func (f *DeltaFIFO) listLocked() []interface{} {
	list := make([]interface{}, 0, len(f.items))
	for _, item := range f.items {
		list = append(list, item.Newest().Object)
	}
	return list
}

// ListKeys returns a list of all the keys of the objects currently
// in the FIFO.
func (f *DeltaFIFO) ListKeys() []string {
	f.lock.RLock()
	defer f.lock.RUnlock()
	list := make([]string, 0, len(f.items))
	for key := range f.items {
		list = append(list, key)
	}
	return list
}

// Get returns the complete list of deltas for the requested item,
// or sets exists=false.
// You should treat the items returned inside the deltas as immutable.
func (f *DeltaFIFO) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := f.KeyOf(obj)
	if err != nil {
		return nil, false, KeyError{obj, err}
	}
	return f.GetByKey(key)
}

// GetByKey returns the complete list of deltas for the requested item,
// setting exists=false if that list is empty.
// You should treat the items returned inside the deltas as immutable.
func (f *DeltaFIFO) GetByKey(key string) (item interface{}, exists bool, err error) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	d, exists := f.items[key]
	if exists {
		// Copy item's slice so operations on this slice
		// won't interfere with the object we return.
		d = copyDeltas(d)
	}
	return d, exists, nil
}

// IsClosed checks if the queue is closed
func (f *DeltaFIFO) IsClosed() bool {
	f.closedLock.Lock()
	defer f.closedLock.Unlock()
	return f.closed
}

// Pop blocks until an item is added to the queue, and then returns it.  If
// multiple items are ready, they are returned in the order in which they were
// added/updated. The item is removed from the queue (and the store) before it
// is returned, so if you don't successfully process it, you need to add it back
// with AddIfNotPresent().
// process function is called under lock, so it is safe update data structures
// in it that need to be in sync with the queue (e.g. knownKeys). The PopProcessFunc
// may return an instance of ErrRequeue with a nested error to indicate the current
// item should be requeued (equivalent to calling AddIfNotPresent under the lock).
//
// Pop returns a 'Deltas', which has a complete list of all the things
// that happened to the object (deltas) while it was sitting in the queue.
func (f *DeltaFIFO) Pop(process PopProcessFunc) (interface{}, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	for {
		// 当队列中没有数据时, 通过 f.cond.Wait 阻塞等待数据, 只有收到 cond.Broadcase 时才说明有数据被添加,
		// 解除当前阻塞状态.
		for len(f.queue) == 0 {
			// When the queue is empty, invocation of Pop() is blocked until new item is enqueued.
			// When Close() is called, the f.closed is set and the condition is broadcasted.
			// Which causes this loop to continue and return from the Pop().
			if f.IsClosed() {
				return nil, ErrFIFOClosed
			}

			f.cond.Wait()
		}
		// 如果队列中不为空, 取出 f.queue 的头部数据
		id := f.queue[0]
		f.queue = f.queue[1:]
		if f.initialPopulationCount > 0 {
			f.initialPopulationCount--
		}
		// item 是一个 Delta 切片, 可能会存在不同操作类型的资源对象,
		// 如 [{"Added", obj1}, {"Updated", obj1}]
		item, ok := f.items[id]
		if !ok {
			// Item may have been deleted subsequently.
			continue
		}
		delete(f.items, id)
		// 将该取出的 f.queue 头部数据对象传入 process 回调函数, 由上层消费者进行处理
		// TODO: 这里 process 回调函数即为 sharedIndexInformer.HandleDeltas 方法
		err := process(item)
		if e, ok := err.(ErrRequeue); ok {
			// 如果 process 回调函数处理出错, 则将该对象重新存入队列尾部.
			f.addIfNotPresent(id, item)
			err = e.Err
		}
		// Don't need to copyDeltas here, because we're transferring
		// ownership to the caller.
		return item, err
	}
}

// Replace will delete the contents of 'f', using instead the given map.
// 'f' takes ownership of the map, you should not reference the map again
// after calling this function. f's queue is reset, too; upon return, it
// will contain the items in the map, in no particular order.
// Replace 将使用 list 对象中的数据以 Sync 操作类型保存到当前 DeltaFIFO 中,
// 同时删除本地存储 Indexer 中缓存的所有旧的资源数据.
func (f *DeltaFIFO) Replace(list []interface{}, resourceVersion string) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	keys := make(sets.String, len(list))

	for _, item := range list {
		key, err := f.KeyOf(item)
		if err != nil {
			return KeyError{item, err}
		}
		keys.Insert(key)
		// 将 Sync 操作类型和 item 资源对象构成一个 Delta, 存储到 f.queue 和 f.items 中
		if err := f.queueActionLocked(Sync, item); err != nil {
			return fmt.Errorf("couldn't enqueue object: %v", err)
		}
	}

	if f.knownObjects == nil {
		// Do deletion detection against our own list.
		queuedDeletions := 0
		for k, oldItem := range f.items {
			if keys.Has(k) {
				continue
			}
			var deletedObj interface{}
			if n := oldItem.Newest(); n != nil {
				deletedObj = n.Object
			}
			queuedDeletions++
			if err := f.queueActionLocked(Deleted, DeletedFinalStateUnknown{k, deletedObj}); err != nil {
				return err
			}
		}

		if !f.populated {
			f.populated = true
			// While there shouldn't be any queued deletions in the initial
			// population of the queue, it's better to be on the safe side.
			f.initialPopulationCount = len(list) + queuedDeletions
		}

		return nil
	}

	// Detect deletions not already in the queue.
	// 删除本地存储 Indexer 中所有旧的数据, 即非本次通过 list 参数传入的数据,
	// 删除操作是通过将旧的数据结合 Deleted 操作类型构造成一个 Delta 对象, 存入 DeltaFIFO 中.
	knownKeys := f.knownObjects.ListKeys()
	queuedDeletions := 0
	for _, k := range knownKeys {
		if keys.Has(k) {
			continue
		}

		// 从 Indexer 中获取指定 key 对应的资源对象
		deletedObj, exists, err := f.knownObjects.GetByKey(k)
		if err != nil {
			deletedObj = nil
			klog.Errorf("Unexpected error %v during lookup of key %v, placing DeleteFinalStateUnknown marker without object", err, k)
		} else if !exists {
			deletedObj = nil
			klog.Infof("Key %v does not exist in known objects store, placing DeleteFinalStateUnknown marker without object", k)
		}
		queuedDeletions++
		if err := f.queueActionLocked(Deleted, DeletedFinalStateUnknown{k, deletedObj}); err != nil {
			return err
		}
	}

	if !f.populated {
		f.populated = true
		f.initialPopulationCount = len(list) + queuedDeletions
	}

	return nil
}

// Resync will send a sync event for each item
// Resync 机制会将 Indexer 本地存储中的资源对象同步到 DeltaFIFO 中, 并将这些资源对象设置为 Sync 的操作类型.
// Resync 函数会在 Reflector 中定时执行, 它的执行周期由 NewReflector 函数传入的 resyncPeriod 参数设定.
func (f *DeltaFIFO) Resync() error {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.knownObjects == nil {
		return nil
	}

	keys := f.knownObjects.ListKeys()
	for _, k := range keys {
		if err := f.syncKeyLocked(k); err != nil {
			return err
		}
	}
	return nil
}

func (f *DeltaFIFO) syncKeyLocked(key string) error {
	// f.knownObjects 是 Indexer 本地存储对象, 通过该对象可以获取 client-go 目前存储的所有资源对象,
	// Indexer 对象在 NewDeltaFIFO 函数实例化 DeltaFIFO 对象时传入.
	obj, exists, err := f.knownObjects.GetByKey(key)
	if err != nil {
		klog.Errorf("Unexpected error %v during lookup of key %v, unable to queue object for sync", err, key)
		return nil
	} else if !exists {
		klog.Infof("Key %v does not exist in known objects store, unable to queue object for sync", key)
		return nil
	}

	// If we are doing Resync() and there is already an event queued for that object,
	// we ignore the Resync for it. This is to avoid the race, in which the resync
	// comes with the previous value of object (since queueing an event for the object
	// doesn't trigger changing the underlying store <knownObjects>.
	id, err := f.KeyOf(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	// 若该资源对象在 DeltaFIFO 中已经存在, 则不需要再次往 DeltaFIFO 中存
	if len(f.items[id]) > 0 {
		return nil
	}

	if err := f.queueActionLocked(Sync, obj); err != nil {
		return fmt.Errorf("couldn't queue object: %v", err)
	}
	return nil
}

// A KeyListerGetter is anything that knows how to list its keys and look up by key.
type KeyListerGetter interface {
	KeyLister
	KeyGetter
}

// A KeyLister is anything that knows how to list its keys.
type KeyLister interface {
	ListKeys() []string
}

// A KeyGetter is anything that knows how to get the value stored under a given key.
type KeyGetter interface {
	GetByKey(key string) (interface{}, bool, error)
}

// DeltaType is the type of a change (addition, deletion, etc)
type DeltaType string

// Change type definition
const (
	Added   DeltaType = "Added"
	Updated DeltaType = "Updated"
	Deleted DeltaType = "Deleted"
	// The other types are obvious. You'll get Sync deltas when:
	//  * A watch expires/errors out and a new list/watch cycle is started.
	//  * You've turned on periodic syncs.
	// (Anything that trigger's DeltaFIFO's Replace() method.)
	Sync DeltaType = "Sync"
)

// Delta is the type stored by a DeltaFIFO. It tells you what change
// happened, and the object's state after* that change.
//
// [*] Unless the change is a deletion, and then you'll get the final
//     state of the object before it was deleted.
type Delta struct {
	Type   DeltaType   // 当前资源对象的操作类型, 如 Added、Updated、Deleted
	Object interface{} // 资源对象
}

// Deltas is a list of one or more 'Delta's to an individual object.
// The oldest delta is at index 0, the newest delta is the last one.
type Deltas []Delta

// Oldest is a convenience function that returns the oldest delta, or
// nil if there are no deltas.
func (d Deltas) Oldest() *Delta {
	if len(d) > 0 {
		return &d[0]
	}
	return nil
}

// Newest is a convenience function that returns the newest delta, or
// nil if there are no deltas.
func (d Deltas) Newest() *Delta {
	if n := len(d); n > 0 {
		return &d[n-1]
	}
	return nil
}

// copyDeltas returns a shallow copy of d; that is, it copies the slice but not
// the objects in the slice. This allows Get/List to return an object that we
// know won't be clobbered by a subsequent modifications.
func copyDeltas(d Deltas) Deltas {
	d2 := make(Deltas, len(d))
	copy(d2, d)
	return d2
}

// DeletedFinalStateUnknown is placed into a DeltaFIFO in the case where
// an object was deleted but the watch deletion event was missed. In this
// case we don't know the final "resting" state of the object, so there's
// a chance the included `Obj` is stale.
type DeletedFinalStateUnknown struct {
	Key string
	Obj interface{}
}
