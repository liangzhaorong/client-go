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

package cache

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/clock"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/buffer"

	"k8s.io/klog"
)

// SharedInformer provides eventually consistent linkage of its
// clients to the authoritative state of a given collection of
// objects.  An object is identified by its API group, kind/resource,
// namespace, and name; the `ObjectMeta.UID` is not part of an
// object's ID as far as this contract is concerned.  One
// SharedInformer provides linkage to objects of a particular API
// group and kind/resource.  The linked object collection of a
// SharedInformer may be further restricted to one namespace and/or by
// label selector and/or field selector.
//
// The authoritative state of an object is what apiservers provide
// access to, and an object goes through a strict sequence of states.
// An object state is either "absent" or present with a
// ResourceVersion and other appropriate content.
//
// A SharedInformer gets object states from apiservers using a
// sequence of LIST and WATCH operations.  Through this sequence the
// apiservers provide a sequence of "collection states" to the
// informer, where each collection state defines the state of every
// object of the collection.  No promise --- beyond what is implied by
// other remarks here --- is made about how one informer's sequence of
// collection states relates to a different informer's sequence of
// collection states.
//
// A SharedInformer maintains a local cache, exposed by GetStore() and
// by GetIndexer() in the case of an indexed informer, of the state of
// each relevant object.  This cache is eventually consistent with the
// authoritative state.  This means that, unless prevented by
// persistent communication problems, if ever a particular object ID X
// is authoritatively associated with a state S then for every
// SharedInformer I whose collection includes (X, S) eventually either
// (1) I's cache associates X with S or a later state of X, (2) I is
// stopped, or (3) the authoritative state service for X terminates.
// To be formally complete, we say that the absent state meets any
// restriction by label selector or field selector.
//
// The local cache starts out empty, and gets populated and updated
// during `Run()`.
//
// As a simple example, if a collection of objects is henceforeth
// unchanging, a SharedInformer is created that links to that
// collection, and that SharedInformer is `Run()` then that
// SharedInformer's cache eventually holds an exact copy of that
// collection (unless it is stopped too soon, the authoritative state
// service ends, or communication problems between the two
// persistently thwart achievement).
//
// As another simple example, if the local cache ever holds a
// non-absent state for some object ID and the object is eventually
// removed from the authoritative state then eventually the object is
// removed from the local cache (unless the SharedInformer is stopped
// too soon, the authoritative state service ends, or communication
// problems persistently thwart the desired result).
//
// The keys in the Store are of the form namespace/name for namespaced
// objects, and are simply the name for non-namespaced objects.
// Clients can use `MetaNamespaceKeyFunc(obj)` to extract the key for
// a given object, and `SplitMetaNamespaceKey(key)` to split a key
// into its constituent parts.
//
// A client is identified here by a ResourceEventHandler.  For every
// update to the SharedInformer's local cache and for every client
// added before `Run()`, eventually either the SharedInformer is
// stopped or the client is notified of the update.  A client added
// after `Run()` starts gets a startup batch of notifications of
// additions of the object existing in the cache at the time that
// client was added; also, for every update to the SharedInformer's
// local cache after that client was added, eventually either the
// SharedInformer is stopped or that client is notified of that
// update.  Client notifications happen after the corresponding cache
// update and, in the case of a SharedIndexInformer, after the
// corresponding index updates.  It is possible that additional cache
// and index updates happen before such a prescribed notification.
// For a given SharedInformer and client, the notifications are
// delivered sequentially.  For a given SharedInformer, client, and
// object ID, the notifications are delivered in order.
//
// A client must process each notification promptly; a SharedInformer
// is not engineered to deal well with a large backlog of
// notifications to deliver.  Lengthy processing should be passed off
// to something else, for example through a
// `client-go/util/workqueue`.
//
// Each query to an informer's local cache --- whether a single-object
// lookup, a list operation, or a use of one of its indices --- is
// answered entirely from one of the collection states received by
// that informer.
//
// A delete notification exposes the last locally known non-absent
// state, except that its ResourceVersion is replaced with a
// ResourceVersion in which the object is actually absent.
type SharedInformer interface {
	// AddEventHandler adds an event handler to the shared informer using the shared informer's resync
	// period.  Events to a single handler are delivered sequentially, but there is no coordination
	// between different handlers.
	AddEventHandler(handler ResourceEventHandler)
	// AddEventHandlerWithResyncPeriod adds an event handler to the
	// shared informer using the specified resync period.  The resync
	// operation consists of delivering to the handler a create
	// notification for every object in the informer's local cache; it
	// does not add any interactions with the authoritative storage.
	AddEventHandlerWithResyncPeriod(handler ResourceEventHandler, resyncPeriod time.Duration)
	// GetStore returns the informer's local cache as a Store.
	GetStore() Store
	// GetController gives back a synthetic interface that "votes" to start the informer
	GetController() Controller
	// Run starts and runs the shared informer, returning after it stops.
	// The informer will be stopped when stopCh is closed.
	Run(stopCh <-chan struct{})
	// HasSynced returns true if the shared informer's store has been
	// informed by at least one full LIST of the authoritative state
	// of the informer's object collection.  This is unrelated to "resync".
	HasSynced() bool
	// LastSyncResourceVersion is the resource version observed when last synced with the underlying
	// store. The value returned is not synchronized with access to the underlying store and is not
	// thread-safe.
	LastSyncResourceVersion() string
}

// SharedIndexInformer provides add and get Indexers ability based on SharedInformer.
type SharedIndexInformer interface {
	SharedInformer
	// AddIndexers add indexers to the informer before it starts.
	AddIndexers(indexers Indexers) error
	GetIndexer() Indexer
}

// NewSharedInformer creates a new instance for the listwatcher.
func NewSharedInformer(lw ListerWatcher, objType runtime.Object, resyncPeriod time.Duration) SharedInformer {
	return NewSharedIndexInformer(lw, objType, resyncPeriod, Indexers{})
}

// NewSharedIndexInformer creates a new instance for the listwatcher.
// NewSharedIndexInformer 根据 listwatcher 创建一个新的 SharedIndexInformer实例
func NewSharedIndexInformer(lw ListerWatcher, objType runtime.Object, defaultEventHandlerResyncPeriod time.Duration, indexers Indexers) SharedIndexInformer {
	realClock := &clock.RealClock{}
	sharedIndexInformer := &sharedIndexInformer{
		processor:                       &sharedProcessor{clock: realClock},
		indexer:                         NewIndexer(DeletionHandlingMetaNamespaceKeyFunc, indexers),
		listerWatcher:                   lw,
		objectType:                      objType,
		resyncCheckPeriod:               defaultEventHandlerResyncPeriod,
		defaultEventHandlerResyncPeriod: defaultEventHandlerResyncPeriod,
		cacheMutationDetector:           NewCacheMutationDetector(fmt.Sprintf("%T", objType)),
		clock:                           realClock,
	}
	return sharedIndexInformer
}

// InformerSynced is a function that can be used to determine if an informer has synced.  This is useful for determining if caches have synced.
type InformerSynced func() bool

const (
	// syncedPollPeriod controls how often you look at the status of your sync funcs
	syncedPollPeriod = 100 * time.Millisecond

	// initialBufferSize is the initial number of event notifications that can be buffered.
	initialBufferSize = 1024
)

// WaitForNamedCacheSync is a wrapper around WaitForCacheSync that generates log messages
// indicating that the caller identified by name is waiting for syncs, followed by
// either a successful or failed sync.
func WaitForNamedCacheSync(controllerName string, stopCh <-chan struct{}, cacheSyncs ...InformerSynced) bool {
	klog.Infof("Waiting for caches to sync for %s", controllerName)

	if !WaitForCacheSync(stopCh, cacheSyncs...) {
		utilruntime.HandleError(fmt.Errorf("unable to sync caches for %s", controllerName))
		return false
	}

	klog.Infof("Caches are synced for %s ", controllerName)
	return true
}

// WaitForCacheSync waits for caches to populate.  It returns true if it was successful, false
// if the controller should shutdown
// callers should prefer WaitForNamedCacheSync()
func WaitForCacheSync(stopCh <-chan struct{}, cacheSyncs ...InformerSynced) bool {
	err := wait.PollImmediateUntil(syncedPollPeriod,
		func() (bool, error) {
			for _, syncFunc := range cacheSyncs {
				if !syncFunc() {
					return false, nil
				}
			}
			return true, nil
		},
		stopCh)
	if err != nil {
		klog.V(2).Infof("stop requested")
		return false
	}

	klog.V(4).Infof("caches populated")
	return true
}

type sharedIndexInformer struct {
	indexer    Indexer // Indexer 是 client-go 用来存储资源对象并自带索引功能的本地存储
	controller Controller

	processor             *sharedProcessor // 共享处理器
	cacheMutationDetector MutationDetector

	// This block is tracked to handle late initialization of the controller
	listerWatcher ListerWatcher  // 实现 ListerWatcher 接口的对象, 用于获取及监控 objectType 代表的资源类型的资源列表
	objectType    runtime.Object // 该 Informer 监听的资源类型, 如 v1.Pod

	// resyncCheckPeriod is how often we want the reflector's resync timer to fire so it can call
	// shouldResync to check if any of our listeners need a resync.
	// 该字段表示多久执行一次 resync(重新同步)， resync 会周期性地执行 List 操作, 将所有的资源存放在
	// Informer Store 中, 如果该字段为 0, 则禁用 resync 功能.
	resyncCheckPeriod time.Duration
	// defaultEventHandlerResyncPeriod is the default resync period for any handlers added via
	// AddEventHandler (i.e. they don't specify one and just want to use the shared informer's default
	// value).
	// defaultEventHandlerResyncPeriod 用于对通过 AddEventHandler 添加的任意 handlers 时默认使用的 resync 周期.
	defaultEventHandlerResyncPeriod time.Duration
	// clock allows for testability
	clock clock.Clock // 实时时钟

	// 这两个字段分别表示当前 shared informer 是否已启动/已关闭
	started, stopped bool
	startedLock      sync.Mutex

	// blockDeltas gives a way to stop all event distribution so that a late event handler
	// can safely join the shared informer.
	// blockDeltas 互斥锁用于停止所有的事件分发, 以便让后续通过 informer.AddEventHandler
	// 添加的事件处理程序能够安全的添加到 shared informer 中.
	blockDeltas sync.Mutex
}

// dummyController hides the fact that a SharedInformer is different from a dedicated one
// where a caller can `Run`.  The run method is disconnected in this case, because higher
// level logic will decide when to start the SharedInformer and related controller.
// Because returning information back is always asynchronous, the legacy callers shouldn't
// notice any change in behavior.
type dummyController struct {
	informer *sharedIndexInformer
}

func (v *dummyController) Run(stopCh <-chan struct{}) {
}

func (v *dummyController) HasSynced() bool {
	return v.informer.HasSynced()
}

func (v *dummyController) LastSyncResourceVersion() string {
	return ""
}

type updateNotification struct {
	oldObj interface{}
	newObj interface{}
}

type addNotification struct {
	newObj interface{}
}

type deleteNotification struct {
	oldObj interface{}
}

// Run 运行该 Informer
func (s *sharedIndexInformer) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	// 构建一个 DeltaFIFO 实例
	fifo := NewDeltaFIFO(MetaNamespaceKeyFunc, s.indexer)

	cfg := &Config{
		Queue:            fifo,
		ListerWatcher:    s.listerWatcher,
		ObjectType:       s.objectType,
		FullResyncPeriod: s.resyncCheckPeriod,
		RetryOnError:     false,
		ShouldResync:     s.processor.shouldResync,

		Process: s.HandleDeltas,
	}

	func() {
		s.startedLock.Lock()
		defer s.startedLock.Unlock()

		s.controller = New(cfg)
		s.controller.(*controller).clock = s.clock
		s.started = true
	}()

	// Separate stop channel because Processor should be stopped strictly after controller
	processorStopCh := make(chan struct{})
	var wg wait.Group
	defer wg.Wait()              // Wait for Processor to stop
	defer close(processorStopCh) // Tell Processor to stop
	wg.StartWithChannel(processorStopCh, s.cacheMutationDetector.Run)
	wg.StartWithChannel(processorStopCh, s.processor.run) // 运行 Processor

	defer func() {
		s.startedLock.Lock()
		defer s.startedLock.Unlock()
		s.stopped = true // Don't want any new listeners
	}()
	s.controller.Run(stopCh)
}

func (s *sharedIndexInformer) HasSynced() bool {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.controller == nil {
		return false
	}
	return s.controller.HasSynced()
}

func (s *sharedIndexInformer) LastSyncResourceVersion() string {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.controller == nil {
		return ""
	}
	return s.controller.LastSyncResourceVersion()
}

func (s *sharedIndexInformer) GetStore() Store {
	return s.indexer
}

func (s *sharedIndexInformer) GetIndexer() Indexer {
	return s.indexer
}

func (s *sharedIndexInformer) AddIndexers(indexers Indexers) error {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.started {
		return fmt.Errorf("informer has already started")
	}

	return s.indexer.AddIndexers(indexers)
}

func (s *sharedIndexInformer) GetController() Controller {
	return &dummyController{informer: s}
}

// AddEventHandler 注册事件处理程序
func (s *sharedIndexInformer) AddEventHandler(handler ResourceEventHandler) {
	s.AddEventHandlerWithResyncPeriod(handler, s.defaultEventHandlerResyncPeriod)
}

func determineResyncPeriod(desired, check time.Duration) time.Duration {
	if desired == 0 {
		return desired
	}
	if check == 0 {
		klog.Warningf("The specified resyncPeriod %v is invalid because this shared informer doesn't support resyncing", desired)
		return 0
	}
	if desired < check {
		klog.Warningf("The specified resyncPeriod %v is being increased to the minimum resyncCheckPeriod %v", desired, check)
		return check
	}
	return desired
}

const minimumResyncPeriod = 1 * time.Second

// AddEventHandlerWithResyncPeriod 添加带有 resync 周期的事件处理程序
func (s *sharedIndexInformer) AddEventHandlerWithResyncPeriod(handler ResourceEventHandler, resyncPeriod time.Duration) {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	// 若该 shared informer 已关闭, 则直接返回
	if s.stopped {
		klog.V(2).Infof("Handler %v was not added to shared informer because it has stopped already", handler)
		return
	}

	// 若配置了 resync 周期 (小于等于 0 则表示禁用 resync 功能)
	if resyncPeriod > 0 {
		if resyncPeriod < minimumResyncPeriod {
			klog.Warningf("resyncPeriod %d is too small. Changing it to the minimum allowed value of %d", resyncPeriod, minimumResyncPeriod)
			resyncPeriod = minimumResyncPeriod
		}

		if resyncPeriod < s.resyncCheckPeriod {
			if s.started {
				klog.Warningf("resyncPeriod %d is smaller than resyncCheckPeriod %d and the informer has already started. Changing it to %d", resyncPeriod, s.resyncCheckPeriod, s.resyncCheckPeriod)
				resyncPeriod = s.resyncCheckPeriod
			} else {
				// if the event handler's resyncPeriod is smaller than the current resyncCheckPeriod, update
				// resyncCheckPeriod to match resyncPeriod and adjust the resync periods of all the listeners
				// accordingly
				s.resyncCheckPeriod = resyncPeriod
				s.processor.resyncCheckPeriodChanged(resyncPeriod)
			}
		}
	}

	// 构建一个 processorListener 对象.
	listener := newProcessListener(handler, resyncPeriod, determineResyncPeriod(resyncPeriod, s.resyncCheckPeriod), s.clock.Now(), initialBufferSize)

	// 若当前 shared informer 还未启动, 则将 listener 注册到 processor 中后直接返回
	if !s.started {
		s.processor.addListener(listener)
		return
	}

	// in order to safely join, we have to
	// 1. stop sending add/update/delete notifications
	// 2. do a list against the store
	// 3. send synthetic "Add" events to the new handler
	// 4. unblock
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()

	s.processor.addListener(listener)
	for _, item := range s.indexer.List() {
		listener.add(addNotification{newObj: item})
	}
}

// HandleDeltas 作为 DeltaFIFO.Pop 函数中 process 回调函数, 当资源对象的操作类型为 Added、Updated、Deleted 时,
// 将该资源对象存储至 Indexer(它是并发安全的存储), 并通过 distribute 函数将资源对象分发至 SharedInformer.
// 我们通过 informer.AddEventHandler 函数添加了对资源事件进行处理的函数, distribute 函数则将资源对象分发到该
// 事件处理函数中.
func (s *sharedIndexInformer) HandleDeltas(obj interface{}) error {
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()

	// from oldest to newest
	for _, d := range obj.(Deltas) {
		switch d.Type {
		case Sync, Added, Updated:
			isSync := d.Type == Sync
			s.cacheMutationDetector.AddObject(d.Object)
			// 若 Indexer 中已存在该对象, 则更新 Indexer 中缓存的对象
			if old, exists, err := s.indexer.Get(d.Object); err == nil && exists {
				if err := s.indexer.Update(d.Object); err != nil {
					return err
				}
				s.processor.distribute(updateNotification{oldObj: old, newObj: d.Object}, isSync)
			} else {
				// 若 Indexer 中不存在, 则添加
				if err := s.indexer.Add(d.Object); err != nil {
					return err
				}
				s.processor.distribute(addNotification{newObj: d.Object}, isSync)
			}
		case Deleted:
			// 删除 Indexer 中缓存的对象
			if err := s.indexer.Delete(d.Object); err != nil {
				return err
			}
			s.processor.distribute(deleteNotification{oldObj: d.Object}, false)
		}
	}
	return nil
}

// sharedProcessor 共享处理器
type sharedProcessor struct {
	listenersStarted bool
	listenersLock    sync.RWMutex
	listeners        []*processorListener // 存储当前 sharedProcessor 管理的 listeners
	syncingListeners []*processorListener // 缓存当前到时间需要执行 resync 操作的 listeners
	clock            clock.Clock          // 实时时钟
	wg               wait.Group
}

// addListener 将 listener 添加到共享处理器 sharedProcessor 中, 同时若当前 sharedProcessor 已经启动
// 之前的 listeners 了, 则直接启动新添加的 listener.
func (p *sharedProcessor) addListener(listener *processorListener) {
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()

	p.addListenerLocked(listener)
	if p.listenersStarted {
		p.wg.Start(listener.run)
		p.wg.Start(listener.pop)
	}
}

func (p *sharedProcessor) addListenerLocked(listener *processorListener) {
	p.listeners = append(p.listeners, listener)
	p.syncingListeners = append(p.syncingListeners, listener)
}

// distribute 函数将资源对象分发到通过 informer.AddEventHandler 函数注册的事件处理函数中
func (p *sharedProcessor) distribute(obj interface{}, sync bool) {
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()

	// 若该资源对象 obj 是通过 resync 操作从 Indexer 中添加到 DeltaFIFO 中, 然后再次走到这里
	if sync {
		// 则仅向当前到时间执行 sync 操作的 listener 分发该资源对象
		for _, listener := range p.syncingListeners {
			listener.add(obj)
		}
	} else {
		// 非 sync 操作则向所有的 listener 分发该资源对象
		for _, listener := range p.listeners {
			listener.add(obj)
		}
	}
}

func (p *sharedProcessor) run(stopCh <-chan struct{}) {
	func() {
		p.listenersLock.RLock()
		defer p.listenersLock.RUnlock()
		for _, listener := range p.listeners {
			p.wg.Start(listener.run)
			p.wg.Start(listener.pop)
		}
		p.listenersStarted = true
	}()
	<-stopCh
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()
	for _, listener := range p.listeners {
		close(listener.addCh) // Tell .pop() to stop. .pop() will tell .run() to stop
	}
	p.wg.Wait() // Wait for all .pop() and .run() to stop
}

// shouldResync queries every listener to determine if any of them need a resync, based on each
// listener's resyncPeriod.
func (p *sharedProcessor) shouldResync() bool {
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()

	p.syncingListeners = []*processorListener{}

	resyncNeeded := false
	now := p.clock.Now()
	for _, listener := range p.listeners {
		// need to loop through all the listeners to see if they need to resync so we can prepare any
		// listeners that are going to be resyncing.
		if listener.shouldResync(now) {
			resyncNeeded = true
			p.syncingListeners = append(p.syncingListeners, listener)
			listener.determineNextResync(now) // 更新下一次执行 resync 的时间
		}
	}
	return resyncNeeded
}

// resyncCheckPeriodChanged 更新 sharedProcessor 下所有 listener 的 resync 周期值
func (p *sharedProcessor) resyncCheckPeriodChanged(resyncCheckPeriod time.Duration) {
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()

	for _, listener := range p.listeners {
		resyncPeriod := determineResyncPeriod(listener.requestedResyncPeriod, resyncCheckPeriod)
		listener.setResyncPeriod(resyncPeriod)
	}
}

// 每调用一次 informer.AddEventHandler 为当前 informer 注册事件处理程序, 都会创建一个对应的 processorListener 对象
type processorListener struct {
	nextCh chan interface{}
	addCh  chan interface{}

	handler ResourceEventHandler // 调用 informer.AddEventHandler 注册的事件处理程序

	// pendingNotifications is an unbounded ring buffer that holds all notifications not yet distributed.
	// There is one per listener, but a failing/stalled listener will have infinite pendingNotifications
	// added until we OOM.
	// TODO: This is no worse than before, since reflectors were backed by unbounded DeltaFIFOs, but
	// we should try to do something better.
	pendingNotifications buffer.RingGrowing // 缓存 pending 的 notifications 的环回缓存区, 默认 1024

	// requestedResyncPeriod is how frequently the listener wants a full resync from the shared informer
	// requestedResyncPeriod 是 listener 执行从 shared informer 中执行全量 resync 的频率(或叫周期)
	requestedResyncPeriod time.Duration
	// resyncPeriod is how frequently the listener wants a full resync from the shared informer. This
	// value may differ from requestedResyncPeriod if the shared informer adjusts it to align with the
	// informer's overall resync check period.
	resyncPeriod time.Duration
	// nextResync is the earliest time the listener should get a full resync
	nextResync time.Time // 下一次执行 resync 的实际时间, 计算方式为: 当前时间 + resyncPeriod
	// resyncLock guards access to resyncPeriod and nextResync
	resyncLock sync.Mutex
}

// newProcessListener 实例化一个 processorListener 对象
func newProcessListener(handler ResourceEventHandler, requestedResyncPeriod, resyncPeriod time.Duration, now time.Time, bufferSize int) *processorListener {
	ret := &processorListener{
		nextCh:                make(chan interface{}),
		addCh:                 make(chan interface{}),
		handler:               handler,
		pendingNotifications:  *buffer.NewRingGrowing(bufferSize),
		requestedResyncPeriod: requestedResyncPeriod,
		resyncPeriod:          resyncPeriod,
	}

	// 确定下一次执行 resync 的具体时间
	ret.determineNextResync(now)

	return ret
}

// add 将 nofitication 对象发送给 p.addCh 这个 channel
func (p *processorListener) add(notification interface{}) {
	p.addCh <- notification
}

// pop 该函数主要实现从 p.addCh 中获取数据, 并将该数据经过 p.pendingNotifications 这个缓冲区
// 传递给 p.nextCh,以便 run() 函数所在的 goroutine 中获取该数据然后进行分发到通过 AddEventHandler 函数
// 注册的三个事件处理函数: OnAdd, OnUpdate, OnDelete
func (p *processorListener) pop() {
	defer utilruntime.HandleCrash()
	defer close(p.nextCh) // Tell .run() to stop

	var nextCh chan<- interface{} // 无缓存 channel
	var notification interface{}
	for {
		select {
		case nextCh <- notification:
			// Notification dispatched
			var ok bool
			// 从环回缓冲区中获取一个数据, 若没有则第 2 个返回值为 false
			notification, ok = p.pendingNotifications.ReadOne()
			if !ok { // Nothing to pop
				nextCh = nil // Disable this select case
			}
		case notificationToAdd, ok := <-p.addCh: // 在这里监听 p.addCh, 等待数据被添加进该 channel
			if !ok {
				return
			}
			if notification == nil { // No notification to pop (and pendingNotifications is empty)
				// Optimize the case - skip adding to pendingNotifications
				notification = notificationToAdd
				nextCh = p.nextCh
			} else { // There is already a notification waiting to be dispatched
				// 向 pendingNotifications 这个缓存区中写入从 p.addCh 监听到 notificationToAdd 数据
				p.pendingNotifications.WriteOne(notificationToAdd)
			}
		}
	}
}

// run 从 p.nextCh 中获取 Notification 对象数据, 根据该对象的类型, 调用通过 AddEventHandler 函数注册的程序处理函数
func (p *processorListener) run() {
	// this call blocks until the channel is closed.  When a panic happens during the notification
	// we will catch it, **the offending item will be skipped!**, and after a short delay (one second)
	// the next notification will be attempted.  This is usually better than the alternative of never
	// delivering again.
	stopCh := make(chan struct{})
	wait.Until(func() {
		// this gives us a few quick retries before a long pause and then a few more quick retries
		err := wait.ExponentialBackoff(retry.DefaultRetry, func() (bool, error) {
			for next := range p.nextCh {
				switch notification := next.(type) {
				case updateNotification:
					p.handler.OnUpdate(notification.oldObj, notification.newObj)
				case addNotification:
					p.handler.OnAdd(notification.newObj)
				case deleteNotification:
					p.handler.OnDelete(notification.oldObj)
				default:
					utilruntime.HandleError(fmt.Errorf("unrecognized notification: %T", next))
				}
			}
			// the only way to get here is if the p.nextCh is empty and closed
			return true, nil
		})

		// the only way to get here is if the p.nextCh is empty and closed
		if err == nil {
			close(stopCh)
		}
	}, 1*time.Minute, stopCh)
}

// shouldResync deterimines if the listener needs a resync. If the listener's resyncPeriod is 0,
// this always returns false.
// shouldResync 检测当前 listener 是否到时间执行 resync 操作
func (p *processorListener) shouldResync(now time.Time) bool {
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()

	// 为 0 表示禁用 resync
	if p.resyncPeriod == 0 {
		return false
	}

	return now.After(p.nextResync) || now.Equal(p.nextResync)
}

// determineNextResync 确定下一次执行 resync 的实际时间
func (p *processorListener) determineNextResync(now time.Time) {
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()

	p.nextResync = now.Add(p.resyncPeriod)
}

func (p *processorListener) setResyncPeriod(resyncPeriod time.Duration) {
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()

	p.resyncPeriod = resyncPeriod
}
