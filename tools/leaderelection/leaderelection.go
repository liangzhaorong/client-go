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

// Package leaderelection implements leader election of a set of endpoints.
// It uses an annotation in the endpoints object to store the record of the
// election state. This implementation does not guarantee that only one
// client is acting as a leader (a.k.a. fencing).
//
// A client only acts on timestamps captured locally to infer the state of the
// leader election. The client does not consider timestamps in the leader
// election record to be accurate because these timestamps may not have been
// produced by a local clock. The implemention does not depend on their
// accuracy and only uses their change to indicate that another client has
// renewed the leader lease. Thus the implementation is tolerant to arbitrary
// clock skew, but is not tolerant to arbitrary clock skew rate.
//
// However the level of tolerance to skew rate can be configured by setting
// RenewDeadline and LeaseDuration appropriately. The tolerance expressed as a
// maximum tolerated ratio of time passed on the fastest node to time passed on
// the slowest node can be approximately achieved with a configuration that sets
// the same ratio of LeaseDuration to RenewDeadline. For example if a user wanted
// to tolerate some nodes progressing forward in time twice as fast as other nodes,
// the user could set LeaseDuration to 60 seconds and RenewDeadline to 30 seconds.
//
// While not required, some method of clock synchronization between nodes in the
// cluster is highly recommended. It's important to keep in mind when configuring
// this client that the tolerance to skew rate varies inversely to master
// availability.
//
// Larger clusters often have a more lenient SLA for API latency. This should be
// taken into account when configuring the client. The rate of leader transitions
// should be monitored and RetryPeriod and LeaseDuration should be increased
// until the rate is stable and acceptably low. It's important to keep in mind
// when configuring this client that the tolerance to API latency varies inversely
// to master availability.
//
// DISCLAIMER: this is an alpha API. This library will likely change significantly
// or even be removed entirely in subsequent releases. Depend on this API at
// your own risk.
package leaderelection

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	rl "k8s.io/client-go/tools/leaderelection/resourcelock"

	"k8s.io/klog"
)

const (
	JitterFactor = 1.2
)

// NewLeaderElector creates a LeaderElector from a LeaderElectionConfig
func NewLeaderElector(lec LeaderElectionConfig) (*LeaderElector, error) {
	if lec.LeaseDuration <= lec.RenewDeadline {
		return nil, fmt.Errorf("leaseDuration must be greater than renewDeadline")
	}
	if lec.RenewDeadline <= time.Duration(JitterFactor*float64(lec.RetryPeriod)) {
		return nil, fmt.Errorf("renewDeadline must be greater than retryPeriod*JitterFactor")
	}
	if lec.LeaseDuration < 1 {
		return nil, fmt.Errorf("leaseDuration must be greater than zero")
	}
	if lec.RenewDeadline < 1 {
		return nil, fmt.Errorf("renewDeadline must be greater than zero")
	}
	if lec.RetryPeriod < 1 {
		return nil, fmt.Errorf("retryPeriod must be greater than zero")
	}
	if lec.Callbacks.OnStartedLeading == nil {
		return nil, fmt.Errorf("OnStartedLeading callback must not be nil")
	}
	if lec.Callbacks.OnStoppedLeading == nil {
		return nil, fmt.Errorf("OnStoppedLeading callback must not be nil")
	}

	if lec.Lock == nil {
		return nil, fmt.Errorf("Lock must not be nil.")
	}
	le := LeaderElector{
		config:  lec,
		clock:   clock.RealClock{},
		metrics: globalMetricsFactory.newLeaderMetrics(),
	}
	le.metrics.leaderOff(le.config.Name)
	return &le, nil
}

type LeaderElectionConfig struct {
	// Lock is the resource that will be used for locking
	Lock rl.Interface

	// LeaseDuration is the duration that non-leader candidates will
	// wait to force acquire leadership. This is measured against time of
	// last observed ack.
	//
	// A client needs to wait a full LeaseDuration without observing a change to
	// the record before it can attempt to take over. When all clients are
	// shutdown and a new set of clients are started with different names against
	// the same leader record, they must wait the full LeaseDuration before
	// attempting to acquire the lease. Thus LeaseDuration should be as short as
	// possible (within your tolerance for clock skew rate) to avoid a possible
	// long waits in the scenario.
	//
	// Core clients default this value to 15 seconds.
	LeaseDuration time.Duration
	// RenewDeadline is the duration that the acting master will retry
	// refreshing leadership before giving up.
	//
	// Core clients default this value to 10 seconds.
	RenewDeadline time.Duration
	// RetryPeriod is the duration the LeaderElector clients should wait
	// between tries of actions.
	//
	// Core clients default this value to 2 seconds.
	RetryPeriod time.Duration

	// Callbacks are callbacks that are triggered during certain lifecycle
	// events of the LeaderElector
	Callbacks LeaderCallbacks

	// WatchDog is the associated health checker
	// WatchDog may be null if its not needed/configured.
	WatchDog *HealthzAdaptor

	// ReleaseOnCancel should be set true if the lock should be released
	// when the run context is cancelled. If you set this to true, you must
	// ensure all code guarded by this lease has successfully completed
	// prior to cancelling the context, or you may have two processes
	// simultaneously acting on the critical path.
	ReleaseOnCancel bool

	// Name is the name of the resource lock for debugging
	Name string
}

// LeaderCallbacks are callbacks that are triggered during certain
// lifecycle events of the LeaderElector. These are invoked asynchronously.
//
// possible future callbacks:
//  * OnChallenge()
type LeaderCallbacks struct {
	// OnStartedLeading is called when a LeaderElector client starts leading
	OnStartedLeading func(context.Context)
	// OnStoppedLeading is called when a LeaderElector client stops leading
	OnStoppedLeading func()
	// OnNewLeader is called when the client observes a leader that is
	// not the previously observed leader. This includes the first observed
	// leader when the client starts.
	OnNewLeader func(identity string)
}

// LeaderElector is a leader election client.
type LeaderElector struct {
	config LeaderElectionConfig
	// internal bookkeeping
	observedRecord    rl.LeaderElectionRecord
	observedRawRecord []byte
	observedTime      time.Time
	// used to implement OnNewLeader(), may lag slightly from the
	// value observedRecord.HolderIdentity if the transition has
	// not yet been reported.
	reportedLeader string

	// clock is wrapper around time to allow for less flaky testing
	clock clock.Clock

	metrics leaderMetricsAdapter

	// name is the name of the resource lock for debugging
	name string
}

// Run starts the leader election loop
// le.acquire 函数尝试从 Etcd 中获取资源锁, 领导者节点获取到资源锁后会执行该领导者程序的主要逻辑(
// 即 le.config.Callbacks.OnStartedLeading 回调函数), 并通过 le.renew 函数定时(默认值为 2 秒)对
// 资源锁续约. 候选节点获取不到资源锁, 它不会退出并定时(默认值为 2 秒)尝试获取资源锁, 直到成功为止.
func (le *LeaderElector) Run(ctx context.Context) {
	defer func() {
		runtime.HandleCrash()
		le.config.Callbacks.OnStoppedLeading()
	}()
	if !le.acquire(ctx) {
		return // ctx signalled done
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go le.config.Callbacks.OnStartedLeading(ctx)
	le.renew(ctx)
}

// RunOrDie starts a client with the provided config or panics if the config
// fails to validate.
func RunOrDie(ctx context.Context, lec LeaderElectionConfig) {
	le, err := NewLeaderElector(lec)
	if err != nil {
		panic(err)
	}
	if lec.WatchDog != nil {
		lec.WatchDog.SetLeaderElection(le)
	}
	le.Run(ctx)
}

// GetLeader returns the identity of the last observed leader or returns the empty string if
// no leader has yet been observed.
func (le *LeaderElector) GetLeader() string {
	return le.observedRecord.HolderIdentity
}

// IsLeader returns true if the last observed leader was this client else returns false.
func (le *LeaderElector) IsLeader() bool {
	return le.observedRecord.HolderIdentity == le.config.Lock.Identity()
}

// acquire loops calling tryAcquireOrRenew and returns true immediately when tryAcquireOrRenew succeeds.
// Returns false if ctx signals done.
// acquire 资源锁获取过程
// 获取资源锁的过程通过 wait.JitterUntil 定时器定时执行, 它接收一个 func 匿名函数和一个 stopCh chan, 内部会定时
// 调用匿名函数, 只有当 stopCh 关闭时, 该定时器才会停止并退出.
// 执行 le.tryAcquireOrRenew 函数来获取资源锁. 如果其获取资源锁失败, 会通过 return 等待下一次定时获取资源锁. 如
// 果其获取资源锁成功, 则说明当前节点可以成为领导者节点, 退出 acquire 函数并返回 true.
func (le *LeaderElector) acquire(ctx context.Context) bool {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	succeeded := false
	desc := le.config.Lock.Describe()
	klog.Infof("attempting to acquire leader lease  %v...", desc)
	wait.JitterUntil(func() {
		succeeded = le.tryAcquireOrRenew()
		le.maybeReportTransition()
		if !succeeded {
			klog.V(4).Infof("failed to acquire lease %v", desc)
			return
		}
		le.config.Lock.RecordEvent("became leader")
		le.metrics.leaderOn(le.config.Name)
		klog.Infof("successfully acquired lease %v", desc)
		cancel()
	}, le.config.RetryPeriod, JitterFactor, true, ctx.Done())
	return succeeded
}

// renew loops calling tryAcquireOrRenew and returns immediately when tryAcquireOrRenew fails or ctx signals done.
// 领导者节点获取资源锁后, 会定时(默认值为 2 秒) 循环更新租约信息, 以保持长久的领导者身份. 若因
// 网络超时而导致租约信息更新失败, 则说明被候选节点抢占了领导者身份, 当前节点会退出进程.
func (le *LeaderElector) renew(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wait.Until(func() {
		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, le.config.RenewDeadline)
		defer timeoutCancel()
		// 领导者节点的续约过程通过 wait.PollImmediateUntil 定时器定时执行, 它接收一个 func 匿名函数(条件函数) 和一个 stopCh,
		// 内部会定时条件函数, 当条件函数返回 true 或 stopCh 关闭时, 该定时器才会停止并退出.
		err := wait.PollImmediateUntil(le.config.RetryPeriod, func() (bool, error) {
			done := make(chan bool, 1)
			go func() {
				defer close(done)
				// 执行 le.tryAcquireOrRenew 函数来实现领导者节点的续约, 其原理与资源锁获取过程相同.
				// le.tryAcquireOrRenew 函数返回 true 说明续约成功, 并进入下一个定时续约; 返回 false
				// 则退出并执行 le.release 函数且释放资源锁.
				done <- le.tryAcquireOrRenew()
			}()

			select {
			case <-timeoutCtx.Done():
				return false, fmt.Errorf("failed to tryAcquireOrRenew %s", timeoutCtx.Err())
			case result := <-done:
				return result, nil
			}
		}, timeoutCtx.Done())

		le.maybeReportTransition()
		desc := le.config.Lock.Describe()
		if err == nil {
			klog.V(5).Infof("successfully renewed lease %v", desc)
			return
		}
		le.config.Lock.RecordEvent("stopped leading")
		le.metrics.leaderOff(le.config.Name)
		klog.Infof("failed to renew lease %v: %v", desc, err)
		cancel()
	}, le.config.RetryPeriod, ctx.Done())

	// if we hold the lease, give it up
	if le.config.ReleaseOnCancel {
		le.release()
	}
}

// release attempts to release the leader lease if we have acquired it.
func (le *LeaderElector) release() bool {
	if !le.IsLeader() {
		return true
	}
	leaderElectionRecord := rl.LeaderElectionRecord{
		LeaderTransitions: le.observedRecord.LeaderTransitions,
	}
	if err := le.config.Lock.Update(leaderElectionRecord); err != nil {
		klog.Errorf("Failed to release lock: %v", err)
		return false
	}
	le.observedRecord = leaderElectionRecord
	le.observedTime = le.clock.Now()
	return true
}

// tryAcquireOrRenew tries to acquire a leader lease if it is not already acquired,
// else it tries to renew the lease if it has already been acquired. Returns true
// on success else returns false.
func (le *LeaderElector) tryAcquireOrRenew() bool {
	now := metav1.Now()
	leaderElectionRecord := rl.LeaderElectionRecord{
		HolderIdentity:       le.config.Lock.Identity(),
		LeaseDurationSeconds: int(le.config.LeaseDuration / time.Second),
		RenewTime:            now,
		AcquireTime:          now,
	}

	// 1. obtain or create the ElectionRecord
	// 1. 首先, 通过 le.config.Lock.Get 函数获取资源锁, 当资源锁不存在时, 当前节点创建该 key(获取锁)并写入
	//    自身节点的信息,创建成功则当前节点成为领导者节点并返回 true.
	oldLeaderElectionRecord, oldLeaderElectionRawRecord, err := le.config.Lock.Get()
	if err != nil {
		if !errors.IsNotFound(err) {
			klog.Errorf("error retrieving resource lock %v: %v", le.config.Lock.Describe(), err)
			return false
		}
		if err = le.config.Lock.Create(leaderElectionRecord); err != nil {
			klog.Errorf("error initially creating leader election record: %v", err)
			return false
		}
		le.observedRecord = leaderElectionRecord
		le.observedTime = le.clock.Now()
		return true
	}

	// 2. Record obtained, check the Identity & Time
	// 2. 当资源锁存在时, 更新本地缓存的租约信息.
	if !bytes.Equal(le.observedRawRecord, oldLeaderElectionRawRecord) {
		le.observedRecord = *oldLeaderElectionRecord
		le.observedRawRecord = oldLeaderElectionRawRecord
		le.observedTime = le.clock.Now()
	}
	// 候选节点会验证领导者节点的租约是否到期, 如果尚未到期, 暂时还不能抢占并返回 false
	if len(oldLeaderElectionRecord.HolderIdentity) > 0 &&
		le.observedTime.Add(le.config.LeaseDuration).After(now.Time) &&
		!le.IsLeader() {
		klog.V(4).Infof("lock is held by %v and has not yet expired", oldLeaderElectionRecord.HolderIdentity)
		return false
	}

	// 3. We're going to try to update. The leaderElectionRecord is set to it's default
	// here. Let's correct it before updating.
	// 3. 如果是领导者节点, 那么 AcquireTime(资源锁获得时间) 和 LeaderTransitions(领导者进行切换的次数) 字段保持不变.
	//    如果是候选节点, 则说明领导者节点的租约到期, 给 LeaderTransitions 字段加 1 并抢占资源锁.
	if le.IsLeader() {
		leaderElectionRecord.AcquireTime = oldLeaderElectionRecord.AcquireTime
		leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions
	} else {
		leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions + 1
	}

	// update the lock itself
	// 4. 通过 le.config.Lock.Update 函数尝试去更新租约信息, 若更新成功, 函数返回 true.
	if err = le.config.Lock.Update(leaderElectionRecord); err != nil {
		klog.Errorf("Failed to update lock: %v", err)
		return false
	}

	le.observedRecord = leaderElectionRecord
	le.observedTime = le.clock.Now()
	return true
}

// maybeReportTransition 若客户端保存的 leader 标识与上一次的不一致, 且配置了 OnNewLeader 回调函数,
// 则调用该 le.config.Callbacks.OnNewLeader 回调函数
func (le *LeaderElector) maybeReportTransition() {
	if le.observedRecord.HolderIdentity == le.reportedLeader {
		return
	}
	le.reportedLeader = le.observedRecord.HolderIdentity
	if le.config.Callbacks.OnNewLeader != nil {
		go le.config.Callbacks.OnNewLeader(le.reportedLeader)
	}
}

// Check will determine if the current lease is expired by more than timeout.
func (le *LeaderElector) Check(maxTolerableExpiredLease time.Duration) error {
	if !le.IsLeader() {
		// Currently not concerned with the case that we are hot standby
		return nil
	}
	// If we are more than timeout seconds after the lease duration that is past the timeout
	// on the lease renew. Time to start reporting ourselves as unhealthy. We should have
	// died but conditions like deadlock can prevent this. (See #70819)
	if le.clock.Since(le.observedTime) > le.config.LeaseDuration+maxTolerableExpiredLease {
		return fmt.Errorf("failed election to renew leadership on lease %s", le.config.Name)
	}

	return nil
}
